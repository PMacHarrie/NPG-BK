'''
Author: Hieu Phung
Date created: 2019-01-07
Python Version: 3.6
'''

import json
import os
import sys
import psycopg2
from elasticsearch import Elasticsearch, helpers
import certifi
from datetime import datetime
import time

import logging
from logging.handlers import TimedRotatingFileHandler


def myMain():
    
    try:
        conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
        conn = psycopg2.connect(conn_string)
        
        es = Elasticsearch(
            ['https://vpc-nde-test-sncp4dqd63lf4rs2vdihlk763e.us-east-1.es.amazonaws.com'],
            port = '443',
            use_ssl = True,
            verify_certs = True,
            ca_certs = certifi.where(),
            timeout = 60)
        
        currBatchSize = 100
        sleepTime = 9
        
        while(1):
            
            if 'esBatchIndex.pySTOP' in os.listdir('.'):
                logger.info(sys.argv[0] + 'STOP file exists, stopping loop')
                break;
            
            try:
                rowCount = queryAndIndex(conn, es, currBatchSize)
                
                if rowCount == currBatchSize:
                    if currBatchSize < 600:
                        currBatchSize += 50
                    if sleepTime > 3:
                        sleepTime -= 3
                elif rowCount < currBatchSize - 25:
                    if currBatchSize > 100:
                        currBatchSize -= 50
                    if sleepTime < 30:
                        sleepTime += 3
                    
            except Exception as e:
                logger.error("Exception caught: " + str(e))
                if conn is not None and conn.closed == 0:
                    conn.rollback()
                    logger.warn("Rolled back")
                if conn.closed != 0:
                    logger.error("DB connection closed, sleeping for 15s then attempting to reconnect")
                    time.sleep(15)
                    conn = psycopg2.connect(conn_string)
                
            logger.info("gonna sleep: " + str(sleepTime))
            time.sleep(sleepTime)
            
    finally:
        if conn is not None and conn.closed == 0:
            conn.close()


def queryAndIndex(conn, es, currBatchSize):
    
    cur = conn.cursor()
    
    dt1 = datetime.now()
    
    logger.info("===== about to get lock =====")
    cur.execute("LOCK TABLE esbatch_lock IN EXCLUSIVE MODE")
    logger.info("got lock, querying w/ batchSize: " + str(currBatchSize))
    
    query = "SELECT fileid, es_json FROM esbatch WHERE errormsg IS NULL ORDER BY rowinserttime asc LIMIT " + str(currBatchSize)
    
    cur.execute(query)
    rows = cur.fetchall()
    rowCount = len(rows)
    logger.info("got rows: " + str(rowCount))
    
    if rowCount == 0:
        conn.rollback()
        logger.info(">>>>> No data to batch <<<<<")
        return 0
    
    actions = []
    fileids = []
    
    for row in rows:
        # print(row)
        
        fileids.append(row[0])
        
        actions.append({
            "_id": row[0],
            "_source": row[1]
        })
    
    # print(actions)
    
    dt2 = datetime.now()
    logger.info("doing es bulk")
    
    try:
        bulkSuccess, _ = helpers.bulk(
            es,
            actions,
            index = "file", 
            doc_type = "_doc")
        dt3 = datetime.now()
    except helpers.BulkIndexError as bie:
        dt3 = datetime.now()
        # print(bie)
        # print(bie.errors)
        bulkSuccess = rowCount
        for error in bie.errors:
            fileid = error.get('index').get('_id')
            errorDets = error.get('index').get('error')
            errorStr = "(1) " + errorDets.get('reason') + ": " + \
                errorDets.get('caused_by').get('type') + " " + errorDets.get('caused_by').get('reason')
            logger.error(str(fileid) + " " + errorStr)
            cur.execute('UPDATE esbatch SET errormsg = %s WHERE fileid = %s', (errorStr, fileid))
            bulkSuccess -= 1
        
    logger.info("finished bulk, dur(excl err handling): " + str(dt3-dt2))
        
    # print(datetime.now(), "deleting fileids") #, fileids)
    cur.execute('DELETE FROM esbatch WHERE fileid IN %s AND errormsg IS NULL', (tuple(fileids),) )
    logger.info(str(cur.rowcount) + " rows deleted from esbatch")
    
    cur.close()
    conn.commit()
    logger.info(">>>>> Tried: %s | successes: %s | errors: %s | dur(es): %s | dur(total): %s <<<<<" % 
        (rowCount, bulkSuccess, rowCount-bulkSuccess, dt3-dt2, datetime.now()-dt1) )
    
    return rowCount;

if __name__ == "__main__":
    
    myPID = str(os.getpid())

    thisScriptName = sys.argv[0][:-3]
    logDir = "esBatchLogs/"

    if not os.path.exists(logDir):
        os.makedirs(logDir)

    logFileName = thisScriptName + "." + myPID + '.log'
    # logFile = open(logDir + logFileName, 'w')
    # sys.stdout = sys.stderr = logFile
    
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    rfh = TimedRotatingFileHandler(
        logDir + logFileName,
        when = "midnight",
        interval = 1,
        backupCount = 30)
    rfh.setFormatter(formatter)
    rfh.setLevel(logging.DEBUG)
    logger.addHandler(rfh)
    
    # create console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    myMain()



