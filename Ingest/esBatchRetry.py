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


def myMain():
    
    try:
        # myPID = str(os.getpid())

        # if not os.path.exists('./esBatchLogs'):
        #     os.makedirs('./esBatchLogs')
    
        # logFileName = 'esbatchRetry.' + myPID + '.log'
        # logFile = open('esBatchLogs/' + logFileName, 'w')
        # sys.stdout = sys.stderr = logFile
        
        conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
        conn = psycopg2.connect(conn_string)
        
        es = Elasticsearch(
            ['https://vpc-nde-test-sncp4dqd63lf4rs2vdihlk763e.us-east-1.es.amazonaws.com'],
            port = '443',
            use_ssl = True,
            verify_certs = True,
            ca_certs = certifi.where(),
            timeout = 60)
        
        currBatchSize = 20
        sleepTime = 9
        
        while(1):
            
            if 'esBatchRetry.pySTOP' in os.listdir('.'):
                print('Got secret STOP command, stopping loop')
                break;
            
            try:
                rowCount = queryAndIndex(conn, es, currBatchSize)
                
                if rowCount == currBatchSize:
                    if currBatchSize < 100:
                        currBatchSize += 20
                    if sleepTime > 6:
                        sleepTime -= 3
                elif rowCount < currBatchSize - 20:
                    if currBatchSize > 20:
                        currBatchSize -= 20
                    if sleepTime < 30:
                        sleepTime += 3
                    
            except Exception as e:
                if conn is not None:
                    conn.rollback()
                print("Exception caught:", e)

            print("gonna sleep:", sleepTime)
            time.sleep(sleepTime)
    finally:
        if conn is not None:
            conn.close()


def queryAndIndex(conn, es, currBatchSize):
    
    cur = conn.cursor()
    
    dt1 = datetime.now()
    
    # print(dt1, "===== about to get lock =====")
    # cur.execute("LOCK TABLE esbatch_lock IN EXCLUSIVE MODE")
    # print(datetime.now(), "got lock, querying w/ batchSize:", currBatchSize)
    print(datetime.now(), "querying w/ batchSize:", currBatchSize)
    
    query = "SELECT fileid, es_json FROM esbatch WHERE errormsg LIKE '%invalid_shape_exception%' AND retrytime IS NULL " \
                + "ORDER BY rowinserttime asc LIMIT " + str(currBatchSize)
    
    cur.execute(query)
    rows = cur.fetchall()
    rowCount = len(rows)
    print(datetime.now(), "got rows:", rowCount)
    
    if rowCount == 0:
        conn.rollback()
        print(">>>>> No data to batch <<<<<")
        return 0
    
    actions = []
    fileids = []
    
    for row in rows:
        # print(row)
        fileids.append(row[0])
        
        esDoc = row[1]
        # print(esDoc)
        esDoc['edmCore']['fileSpatialArea']['orientation'] = "cw"
        # print(esDoc)
        
        actions.append({
            "_id": row[0],
            "_source": esDoc
        })
    
    # print(actions)
    # print(fileids)
    
    dt2 = datetime.now()
    print(dt2, "doing es bulk retry")
    
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
            errorStr = "(retry) " + errorDets.get('reason') + ": " + \
                errorDets.get('caused_by').get('type') + " " + errorDets.get('caused_by').get('reason')
            print(fileid, errorStr)
            cur.execute('UPDATE esbatch SET errormsg = errormsg || chr(10) || %s, retrytime = now() WHERE fileid = %s', 
                            (errorStr, fileid))
            bulkSuccess -= 1
            fileids.remove(int(fileid))
        
    print(datetime.now(), "finished bulk retry, dur(excl err handling):", dt3-dt2)
        
    # print(datetime.now(), "deleting fileids") #, fileids)
    # cur.execute('DELETE FROM esbatch WHERE fileid IN %s AND errormsg IS NULL', (tuple(fileids),) )
    # print(datetime.now(), cur.rowcount, "rows deleted from esbatch")
    
    
    if bulkSuccess > 0:
        print(datetime.now(), "updating successes")
        cur.execute("UPDATE esbatch SET errormsg = errormsg || chr(10) || '(retry) success', retrytime = now() WHERE fileid IN %s",
                        (tuple(fileids),) )
        print(datetime.now(), cur.rowcount, "rows updated")
    
    cur.close()
    conn.commit()
    print(">>>>> Tried:", rowCount, "| retry successes:", bulkSuccess, "| retry errors:", rowCount-bulkSuccess, 
        "| dur(es):", dt3-dt2, "| dur(total):", datetime.now()-dt1, "<<<<<")
    
    return rowCount;

if __name__ == "__main__":
    myMain()

