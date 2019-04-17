"""
    Author: Hieu Phung, Jonathan Hansford; SOLERS INC
    Contact: 
    Last modified: 2019-01-11
    Python Version: 3.6
"""

import sys
import os
import json
import time
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

import boto3
import psycopg2


completeJobSpecQ = 'https://sqs.us-east-1.amazonaws.com/784330347242/PGFactory_CompleteJobSpec'
storedProcName = "SP_GET_COMPLETED_JOB_SPECS"
sleepTime = 5 #seconds


def myMain():
    
    thisScriptName = sys.argv[0][:-3]
    logger.info(thisScriptName + ' starting')
    conn = None
    
    try:
        sqs = boto3.client('sqs', region_name='us-east-1')
            
        conn = psycopg2.connect(
            host = 'nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com',
            dbname = 'nde_dev1',
            user = 'nde_dev1',
            password = 'nde'
        )
        
        while(1):
            
            if (sys.argv[0] + 'STOP') in os.listdir('.'):
                logger.info(sys.argv[0] + 'STOP file exists, stopping loop')
                break;
            
            try:
                runStoredProcAndSendMsg(conn, sqs)
            except Exception as e:
                if conn is not None:
                    conn.rollback()
                logger.error("Exception caught: " + str(e))

            logger.info("====== sleeping: " + str(sleepTime) + "s ======")
            time.sleep(sleepTime)
            
    finally:
        if conn is not None:
            conn.close()


def runStoredProcAndSendMsg(conn, sqs):
    
    # hostname = os.uname()[1]
    # servicename = "jobSpecPoller"
    
    dt1 = datetime.now()
    logger.info("calling " + storedProcName)
    
    cur = conn.cursor()
    cur.callproc(storedProcName, ("hostname", "servicename"))
    
    row = cur.fetchone()
    logger.info("returned from stored proc call, dur: %s" % (datetime.now()-dt1,) )
    
    successCount = 0
    if row is not None and row[0] is not None:
        cur.execute('FETCH ALL IN "' + row[0] + '"')
        rows = cur.fetchall()
        
        logger.info(str(len(rows)) + " rows in sp cursor")
        
        conn.commit()
        cur.close()
        
        for row in rows:
            msgBody = {
                'prodPartialJobId': row[0],
                'prRuleName': row[1]
            }
            sqsResponse = sqs.send_message(
                QueueUrl = completeJobSpecQ ,
                MessageAttributes = {},
                MessageBody = json.dumps(msgBody)
            )
            
            if sqsResponse.get('ResponseMetadata').get('HTTPStatusCode') == 200:
                logger.info(">> SQS msg sent: " + str(msgBody))
                successCount += 1
            else:
                logger.error("ERROR: Got non-200 response from SQS send_message for: " + str(msgBody))
                logger.error("rolling back PJS for prodpartialjobid: " + str(row[0]))
                
                cur = conn.cursor()
                cur.execute("DELETE FROM jobspecparameters WHERE prodpartialjobid = %s", (row[0],) )
                if cur.rowcount == 0:
                    logger.warn("possible ERROR: 0 rows deleted from jobspecparameters")
                else:
                    logger.info(str(cur.rowcount) + " rows deleted from jobspecparameters")
                
                cur.execute("UPDATE productionjobspec SET pjscompletionstatus = 'INCOMPLETE' " +
                    "WHERE prodpartialjobid = %s", (row[0],) )
                if cur.rowcount == 1:
                    logger.info("pjscompletionstatus set to 'INCOMPLETE' for prodpartialjobid: " + str(row[0]))
                else:
                    logger.error("ERROR: failed to set pjscompletionstatus = 'INCOMPLETE'")
                
                conn.commit()
                cur.close()
    else:
        raise Exception("Stored proc returned null")
        
    # cur.close()
    # conn.commit()
    
    logger.info(">>>>> Successfully sent %s messages, Total dur: %s <<<<<" % (successCount, datetime.now() - dt1) )
    

if __name__ == "__main__":
    myPID = str(os.getpid())

    thisScriptName = sys.argv[0][:-3]
    logDir = thisScriptName + "Logs/"

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
    # ch = logging.StreamHandler()
    # ch.setLevel(logging.INFO)
    # ch.setFormatter(formatter)
    # logger.addHandler(ch)
    
    myMain()

    # logFile.close()


