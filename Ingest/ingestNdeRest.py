'''
File name: ingestNdeRest.py
Author: Hieu Phung, Peter MacHarrie
Date created: 2018-12-20
Python Version: 3.6
'''

import boto3
import json
import os
import sys
import requests
from datetime import datetime
import time
import psycopg2
import logging
from logging.handlers import TimedRotatingFileHandler

sqs = boto3.client('sqs', region_name='us-east-1')
# sns = boto3.client('sns', region_name='us-east-1')
# s3 = boto3.resource('s3')

insQueueUrl = 'https://sqs.us-east-1.amazonaws.com/784330347242/IncomingFilesQ'
# ndeRestUrl = 'mlf6ufvhl6.execute-api.us-east-1.amazonaws.com/debug/file'  #macDev
ndeRestUrl = 'https://vunlor9i2m.execute-api.us-east-1.amazonaws.com/nde-rest/file'

dw_conn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"

def myMain():
    
    dw_conn = psycopg2.connect(dw_conn_string)

    while(1):
        pollQueueAndProcess(dw_conn)
        if (sys.argv[0] + 'STOP') in os.listdir('.'):
            logger.info(sys.argv[0] + 'STOP file exists, stopping loop')
            break;
        
        if dw_conn.closed != 0:
            logger.error("DB connection closed, sleeping for 15s then attempting to reconnect")
            time.sleep(15)
            dw_conn = psycopg2.connect(dw_conn_string)
    
    dw_conn.close()



def pollQueueAndProcess(conn):
    logger.info('===== Getting messages =====')
    response = sqs.receive_message(
        QueueUrl = insQueueUrl,
        MaxNumberOfMessages=1,
        AttributeNames = [
            'All'
        ],
        MessageAttributeNames = [
            'All'
        ],
        WaitTimeSeconds = 10
    )

    if 'Messages' in response:
        dt1 = datetime.now()

        queueMsg = response['Messages'][0]

        sentTsMillis = queueMsg['Attributes']['SentTimestamp']
        sentTs = datetime.fromtimestamp(int(sentTsMillis)/1000)

        msgBodyStr = queueMsg['Body']
        receipt_handle = queueMsg['ReceiptHandle']
        # logger.info("Got Message from Queue:", msgBodyStr)

        msgBody = json.loads(msgBodyStr)
        # print("MessageId=", msgBody.get('MessageId'))

        msg = json.loads(msgBody.get('Message'))

        filename = msg.get('filename')
        logger.info("Got message (sentTs {0}) filename: {1}".format(sentTs, filename) )

        insLambdaInput = {
            "filename": filename,
            "filestoragereference": {
                "bucket": "ndepg",
                "key" : "i/" + filename
            }
        }

        logger.info("sending file PUT")
        response = requests.put(ndeRestUrl, data = json.dumps(insLambdaInput))

        if str(response.status_code) == '200':
            logger.info("PUT response: code: 200 %s" % response.text)
            sqs.delete_message(
                QueueUrl = insQueueUrl,
                ReceiptHandle = receipt_handle
            )
            logger.info("deleted sqs message")
            
            cur = conn.cursor()
            cur.execute("UPDATE if_objectevent SET if_filemessagecreatetime = %s WHERE if_filename = %s",
                (sentTs, filename))
            if cur.rowcount == 0:
                logger.warn("No existing record in if_objectevent, doing INSERT")
                cur.execute("INSERT INTO if_objectevent (if_filemessagecreatetime, if_filename) VALUES (%s, %s)",
                    (sentTs, filename))
            conn.commit()
            cur.close()
            logger.info("finshed if_objectevent if_filemessagecreatetime update/insert")
            
        elif str(response.status_code) in ['400', '404', '407']:
            logger.info("PUT response: code: %s %s" % (response.status_code, response.text) )
            sqs.delete_message(
                QueueUrl = insQueueUrl,
                ReceiptHandle = receipt_handle
            )
            logger.info("deleted sqs message:") #, receipt_handle)
        elif str(response.status_code) == '500':
            logger.info("PUT response: code: 500 %s" % response.text)
            logger.info("NOT deleting sqs message")
        else:
            logger.info("Lambda error? Unexpected status code: %s %s" % (response.status_code, response.text) )

        dt3 = datetime.now()
        logger.info(">>>>> {0} ** dur (since SQS rcv): {1}s ** dur (since msg to SQS): {2}s <<<<<".format(
            filename, (dt3-dt1).total_seconds(), (dt3-sentTs).total_seconds()) )

    else:
        logger.info('No Messages in Queue')



if __name__ == "__main__":
    
    myPID = str(os.getpid())

    thisScriptName = sys.argv[0][:-3]
    logDir = "ingestLogs/"

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


