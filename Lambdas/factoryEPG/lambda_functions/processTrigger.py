"""
    Author: Jonathan Hansford; SOLERS INC
    Contact: 
    Last modified: 2019-01-31
    Python Version: 3.6
"""

import sys
import os
import json
import re
import boto3
import botocore
from datetime import datetime
import psycopg2

sys.path.append(os.environ['LAMBDA_TASK_ROOT'] + '/lambda_functions')
import factoryEPGCommon

print('Loading processTrigger')
TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

# Database connection to be reused by subsequent Lambda executions in the same container.
conn = psycopg2.connect(
    host = os.environ['RD_HOST'],
    dbname = os.environ['RD_DBNM'],
    user = os.environ['RD_USER'],
    password = os.environ['RD_PSWD']
)


def lambda_handler(event, context):
    # This lambda function is triggered by SQS.
    # This means that an array of messages will be added into 'Records' element of the event dictionary.
    # See: https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
    print('Got %d message(s) in this batch.' % len(event['Records']))
    
    for message in event['Records']:
        processValidatedProductMessage(conn, json.loads(message['body']))
    
    # This Lambda function has nothing to return.
    return None
    
    
def processValidatedProductMessage(databaseConnection, validatedProductMessage):
    """
    Processes the given message from the ValidatedProduct topic by calling SP_GET_POTENTIAL_JOB_SPECS
    
    This assumes that the validatedProductMessage has the following structure (at a minimum, other fields won't hurt)
    {
        "MessageAttributes": {
            "productId": {
                "Value: <the product ID>
            }
            "fileId": {
                "Value": <the file ID>
            }
            "fileStartTime": {
                "Value": <the observation start time of the file, as a string in the format specified by TIME_FORMAT> 
            }
            "fileEndTime": {
                "Value": <the observation end time of the file, as a string in the format specified by TIME_FORMAT>
            }
        }
    }
    """
    print('Starting to process a validated product message: %s' % str(validatedProductMessage))
 
    # The arguments to sp_get_potential_job_specs are v_fileid, v_productid, and v_hostname
    # The value of v_hostname is never used, so we will pass null.
    fileId = int(validatedProductMessage['MessageAttributes']['fileId']['Value'])
    productId = int(validatedProductMessage['MessageAttributes']['productId']['Value'])
    hostname = None
    
    # sp_get_potential_job_specs throws an error if the fileStartTime == fileEndTime
    # To avoid retrying the message multiple times for no reason in that case, we will check explicitly for this possibility.
    fileStartTime = datetime.strptime(validatedProductMessage['MessageAttributes']['fileStartTime']['Value'], TIME_FORMAT)
    fileEndTime = datetime.strptime(validatedProductMessage['MessageAttributes']['fileEndTime']['Value'], TIME_FORMAT)
    
    if fileStartTime == fileEndTime:
        print('The start time and end time for the file with id = %d (productId = %d) are the same. No job specifications will be created, and sp_get_potential_job_specs will not be called.' % (fileId, productId))
    else:
        print('Calling sp_get_potential_job_specs with arguments: fileId = %d, productId = %d' % (fileId, productId))
        
        with databaseConnection:
            storedProcedureTimer = factoryEPGCommon.CodeSegmentTimer('Running sp_get_potential_job_specs')
            
            serverSideCursor = factoryEPGCommon.runStoredProcedureToGetServerSideCursor(databaseConnection, 'sp_get_potential_job_specs', (fileId, productId, hostname))
        
            storedProcedureTimer.finishAndPrintTime()
            fetchResultsTimer = factoryEPGCommon.CodeSegmentTimer('Fetching results from serverSideCursor')
            
            results = serverSideCursor.fetchall()
            serverSideCursor.close()
            
            fetchResultsTimer.finishAndPrintTime()
            
            if len(results) == 0:
                print('No job specifications were created.')
            else:
                for row in results:
                    prId, jobStart, jobEnd, jobClass, jobPriority = row
                    print('Created a new productionJobSpec with prId = %d, jobStart = %s, jobEnd = %s' % (prId, jobStart, jobEnd))
    
    print('Finished processing validated product message for fileId: %d' % fileId)