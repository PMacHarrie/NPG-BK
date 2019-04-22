"""
    Author: Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import os
import json
import psycopg2

from ndeutil import *

def lambda_handler(event, context):
    
    try:
        odJobId = int(event['pathParameters']['jobid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing or invalid Job ID")
        
    # get the msg body for proxy integration case
    if 'resource' in event:
        if event['body'] is None:
            return createErrorResponse(400, "Validation error", 
                "body required")
        if isinstance(event['body'], dict):
            inputBody = event['body']
        else:
            inputBody = json.loads(event['body'])
    else:
        inputBody = event
        
    if inputBody.get('newJobStatus') is None:
        return createErrorResponse(400, "Validation Error", "Only newJobStatus is currently supported")
    
    validStatuses = ['QUEUED', 'COMPLETE']
    if inputBody.get('newJobStatus') not in validStatuses:
        return createErrorResponse(400, "Validation Error", "Valid values for newJobStatus are: " + str(validStatuses))
    newStatus = inputBody.get('newJobStatus')
    
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host=os.environ['RD_HOST'],
            dbname=os.environ['RD_DBNM'],
            user=os.environ['RD_USER'],
            password=os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        cur.execute("UPDATE ondemandjob SET odjobstatus = %s WHERE odjobid = %s RETURNING odjobid, odjobstatus", 
            (newStatus, odJobId) )
        row = cur.fetchone()
        
        if row is None:
            return createErrorResponse(404, "Not Found", "No job found for ID: " + str(odJobId))
        else:
            if newStatus != row[1]:
                print("ERROR: returned jobId does NOT match input jobId?!")
                return createErrorResponse(500, "Internal Error", "Failed to update Job Status, please try again later")
        # print(row)

        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": '*'
            },
            "body": json.dumps({
                "message": "Successfully updated Job (" + str(odJobId) + ") with new Job Status: " + newStatus
            })
        }
        
        cur.close()
        conn.commit()

        return response
    
    except Exception as e:
        conn.rollback()

        print("ERROR encountered:", e)
        return createErrorResponse(500, "Internal error", 
            "An exception was encountered, please try again later.")
    finally:
        conn.close()
        