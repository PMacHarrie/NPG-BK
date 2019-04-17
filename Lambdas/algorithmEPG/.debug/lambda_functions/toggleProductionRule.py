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


print("Loading toggleProductionRule")

def lambda_handler(event, context):
    
    try:
        algoId = int(event['pathParameters']['algoid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing or invalid Algorithm ID")
    print("algoId", algoId)
    
    try:
        ruleId = int(event['pathParameters']['ruleid'])
    except KeyError as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing Production Rule ID")
    except ValueError as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Production Rule ID must be integer")
    print("ruleId", ruleId)
    
    if 'resource' in event:
        if event['body'] is None:
            return createErrorResponse(400, "Validation error", "body required")
        if isinstance(event['body'], dict):
            inputBody = event['body']
        else:
            inputBody = json.loads(event['body'])
    else:
        inputBody = event
    
    if inputBody.get('newPrActiveFlag') is None:
        return createErrorResponse(400, "Validation Error", "Only newPrActiveFlag is currently supported")
    
    validVals = [0, 1, '0', '1']
    print("newPrActiveFlag", inputBody.get('newPrActiveFlag'))
    if inputBody.get('newPrActiveFlag') not in validVals:
        return createErrorResponse(400, "Validation Error", "Valid values for newPrActiveFlag are 0 or 1" )
    newFlag = int(inputBody.get('newPrActiveFlag'))
    
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        print('Executing update...')
        cur.execute("UPDATE productionrule SET practiveflag_nsof = %s WHERE prid = %s " +
            "RETURNING prid, practiveflag_nsof", (newFlag, ruleId) )
        row = cur.fetchone()
        
        if row is None:
            return createErrorResponse(404, "Not Found", "No production rule found for ID: " + str(ruleId))
        else:
            if newFlag != row[1]:
                return createErrorResponse(500, "Internal Error", 
                    "Failed to update Production Rule Active Flag, please try again later")
        # print(row)
        
        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "message": "Successfully updated Production Rule (ID " + str(ruleId) + 
                    ") with new Active Flag: " + str(newFlag)
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
        