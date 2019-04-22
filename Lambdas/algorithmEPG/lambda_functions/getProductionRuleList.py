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


print('Loading getProductionRuleList')

def lambda_handler(event, context):
    
    try:
        algoIdParam = int(event['pathParameters']['algoid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing or invalid Algorithm ID")
    
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        cur.execute("SELECT prid, prrulename, prruletype, practiveflag_nsof FROM productionrule WHERE algorithmid = %s",
            (algoIdParam,))
        rows = cur.fetchall()
        # print(len(rows))
        
        resultArray = []
        for row in rows:
            resultArray.append( 
                {"ruleId" : row[0], 
                 "ruleName" : row[1], 
                 "ruleType" : row[2],
                 "activeFlag": row[3]
                } )
        
        cur.close()
        conn.rollback()

        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "productionRules": resultArray
            })
        }
        return response
    
    except Exception as e:
        conn.rollback()
        print("ERROR encountered querying:", e)
        return createErrorResponse(500, "Internal error", 
            "An exception was encountered, please try again later.")
    finally:
        conn.close()
        