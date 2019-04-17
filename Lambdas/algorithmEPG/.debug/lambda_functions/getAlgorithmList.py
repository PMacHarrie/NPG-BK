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


print('Loading getAlgorithmList')

def lambda_handler(event, context):
    
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        cur.execute("SELECT algorithmid, algorithmname, algorithmversion, algorithmtype FROM algorithm")
        rows = cur.fetchall()
        # print(len(rows))
        
        resultArray = []
        for row in rows:
            resultArray.append( 
                {"algorithmId": row[0], 
                 "algorithmName": row[1], 
                 "algorithmVersion": row[2],
                 "algorithmType": row[3]
                } )
        
        cur.close()
        conn.rollback()

        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": '*'
            },
            "body": json.dumps({
                "algorithms": resultArray
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
        
        