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


print('Loading searchProduct')

def lambda_handler(event, context):

    urlParams = event['queryStringParameters']
    
    if urlParams is None or urlParams is None:
        return createErrorResponse(400, "Validation Error", "Missing query parameters (filenamePattern)")
    
    if 'filenamePattern' not in urlParams:
        return createErrorResponse(400, "Validation Error", "Valid parameters: filenamePattern")
    
    print('Querying DB...')
    
    try:
        conn = psycopg2.connect(
                host = os.environ['RD_HOST'],
                dbname = os.environ['RD_DBNM'],
                user = os.environ['RD_USER'],
                password = os.environ['RD_PSWD']
                )
        cur = conn.cursor()

        cur.execute("SELECT productid, productshortname, productDescription FROM productdescription "
            "WHERE %s like productfilenamepattern", (urlParams['filenamePattern'],) ) 
        rows = cur.fetchall()
        # print(len(rows))
        
        resultArray = []
        for row in rows:
            resultArray.append( 
                {"productId": row[0], 
                 "productShortname": row[1], 
                 "productDescription": row[2]} )
        
        cur.close()
        conn.rollback()

        response = {
            "isBase64Encoded": True,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": '*'
            },
            "body": json.dumps({
                "products": resultArray
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
