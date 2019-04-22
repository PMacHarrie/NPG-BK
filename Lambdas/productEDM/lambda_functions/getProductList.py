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


print('Loading getProductList')

def lambda_handler(event, context):
    
    try:
        print('Querying DB...')
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        cur.execute("""
            SELECT pd.productId, pd.productShortname, pd.productDescription, 
              COALESCE( 
                (SELECT p.platformname 
                  FROM productplatform_xref ppx, platform p 
                  WHERE ppx.productid = pd.productId 
                    AND ppx.platformid = p.platformid
                ), 'N/A'
              )
            FROM productDescription pd""")
        rows = cur.fetchall()
        # print(len(rows))
        
        print('Getting results...', len(rows))
        
        resultArray = []
        for row in rows:
            resultArray.append({
                "productId": row[0], 
                "productShortname": row[1], 
                "productDescription": row[2],
                "platformName": row[3]
            })
        
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
                "products": resultArray
            })
        }
        return response
    
    except psycopg2.Error as e:
        conn.rollback()
        
        print("ERROR encountered querying:", e)
        return createErrorResponse(500, "Internal error", 
            "An exception was encountered, please try again later.")
    finally:
        conn.close()
        



