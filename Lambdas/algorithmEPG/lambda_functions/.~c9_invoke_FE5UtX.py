import os
import json
import psycopg2

from ndeutil import *


print('Loading getAlgorithmList')

def lambda_handler(event, context):
    
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host=os.environ['RD_HOST'],
            dbname=os.environ['RD_DBNM'],
            user=os.environ['RD_USER'],
            password=os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        cur.execute("select algorithmid, algorithmname, algorithmversion from algorithm")
        rows=cur.fetchall()
        # print(len(rows))
        
        resultArray = []
        for row in rows:
            resultArray.append( 
                {"algorithmId" : row[0], 
                 "algorithmName" : row[1], 
                 "algorithmVersion" : row[2]} )
        
        cur.close()
        conn.rollback()
        conn.close()
        
        response = {
            "isBase64Encoded": True,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "algorithms" : resultArray
            })
        }
        return response
    
    except psycopg2.Error as e:
        conn.rollback()
        conn.close()
        
        print("ERROR encountered querying Algorithm:", e)
        return createErrorResponse(500, "Error while querying", 
            "An error was encountered getting results. Please try again later.")
        
        