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
            host=os.environ['RD_HOST'],
            dbname=os.environ['RD_DBNM'],
            user=os.environ['RD_USER'],
            password=os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        cur.execute("select prid, prrulename, prruletype from productionrule where algorithmid = %s",
            (algoIdParam,))
        rows=cur.fetchall()
        # print(len(rows))
        
        resultArray = []
        for row in rows:
            resultArray.append( 
                {"ruleId" : row[0], 
                 "ruleName" : row[1], 
                 "ruleType" : row[2]} )
        
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
                "productionRules" : resultArray
            })
        }
        return response
    
    except psycopg2.Error as e:
        conn.rollback()
        conn.close()
        
        print("ERROR encountered querying Production Rules:", e)
        return createErrorResponse(500, "Error while querying", 
            "An error was encountered getting results. Please try again later.")
        
        