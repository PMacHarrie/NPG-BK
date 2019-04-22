"""
    Author: Peter MacHarrie, Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import os
import json
import psycopg2

from ndeutil import *


print('Loading getDataCubeList')

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
        
        sql="""SELECT
                    e_dimensionListName,
                    e_dimensionListId,
                    JSON_AGG(measureName),
                    SUM(measures) measureCount
                FROM (
                    SELECT e_dimensionListName, l.e_dimensionListId, 
                        SPLIT_PART(measureName, '@',2) measureName, 1 measures
                    FROM EnterpriseDimensionList l, enterpriseMeasure m
                    WHERE l.e_dimensionListId = m.e_dimensionListId
                    GROUP BY 1, 2, 3
                    ) t1
                GROUP BY 1, 2
                ORDER BY 1
                """
        cur.execute(sql)
        rows = cur.fetchall()
        # print(len(rows))
        
        print('Going through results...')
        dataCubeList = {}
    
        for row in rows:
            #print (row)
            dataCubeList[row[0]]= {
                "dataCubeId": row[1],
                "products": row[2], 
                "measureCount": row[3]
            }
        print (dataCubeList)
        
        cur.close()
        conn.rollback()

        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({
                "datacubes" : dataCubeList
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