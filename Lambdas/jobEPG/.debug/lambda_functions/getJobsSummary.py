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


print('Loading getJobsSummary')

def lambda_handler(event, context):
    print(event)
    
    urlParams = event.get('queryStringParameters')
    
    try:
        if urlParams is not None and urlParams.get('lastHoursInterval') is not None:
            hoursInterval = int(urlParams.get('lastHoursInterval'))
            if hoursInterval > 240 or hoursInterval < 1:
                return createErrorResponse(400, "Validation Error", "lastHoursInterval must be >1 and <120")
        else:
            hoursInterval = 8
    except ValueError as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "lastHoursInterval must be an integer")
    
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        # cur.execute("SELECT pj.prjobid, pjs.prodpartialjobid, pr.prrulename, pj.prjobstatus, pj.prjobenqueuetime, " +
        #     "pj.prjobstarttime FROM productionjob pj, productionjobspec pjs, productionrule pr " +
        #     "WHERE pj.prodpartialjobid = pjs.prodpartialjobid AND pjs.prid = pr.prid")
        # rows = cur.fetchall()
        # print(len(rows))
        
        # for row in rows:
        #     resultArray.append( 
        #         {"jobId" : row[0], 
        #          "jobSpecId" : row[1], 
        #          "ruleName" : row[2]
        #         #  "jobStatus" : row[3],
        #         #  "enqueueTime" : row[4],
        #         #  "startTime" : row[5]
        #         } )
        
        cur.execute("SELECT odalgorithmname, odjobstatus, count(*) FROM ondemandjob "
            "WHERE odjobenqueuetime > NOW() - INTERVAL '%s hours' GROUP BY odalgorithmname, odjobstatus",
            (hoursInterval,) );
        rows = cur.fetchall()

        resultObj = {}
        
        for row in rows:
            algoName = row[0]
            if algoName not in resultObj:
                resultObj[algoName] = { row[1]: row[2] }
            else:
                resultObj[algoName][row[1]] = row[2]
            
        print(resultObj)

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
                "jobsCountByAlgoByStatus": resultObj
            })
        }
        return response
    
    except Exception as e:
        conn.rollback()
        
        print("ERROR encountered querying Jobs:", e)
        return createErrorResponse(500, "Internal error", 
            "An exception was encountered, please try again later.")
    finally:
        conn.close()