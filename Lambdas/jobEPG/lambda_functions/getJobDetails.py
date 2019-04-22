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
        
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        cur.execute("SELECT odjobid, odjobspec, odjobenqueuetime, odjobstarttime, odjobcompletiontime, " +
            "odjobstatus, odalgorithmreturncode, oddataselectionreturncode, odjobhighesterrorclass, " +
            "odjobpid, odjobcpu_util, odjobmem_util, odjobio_util, odjoboutput " +
            "FROM ondemandjob WHERE odjobid = %s", (odJobId,) )
        row = cur.fetchone()
        
        if row is None:
            return createErrorResponse(404, "Not Found", "No job found for ID: " + str(odJobId))
        # print(row)
        
        resultObj = {}
        
        resultObj['jobId'] = row[0]
        resultObj['jobSpec'] = row[1]
        resultObj['jobEnqueueTime'] = row[2].strftime("%Y-%m-%dT%H:%M:%S.%fZ") if row[2] is not None else None
        resultObj['jobStartTime'] = row[3].strftime("%Y-%m-%dT%H:%M:%S.%fZ") if row[3] is not None else None
        resultObj['jobCompletionTime'] = row[4].strftime("%Y-%m-%dT%H:%M:%S.%fZ") if row[4] is not None else None
        resultObj['jobStatus'] = row[5]
        resultObj['algorithmReturnCode'] = row[6]
        resultObj['dssReturnCode'] = row[7]
        resultObj['jobHighestErrorClass'] = row[8]
        resultObj['jobPid'] = row[9]
        resultObj['jobCpuUtil'] = row[10]
        resultObj['jobMemUtil'] = row[11]
        resultObj['jobIoUtil'] = row[12]
        resultObj['jobOutput'] = row[13]
        
        print(resultObj)

        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": '*'
            },
            "body": json.dumps(resultObj)
        }
        
        cur.close()
        conn.rollback()

        return response
    
    except Exception as e:
        conn.rollback()

        print("ERROR encountered:", e)
        return createErrorResponse(500, "Internal error", 
            "An exception was encountered, please try again later.")
    finally:
        conn.close()