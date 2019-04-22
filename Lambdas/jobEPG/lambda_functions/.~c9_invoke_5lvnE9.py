import os
import json
import psycopg2
from datetime import datetime
import re

from ndeutil import *


def lambda_handler(event, context):
    
    if 'resource' in event:
        if event.get('body') is None:
            return createErrorResponse(400, "Validation error", 
                "body (search json) required")
        if isinstance(event.get('body'), dict):
            inputBody = event.get('body')
        else:
            inputBody = json.loads(event.get('body'))
    else:
        inputBody = event
    
    inQuery = inputBody.get('query')
    if inQuery is None:
        return createErrorResponse(400, "Validation error", "Must specify query")
    print("inQuery", inQuery)
    
    whereClauses = []
    queryParams = {}
    
    # validate and format jobStatus
    if inQuery.get('jobStatus') is not None:
        if not isinstance(inQuery.get('jobStatus'), list):
            inQuery['jobStatus'] = [inQuery.get('jobStatus')]
        for status in inQuery.get('jobStatus'):
            if status not in ['QUEUED', 'COMPLETE', 'FAILED', 'RUNNING', 'COPYINPUT']:
                return createErrorResponse(400, "Validation error", "Invalid jobStatus: " + status)
        whereClauses.append(" odJobStatus in %(jobStatus)s ")
        queryParams['jobStatus'] = tuple(inQuery['jobStatus'])
    print("whereClauses", whereClauses)
    
    # validate and format algorithm
    algo = inQuery.get('algorithm')
    if algo is not None:
        if algo.get('name') is None or algo.get('version') is None:
            return createErrorResponse(400, "Validation error", "Both name and version required for algorithm")
        whereClauses.append(" odalgorithmname = %(algorithmName)s ")
        whereClauses.append(" odalgorithmversion = %(algorithmVersion)s ")
        queryParams['algorithmName'] = algo.get('name')
        queryParams['algorithmVersion'] = algo.get('name')
            
    # validate and format timeRanges
    if inQuery.get('enqueueTime') is not None:
        validateTimeRange(inQuery.get('enqueueTime'), 'enqueueTime', 'odjobenqueuetime', whereClauses, queryParams)
    if inQuery.get('startTime') is not None:
        validateTimeRange(inQuery.get('startTime'), 'startTime', 'odjobstarttime', whereClauses, queryParams)
    if inQuery.get('completionTime') is not None:
        validateTimeRange(inQuery.get('completionTime'), 'completionTime', 'odjobcompletiontime', whereClauses, queryParams)
        
    print("whereClauses", whereClauses)
    print("queryParams", queryParams)
    
    # validate the "result" portion of the request
    inResult = inputBody.get('result')
    print("inResult", inResult)
    if inResult is None:
        print("No result output result format specified, using defaults (enqueueTime desc)")
        orderBy = " ORDER BY odjobenqueuetime desc "
        limit = " LIMIT 100 "
    else:
        if inResult.get('sort') is not None:
            if not isinstance(inResult.get('sort'), list):
                return createErrorResponse(400, "Validation error", "sort should be an array of " + 
                    "{\"attr1\": \"asc\" | \"desc\"}")
            temp = []
            for sortItem in inResult.get('sort'):
                (attrName, sortDir), = sortItem.items()
                if not re.match('^\w+$', attrName):
                    return createErrorResponse(400, "Validation error", "invalid sort attr: " + attrName)
                if sortDir not in ['asc', 'desc']:
                    return createErrorResponse(400, "Validation error", "sort value must be: asc | desc")
                temp.append(attrName + " " + sortDir)
            orderBy = " ORDER BY " + ', '.join(temp)

        if inResult.get('limit') is not None:
            limit = inResult.get('limit')
            if not isinstance(limit, dict):
                return createErrorResponse(400, "Validation error", "limit must contain from and size keys")
            if limit.get('from') is None or limit.get('size') is None:
                return createErrorResponse(400, "Validation error", "from and size are required")
            if not isinstance(limit.get('from'), int) or not isinstance(limit.get('size'), int):
                return createErrorResponse(400, "Validation error", "from and size should be numeric")
            limit = " LIMIT " + str(limit.get('size')) + " OFFSET " + str(limit.get('from'))
    
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host=os.environ['RD_HOST'],
            dbname=os.environ['RD_DBNM'],
            user=os.environ['RD_USER'],
            password=os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        cur.execute("SELECT odjobid, odalgorithmname, odalgorithmversion, odjobstatus, odjobenqueuetime, " + 
            "odjobstarttime, odjobcompletiontime FROM ondemandjob WHERE " + ' AND '.join(whereClauses) +
            orderBy + limit, queryParams)
        rows = cur.fetchall()

        resultObj = {}
        
        for row in rows:
            resultArray.append( 
                {"jobId" : row[0], 
                 "algorithmName" : row[1], 
                 "algorithmVersion" : row[2],
                 "jobStatus": row[3],
                 "enqueueTime": row[4].strftime("%Y-%m-%dT%H:%M:%S"),
                 "startTime": row[5].strftime("%Y-%m-%dT%H:%M:%S"),
                 "completionTime": row[6].strftime("%Y-%m-%dT%H:%M:%S")
                } )
            
        print(resultObj)

        cur.close()
        conn.rollback()

        response = {
            "isBase64Encoded": True,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "jobsCountByAlgoByStatus" : resultObj
            })
        }
        return response
    except psycopg2.Error as e:
        print(e)
    except Exception as e:
        conn.rollback()
        
        print("ERROR encountered querying Jobs:", e)
        return createErrorResponse(500, "Internal error", 
            "An exception was encountered, please try again later.")
    finally:
        conn.close()
    
    
def validateTimeRange(timeInputVal, timeParamName, timeColumnName, whereClauses, queryParams):
    
    if not isinstance(timeInputVal, dict):
        return createErrorResponse(400, "Validation error", timeParamName + " must contain lte or gte keys (or both)")
    
    temp = []
    try:
        if timeInputVal.get('lte') is not None:
            lte = datetime.strptime(timeInputVal.get('lte'),'%Y%m%dT%H%M%S.%fZ')
            temp.append(" " + timeColumnName + " <= %(" + timeParamName + "Lte)s ")
            queryParams[timeParamName + "Lte"] = lte
        if timeInputVal.get('gte') is not None:
            gte = datetime.strptime(timeInputVal.get('gte'),'%Y%m%dT%H%M%S.%fZ')
            temp.append(" " + timeColumnName + " >= %(" + timeParamName + "Gte)s ")
            queryParams[timeParamName + "Gte"] = gte
            
        if lte is None and gte is None:
            return createErrorResponse(400, "Validation error", timeParamName + " must contain lte or gte keys (or both)")
            
        if len(temp) == 2 and lte < gte:
            whereClauses.append('OR'.join(temp))
        else:
            whereClauses.extend(temp)
        
        print(whereClauses)
    except ValueError as ve:
        return createErrorResponse(400, "Validation error",  "startTime and endTime must be formatted as: YYYYMMDDTHHmmSS.sssZ")
