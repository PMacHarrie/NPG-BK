"""
    Author: Hieu Phung, Peter MacHarrie; SOLERS INC
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
        print('Connecting to Postgres...')
        
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        query = '''
            SELECT
                eventJson->>'wfo' wfo,
                to_char(startTime, 'YYYY-MM-DD HH24:MI:SS'),
                to_char(endTime, 'YYYY-MM-DD HH24:MI:SS'),
                eventType,
                to_char(eventTime, 'YYYY-MM-DD HH24:MI:SS'),
                st_asText(eventLocationPoint),
                lsrId
            FROM
                (
                SELECT
                    CASE WHEN productShortName like '%CMIPM1%' then 'M1' else 'M2' end Meso_inst,
                    fileSpatialArea,
                    MIN(fileStartTime) startTime,
                    MAX(fileEndTime) endTime
                    FROM filemetadata f, productDescription p
                WHERE
                    productShortName in ('ABI_L2_CMIPM1_C10', 'ABI_L2_CMIPM2_C10')
                    AND p.productId = f.productId
                    AND fileEndTime >= now() - interval '7' day
                GROUP BY 1, 2
                ORDER BY 1, 2
                ) t,
                localStormReport r
            WHERE meso_inst in ( 'M1', 'M2')
                AND fileSpatialArea is not null
                AND st_intersects(r.eventLocationPoint, t.fileSpatialArea) = True
                AND r.eventTime BETWEEN t.startTime AND t.endTime
            ORDER BY
                wfo,
                eventTime
        '''
    
        cur.execute(query)
        rows = cur.fetchall()
        
        print("# of rows:", len(rows))
        #print (rows)
    
        lsr = {}
        
        for row in rows:
            #print (lsr)
            # print(row)
            if row[0] in lsr:
                lsr[row[0]]["eventCounts"]["total"] += 1
                if row[3] in lsr[row[0]]["eventCounts"]:
                    lsr[row[0]]["eventCounts"][row[3]] += 1
                else:
                    lsr[row[0]]["eventCounts"][row[3]] = 1
                lsr[row[0]]["storms"].append({
                    "eventType" : row[3], 
                    "location"  : row[5], 
                    "eventTime" : row[4],
                    "lsrId": row[6]
                })
            else:
                lsr[row[0]] = { "eventCounts" : {"total": 1, row[3]: 1} }
                lsr[row[0]]["storms"] = [{
                    "eventType" : row[3], 
                    "location"  : row[5], 
                    "eventTime" : row[4], 
                    "lsrId": row[6]
                }]
                
        # print ("Storms observed by GOES-16 Mesoscale 2")
        # i = 0
        # for wfo in lsr:
        #     print (wfo, "Total Observed Storm Reports->", lsr[wfo]['numberOfStormReports'])
        #     i=1
        #     myEvents = {}
        #     for event in lsr[wfo]['storms']:
        #         eventType = event['eventType']
        #         if eventType in myEvents:
        #             myEvents[eventType] += 1
        #         else:
        #             myEvents[eventType] = 1
        #     for event in myEvents:
        #         print ("events:", event, myEvents[event])
            
        #     print ("First 5 events:")
            
        #     for event in lsr[wfo]['storms']:
        #         if i <= 5:
        #             print (i, "  ", event['location'], event['eventTime'], event['eventType'])
        #             i+=1
        
        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": '*'
            },
            "body": json.dumps({
                "result": lsr
            })
        }
        
        return response
        
    except Exception as e:
        conn.rollback()

        print("ERROR encountered:", e)
        return createErrorResponse(500, "Internal error", 
            "An exception was encountered, please try again later.")
    finally:
        conn.close()
