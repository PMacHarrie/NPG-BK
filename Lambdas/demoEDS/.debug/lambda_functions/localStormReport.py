
import os
import json
import psycopg2


def lambda_handler(event, context):
    
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
        if row[0] in lsr:
            lsr[row[0]]["numberOfStormReports"] += 1
            lsr[row[0]]["storms"].append({
                "eventType" : row[3], 
                "location"  : row[5], 
                "eventTime" : row[4],
                "lsrId": row[6]
            })
        else:
            lsr[row[0]] = { "numberOfStormReports" : 1 }
            lsr[row[0]]["storms"] = [{
                "eventType" : row[3], 
                "location"  : row[5], 
                "eventTime" : row[4], 
                "lsrId": row[6]
            }]
    
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
