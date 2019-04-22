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


print('Loading getDataCubeMetadata')

def lambda_handler(event, context):
    
    try:
        datacubeId = int(event['pathParameters']['datacubeid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing or invalid Data Cube ID")
    
    print("datacubeId:", datacubeId)
    
    dataCube = {
        "id" : datacubeId,
        "name" : {},
        "dimensions" : {},
        "measures"   : {}
    }
    
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
        cur = conn.cursor()

        cur.execute("SELECT e_dimensionListName FROM EnterpriseDimensionList l WHERE l.e_dimensionListId = %s", (datacubeId,))
        row = cur.fetchone()
        
        if row is None:
            return createErrorResponse(404, "Not Found", "No Data Cube found for ID: " + datacubeId)
            
        dataCube['name']=row[0]
        
        # get dataCube dimensions
        sql = """SELECT e_dimensionOrder, e_dimensionName, e_dimensionDataPartition, e_dimensionEnd, e_dimensionStorageSize
            FROM enterpriseOrderedDimension o, enterpriseDimension d
            WHERE o.e_dimensionlistid = %s AND o.e_dimensionId = d.e_dimensionId
            ORDER BY e_dimensionOrder
            """
        cur.execute(sql, (datacubeId,))
        rows = cur.fetchall()
        
        for row in rows:
            #print (row)
            dataCube['dimensions'][row[1]] = {
                "order": row[0], 
                "dataPartition": row[2], 
                "dimensionSize": row[3], 
                "storageSize":   row[4]
            }

        #get dataCube measures
        sql = """SELECT 
                    measureName,
                    x.h_transformType,
                    x.h_bitoffset,
                    x.h_bitlength,
                    x.h_scalefactorreference,
                    x.h_addoffsetreference,
                    a.h_arrayname, 
                    a.h_datatype,
                    g.h_groupName,
                    productShortName,
                    platformName
                FROM
                    enterprisemeasure m,
                    measure_h_array_xref x,
                    hdf5_array a,
                    hdf5_group g,
                    productDescription p,
                    productPlatform_xref x2,
                    platForm f
                WHERE
                    e_dimensionlistid = %s
                    and m.measureId = x.measureId
                    and a.h_arrayId = x.h_arrayId
                    and a.h_groupId = g.h_groupId
                    and g.productId = p.productId
                    and p.productId = x2.productId
                    and x2.platformId = f.platformId
                ORDER BY platformName, measurename
            """
        cur.execute(sql, (datacubeId,))
        rows = cur.fetchall()
        
        for row in rows:
            #print (row)
            dataCube["measures"][row[0]] = { 
                "h_transformType": row[1],
                "h_bitoffset": row[2],
                "h_bitlength": row[3],
                "h_scalefactorreference": row[4],
                "h_addoffsetreference":   row[5],
                "h_arrayname": row[6],
                "h_datatype":  row[7],
                "h_groupName": row[8],
                "productShortName": row[9],
                "platformName": row[10]
            }
        
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
                "datacube": dataCube
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
    
