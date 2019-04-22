"""
    Author: Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import os
import json
import elasticsearch
import certifi
import psycopg2


print('Loading function')
#####################TODO: NEEDS REWORK ####################
#####################TODO: NEEDS REWORK ####################
#####################TODO: NEEDS REWORK ####################
#####################TODO: NEEDS REWORK ####################
def lambda_handler(event, context):
    
    #get id from event path
    prodId = int(event['pathParameters']['id'])
    
    successSql = deleteFromSql(prodId)
    
    successEs = deleteFromEs(prodId)
    
    response = {}
    if successSql or successEs:
        response = {
            "isBase64Encoded": True,
            "statusCode": 200,
            "headers": {  },
            "body": "{}"
        }
    else:
        response = {
            "isBase64Encoded": True,
            "statusCode": 500,
            "headers": {  },
            "body": '{"errorMessage": "Product DNE"}'
        }
        
    return response

def deleteFromSql(prodId):
    
    try:
        delPlatformXRef = 'DELETE FROM productplatform_xref where productid = ' + str(prodId) + ';'
        delIngestProcessStep = 'DELETE FROM ingestprocessstep where productid = ' + str(prodId) + ';'
        delQuality = 'DELETE FROM productqualitysummary where productid = ' + str(prodId) + ';'
        delProd = 'DELETE FROM productdescription where productid = ' + str(prodId) + ';'
        
        #connect to db
        conn = psycopg2.connect(
            host=os.environ['RD_HOST'],
            dbname=os.environ['RD_DBNM'],
            user=os.environ['RD_USER'],
            password=os.environ['RD_PSWD']
            )
        cursor = conn.cursor()
        
        #execute delete
        cursor.execute(delPlatformXRef)
        cursor.execute(delIngestProcessStep)
        cursor.execute(delQuality)
        cursor.execute(delProd)
        rowCount = cursor.rowcount
        conn.commit()
        conn.close()
        
        if rowCount > 0:
            return True
        else:
            return False
    except:
        return False


def deleteFromEs(prodId):
    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port=os.environ['ES_PORT'],
        use_ssl=True,
        verify_certs=True,
        ca_certs=certifi.where())
    
    try:
        es.delete(index="product", doc_type="_doc", id=prodId)
        return True
    except elasticsearch.exceptions.NotFoundError:
        print("ES record not found")
        return False