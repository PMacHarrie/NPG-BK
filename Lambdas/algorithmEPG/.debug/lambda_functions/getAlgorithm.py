"""
    Author: Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import os
import json
import certifi
import elasticsearch

from ndeutil import *


print('Loading getAlgorithm')

def lambda_handler(event, context):
    
    try:
        algoId = int(event['pathParameters']['algoid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing or invalid Algorithm ID")
    
    print("algoId:", algoId)

    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port = os.environ['ES_PORT'],
        use_ssl = True,
        verify_certs = True,
        ca_certs = certifi.where()
    )

    try:
        res = es.get(
            index = "algorithm", 
            doc_type = "_doc", 
            id = algoId
        )
        
        algoJson = res['_source']
        print(algoJson)
        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "result" : algoJson
            })
        }
        return response
    except elasticsearch.exceptions.NotFoundError as e:
        s = "No results for Algorithm ID: " + str(algoId)
        print(s)
        return createErrorResponse(404, "Not Found", s)
    except Exception as e:
        print("ERROR while querying:", str(e))
        return createErrorResponse(500, "Internal error", str(e))

