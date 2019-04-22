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


print('Loading getProduct')

def lambda_handler(event, context):
    print(event)
    
    try:
        productId = int(event['pathParameters']['productid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "{productid} missing or incorrectly formatted")
    
    print("productId:", productId)

    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port = os.environ['ES_PORT'],
        use_ssl = True,
        verify_certs = True,
        ca_certs = certifi.where()
    )

    print('Querying Elasticsearch...')
    
    try:
        res = es.get(
            index = "product", 
            doc_type = "_doc", 
            id = productId
        )

        productJson = res['_source']
        print(productJson)
        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": '*'
            },
            "body": json.dumps({
                "result": productJson
            })
        }
        return response
    except elasticsearch.exceptions.NotFoundError as e:
        s = "No results for Product ID: " + str(productId)
        print(s)
        return createErrorResponse(404, "Not Found", s)
    except Exception as e:
        print("ERROR while querying:", str(e))
        return createErrorResponse(500, "Internal error", "Error while querying: " + str(e))


