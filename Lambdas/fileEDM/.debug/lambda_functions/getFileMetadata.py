"""
    Author: Hieu Phung, Peter MacHarrie; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import os
import json
import elasticsearch
import certifi

from ndeutil import *


print('Loading getFileMetadata')

def lambda_handler(event, context):
    
    print(event)
    
    try:
        fileId = int(event['pathParameters']['fileid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "{fileid} missing or incorrectly formatted")
    
    print("fileId:", fileId)

    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port = os.environ['ES_PORT'],
        use_ssl = True,
        verify_certs = True,
        ca_certs = certifi.where()
    )
        
    try:
        res = es.get(
            index = "file", 
            doc_type = "_doc", 
            id = fileId, 
            _source_include = ['edmCore','objectMetadata']
        )
        fileJson = res['_source']
        
        print(fileJson)
        response = {
            "isBase64Encoded": True,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": '*'
            },
            "body": json.dumps({
                "result" : fileJson
            })
        }
        return response
        
    except elasticsearch.exceptions.NotFoundError as e:
        s = "No results for File ID: " + str(fileId)
        print(s)
        return createErrorResponse(404, "Not Found", s)
    except Exception as e:
        print("ERROR while querying:", str(e))
        return createErrorResponse(500, "Error while querying", str(e))
        
        