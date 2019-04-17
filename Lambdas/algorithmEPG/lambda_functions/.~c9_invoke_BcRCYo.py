
import os
import json
import certifi
import elasticsearch

from ndeutil import *


print('Loading getProductionRule')

def lambda_handler(event, context):
    
    try:
        algoIdParam = int(event['pathParameters']['algoid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing or invalid Algorithm ID")
        
    try:
        ruleId = int(event['pathParameters']['ruleid'])
    except KeyError as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing Production Rule ID")
    except ValueError as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Production Rule ID must be integer")
    
    print("ruleId:", ruleId)

    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port=os.environ['ES_PORT'],
        use_ssl=True,
        verify_certs=True,
        ca_certs=certifi.where())

    try:
        res = es.get(index="productionrule", doc_type="_doc", id=ruleId)

        ruleObj = res['_source']
        print(ruleObj)
        
        resAlgo = es.get(index="algorithm", doc_type="_doc", id=algoIdParam)
        algoObj = resAlgo['_source']
        print(algoObj)
        
        if algoObj.get('algorithmName') != ruleObj.get('Algorithm').get('algorithmName'):
            return createErrorResponse(400, "Validation Error", 
                "algoid URI (" + algoIdParam + ") does not match Algorithm ID registered for " +
                ruleObj.get('Algorithm').get('algorithmName') )
        
        response = {
            "isBase64Encoded": True,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "result" : ruleObj
            })
        }
        return response
    except elasticsearch.exceptions.NotFoundError as e:
        s = "No results for Production Rule ID: " + str(ruleId)
        print(s)
        return createErrorResponse(404, "Not Found", s)
    except Exception as e:
        print("ERROR while querying:", str(e))
        return createErrorResponse(500, "Error while querying", str(e))

