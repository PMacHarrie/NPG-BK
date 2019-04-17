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


print('Loading getProductionRule')

def lambda_handler(event, context):
    
    try:
        algoId = int(event['pathParameters']['algoid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing or invalid Algorithm ID")
    print("algoId", algoId)
    
    try:
        ruleId = int(event['pathParameters']['ruleid'])
    except KeyError as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing Production Rule ID")
    except ValueError as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Production Rule ID must be integer")
    print("ruleId", ruleId)
    
    print("ruleId:", ruleId)

    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port = os.environ['ES_PORT'],
        use_ssl = True,
        verify_certs = True,
        ca_certs = certifi.where())

    try:
        try:
            res = es.get(
                index = "productionrule", 
                doc_type = "_doc", 
                id = ruleId
            )

            ruleObj = res['_source']
            print(ruleObj)
        except elasticsearch.exceptions.NotFoundError as e:
            s = "No results for Production Rule ID: " + str(ruleId)
            print(s)
            return createErrorResponse(404, "Not Found", s)
        
        try:
            resAlgo = es.get(
                index = "algorithm", 
                doc_type = "_doc",
                id = algoId)
            algoObj = resAlgo['_source']
            print(algoObj)
        except elasticsearch.exceptions.NotFoundError as e:
            s = "No results for Algorithm ID: " + str(algoId)
            print(s)
            return createErrorResponse(404, "Not Found", s)
        
        if 'Algorithm' in algoObj:
            algoObj = algoObj.get('Algorithm')
        if 'ProductionRule' in ruleObj:
            ruleObj = ruleObj.get('ProductionRule')
        
        if algoObj.get('algorithmName') != ruleObj.get('Algorithm').get('algorithmName') or \
            algoObj.get('algorithmVersion') != ruleObj.get('Algorithm').get('algorithmVersion'):
            return createErrorResponse(400, "Validation Error", 
                "Algorithm for algoId URI (" + str(algoId) + 
                    ") does not match Algorithm for Production Rule Id: " + str(ruleId))
        
        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "result" : ruleObj
            })
        }
        return response
    
    except Exception as e:
        print("ERROR while querying:", str(e))
        return createErrorResponse(500, "Internal error", str(e))

