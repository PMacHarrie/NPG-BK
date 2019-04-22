"""
    Author: Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import os
import math
import json
import elasticsearch
import certifi
import psycopg2

from ndeutil import *


print('Loading registerAlgorithm')

def lambda_handler(event, context):
    print(event)
    
    # get the msg body for proxy integration case
    if 'resource' in event:
        if event.get('body') is None:
            return createErrorResponse(400, "Validation error", 
                "body (algorithm json) required")
        if isinstance(event.get('body'), dict):
            inputBody = event.get('body')
        else:
            inputBody = json.loads(event.get('body'))
    else:
        inputBody = event
    
    algo = inputBody.get('Algorithm')
    print("algo", algo)
    if algo is None:
        return createErrorResponse(400, "Validation Error", "body must contain Algorithm")
    
    # did the client request a create or update
    isUpdate = False
    if 'resource' in event and event['resource'] == '/algorithm/{algoid}':
        isUpdate = True
    
    print('isUpdate?', isUpdate)
    
    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        # check for existing
        cur.execute( "SELECT algorithmid FROM algorithm WHERE algorithmname = %s AND algorithmversion = %s", 
            (algo.get('algorithmName'), algo.get('algorithmVersion')) )
        row = cur.fetchone()
        algoId = row[0] if row is not None else None
        cur.close()
        
        print("algoId", algoId)
        
        # if isUpdate and {id} is None, proceed to register normally
        if not isUpdate and algoId is not None:
            s = "Algorithm already exists with name/version: " + algo['algorithmName'] + " " + algo['algorithmVersion']
            print(s)
            return createErrorResponse(409, "Conflict: resource exists", s)
        elif isUpdate:
            if algoId is None:
                s = ("Unable to update; algorithm with name/version NOT registered: " +
                        algo['algorithmName'] + " " + algo['algorithmVersion'])
                print(s)
                return createErrorResponse(404, "Not Found", s)
            elif algoId != int(event['pathParameters']['algoid']):
                s = "Unable to update; requested URI {algoid} does not match existing Algorithm ID: " + str(algoId)
                print(s)
                return createErrorResponse(400, "Validation error", s)
    
        # start processing the input, rollback if any sections fail
        try:
            algoId = doAlgorithm(conn, algo, algoId, isUpdate)
        except ValueError as e:
            print("Exception in doAlgorithm:", e)
            return createErrorResponse(400, "Validation error", str(e))
            
        print("algoId", algoId)
        
        if 'AlgorithmParameters' in algo:
            try:
                doAlgoParameters(conn, algo['AlgorithmParameters'], algoId, isUpdate)
            except ValueError as e:
                print("Exception in doAlgoParameters:", e)
                return createErrorResponse(400, "Validation error", str(e))
                
        if 'AlgorithmInputs' in algo:
            try:
                doAlgoInputs(conn, algo['AlgorithmInputs'], algoId, isUpdate)
            except ValueError as e:
                print("Exception in doAlgoInputs:", e)
                return createErrorResponse(400, "Validation error", str(e))
        
        if 'AlgorithmOutputs' in algo:
            try:
                doAlgoOutputs(conn, algo['AlgorithmOutputs'], algoId, isUpdate)
            except ValueError as e:
                print("Exception in doAlgoOutputs:", e)
                return createErrorResponse(400, "Validation error", str(e))
                
        esSuccess = createElasticDoc(algoId, json.dumps({"Algorithm": algo}))
        
        if esSuccess:
            conn.commit()
        else:
            conn.rollback()
            return create_error_response(500, "Internal Error", 
                "Error encountered updating ES: " + str(e))
        
        response = {
            "isBase64Encoded": False,
            "statusCode": 200 if isUpdate else 201,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({
                "message": "Algorithm was " + ("updated" if isUpdate else "created") + " successfully",
                "algorithmId": algoId
            })
        }
        return response
    except Exception as e:
        print("ERROR: " + str(e))
        return createErrorResponse(500, "Internal error", "An exception was encountered, please try again later")
    finally:
        conn.rollback()
        conn.close()
    
def doAlgorithm(conn, algo, algoId, isUpdate):
    print(">> Starting doAlgorithm")
    
    # fix algorithmCommandPrefix for single space issue
    if len(algo['algorithmCommandPrefix']) == 0:
        algo['algorithmCommandPrefix'] = ' '
    
    # process UNDEFINED log message context
    if algo['algorithmLogMessageContext'] == 'UNDEFINED':
        algo['algorithmLogMessageContext'] = '.'
    
    cur = conn.cursor()
    
    if not isUpdate:
        cur.execute("INSERT INTO algorithm (algorithmid, algorithmExecutableName) VALUES "
            "(nextval('s_algorithm\'), %s) RETURNING algorithmid", (algo['algorithmExecutableName'],))
        algoId = cur.fetchone()[0]
        print("new algoId", algoId)
    
    excludeFields = ['algorithmExecutableName', 'AlgorithmParameters', 'AlgorithmInputs', 'AlgorithmOutputs']
    fieldsSnippet = ', '.join([f + "=%(" + f + ")s" for f in list(algo.keys()) if f not in excludeFields])
    print(fieldsSnippet)
    
    cur.execute("UPDATE algorithm SET " + fieldsSnippet + " WHERE algorithmid = %(algorithmid)s",
        {'algorithmid': algoId, **algo})

    cur.close()
    
    print(">> Finished doAlgorithm")
    return algoId


def doAlgoParameters(conn, algoParams, algoId, isUpdate):
    print(">> Starting doAlgoParameters")
    
    cur = conn.cursor()
    for algoParam in algoParams:
        skip = False
        
        cur.execute("SELECT algoParameterName FROM algoParameters WHERE algoParameterName = %s AND algorithmId = %s",
            (algoParam.get('algoParameterName'), algoId))
        row = cur.fetchone()
        if row is not None and isUpdate:
            cur.execute("UPDATE AlgoParameters SET algoParameterDataType = %s WHERE algoParameterName = %s AND " +
                "algorithmId = %s", (algoParam.get('algoParameterDataType'), algoParam.get('algoParameterName'), algoId))
            print("Updated AlgoParam: ", algoParam.get('algoParameterName'))
        else:
            cur.execute("INSERT INTO AlgoParameters (algoParameterId, algorithmId, algoParameterName, algoParameterDataType) "
                "VALUES (nextval('s_algoparameters\'), %s, %s, %s)", 
                (algoId, algoParam.get('algoParameterName'), algoParam.get('algoParameterDataType')))
            print("Added AlgoParam: ", algoParam.get('algoParameterName'))
    
    cur.close()
    print(">> Finished doAlgoParameters")
    
    
def doAlgoInputs(conn, algoInputs, algoId, isUpdate):
    print(">> Starting doAlgoInputs")
    
    cur = conn.cursor()
    for algoInput in algoInputs:
        cur.execute("SELECT productId FROM productDescription WHERE productShortName = %s", 
            (algoInput.get('productShortName'),))
        rows = cur.fetchall()
        
        if len(rows) == 0: 
            s = "ERROR: Algorithm Input Product: " + algoInput.get('productShortName') + " is not registered"
            print(s)
            raise ValueError(s)

        for row in rows:
            productId = row[0]
            skip = False
            if isUpdate:
                cur.execute("SELECT * FROM algoinputprod WHERE productid = %s AND algorithmid = %s",
                    (productId, algoId))
                row = cur.fetchone()
                if row is not None: # already exists
                    print("AlgoInput already exists: ", algoInput.get('productShortName'), "productId:", productId)
                    skip = True 
                    
            if not skip:
                cur.execute("INSERT INTO algoinputprod (productId, algorithmId) VALUES (%s, %s)", 
                    (productId, algoId))
                print("Added AlgoInput: ", algoInput.get('productShortName'), "productId: ", productId)

    cur.close()
    print(">> Finished doAlgoInputs")
    
    
def doAlgoOutputs(conn, algoOutputs, algoId, isUpdate):
    print(">> Starting doAlgoOutputs")
    
    cur = conn.cursor()
    for algoOutput in algoOutputs:
        cur.execute("SELECT productId FROM productDescription WHERE productShortName = %s", 
            (algoOutput.get('productShortName'),))
        rows = cur.fetchall()
        
        if len(rows) == 0: 
            s = "ERROR: Algorithm Output Product: " + algoOutput.get('productShortName') + " is not registered"
            print(s)
            raise ValueError(s)

        for row in rows:
            productId = row[0]
            skip = False
            if isUpdate:
                cur.execute("SELECT * FROM algooutputprod WHERE productid = %s AND algorithmid = %s",
                    (productId, algoId))
                row = cur.fetchone()
                if row is not None:
                    print("AlgoOutput already exists: ", algoOutput.get('productShortName'), "productId:", productId)
                    skip = True 
                    
            if not skip:
                cur.execute("INSERT INTO algooutputprod (productId, algorithmId) VALUES (%s, %s)", 
                    (productId, algoId))
                print("Added AlgoOutput: ", algoOutput.get('productShortName'), "productId:", productId)
    
    cur.close()
    print(">> Finished doAlgoOutputs")
    
    
def createElasticDoc(algoId, algoJson):
    print(">> Starting createElasticDoc")
    
    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port = os.environ['ES_PORT'],
        use_ssl = True,
        verify_certs = True,
        ca_certs = certifi.where())
    
    try:
        res = es.index(
            index = 'algorithm',
            doc_type = '_doc',
            id = algoId,
            body = algoJson
        )
        
        if res['result'] == 'created' and res['_id'] == str(algoId):
            print("Successfully added to Index:", res)
            return True
        elif res['result'] == 'updated' and res['_id'] == str(algoId):
            print("Successfully updated Index:", res)
            return True
        else:
            print("Failed to add to Index:", res)
            return False
    except Exception as e:
        print("ERROR: Failed to add to Elastic Search:", str(e))
        return False
    finally: 
        print(">> Finished createElasticDoc")