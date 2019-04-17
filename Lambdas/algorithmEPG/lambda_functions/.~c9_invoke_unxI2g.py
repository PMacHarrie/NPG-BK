
import os
import math
import json
import elasticsearch
import certifi
import psycopg2
import xml.etree.ElementTree as ET
from datetime import datetime

from ndeutil import *


print('Loading registerProductionRule')

def lambda_handler(event, context):
    print(event)

    try:
        algoIdParam = int(event['pathParameters']['algoid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "Missing or invalid Algorithm ID")
        
    # get the msg body for proxy integration case
    if 'resource' in event:
        if event['body'] is None:
            return createErrorResponse(400, "Validation error", 
                "body (production rule json) required")
        if isinstance(event['body'], dict):
            inputBody = event['body']
        else:
            inputBody = json.loads(event['body'])
    else:
        inputBody = event
    
    prRule = inputBody.get('ProductionRule')
    if prRule is None:
        return createErrorResponse(400, "Validation error", "JSON root should be 'ProductionRule'")

    print("prRule", prRule)
    
    # did the client request a create or update
    isUpdate = False
    if 'resource' in event and event['resource'] == '/algorithm/{algoid}/rule/{ruleid}':
        isUpdate = True
    
    print('isUpdate?', isUpdate)
    
    # check if algorithm has been registered
    algo = prRule.get('Algorithm')
    if algo is None:
        return createErrorResponse(400, "Validation error", "No Algorithm defined in ProductionRule JSON")
    
    try:
        conn = psycopg2.connect(
                host=os.environ['RD_HOST'],
                dbname=os.environ['RD_DBNM'],
                user=os.environ['RD_USER'],
                password=os.environ['RD_PSWD']
                )
                
        cur = conn.cursor()
        cur.execute( "SELECT algorithmid FROM algorithm WHERE algorithmname = %s AND algorithmversion = %s", 
            (algo.get('algorithmName'), algo.get('algorithmVersion')) )
        row = cur.fetchone()
        if row is None:
            return createErrorResponse(400, "Validation error", 
                "Algorithm not registered: " + algo.get('algorithmName') + " " + algo.get('algorithmVersion'))
        else:
            algoId = row[0]
            # print("algoId", algoId)
            # print(event.get('pathParameters').get('algoid'))
            if algoId != algoIdParam:
                return createErrorResponse(400, "Validation error", "URI {algoid} does not match registered AlgorithmId")
            print("Verified algorithm registererd", algo.get('algorithmName'), algo.get('algorithmVersion'))
            
        cur.execute( "SELECT prid FROM productionrule WHERE prrulename = %s AND algorithmid = %s", 
            (prRule.get('prRuleName'), algoId) )
        row = cur.fetchone()
        prRuleId = row[0] if row is not None else None
        cur.close()
        print("prRuleId", prRuleId)
        
        # if isUpdate and {id} is None, proceed to register normally
        if not isUpdate and prRuleId is not None:
            s = ("Production rule already exists with name: " + prRule.get('prRuleName') + 
                " for Algorithm: " + algo.get('algorithmName'))
            print(s)
            return createErrorResponse(409, "Conflict: resource exists", s)
        elif isUpdate:
            if prRuleId is None:
                s = ("Unable to update; production rule with name: " + prRule.get('prRuleName') +
                        " for Algorithm: " + algo.get('algorithmName') + " NOT registered")
                print(s)
                return createErrorResponse(404, "Not Found", s)
            elif prRuleId != int(event['pathParameters']['ruleid']):
                s = "Unable to update; requested URI {ruleid} does not match existing rule ID: " + str(prRuleId)
                print(s)
                return createErrorResponse(400, "Validation error", s)
        
        # start processing the input, rollback if any sections fail
        try:
            prRuleId = doPrRule(conn, prRule, prRuleId, isUpdate, algoId)
        except ValueError as e:
            print("Exception in doPrRule:", e)
            return createErrorResponse(400, "Validation error", str(e))
        print("prRuleId", prRuleId)
        
        try:
            doPrParameters(conn, prRule.get('ProductionRuleParameters'), prRuleId, isUpdate, algoId)
        except ValueError as e:
            print("Exception in doPrRule:", e)
            return createErrorResponse(400, "Validation error", str(e))
            
        try:
            doPrInputs(conn, prRule.get('ProductionRuleInputs'), prRuleId, isUpdate, algoId)
        except ValueError as e:
            print("Exception in doPrInputs:", e)
            return createErrorResponse(400, "Validation error", str(e))
            
        try:
            doPrOutputs(conn, prRule.get('ProductionRuleOutputs'), prRuleId, algoId)
        except ValueError as e:
            print("Exception in doPrOutputs:", e)
            return createErrorResponse(400, "Validation error", str(e))
        
        esSuccess = createElasticDoc(prRuleId, json.dumps(prRule))
        
        if esSuccess:
            conn.commit()
        else:
            conn.rollback()
            
            return create_error_response(500, "Internal Error", 
                "Error encountered updating ES: " + str(e))
        
        response = {
            "isBase64Encoded": True,
            "statusCode": 200 if isUpdate else 201,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({
                "message": "Production Rule was " + ("updated" if isUpdate else "created") + " successfully",
                "prRuleId": prRuleId
            })
        }
        return response
    except Exception as e:
        print("ERROR: " + str(e))
        return createErrorResponse(500, "Internal error", "An exception was encountered, please try again later")
    finally:
        conn.rollback()
        conn.close()
    

def doPrRule(conn, prRule, prRuleId, isUpdate, algoId):
    print(">> Starting doPrRule")
    
    if prRule.get('prRuleType') not in ['Granule', 'Temporal', 'Orbital', 'GranuleExact']:
        raise ValueError("Invalid prRuleType")
    
    cur = conn.cursor()
    cur.execute("SELECT jobclass FROM jobclasscode WHERE jobclassdescription =  %s", 
        (prRule.get('jobClass'),))
    row = cur.fetchone()
    if row is None:
        raise ValueError("Invalid job class")
    else:
        jobclass = row[0]
        
    cur.execute("SELECT jobpriority FROM jobprioritycode WHERE jobprioritydescription =  %s", 
        (prRule.get('jobPriority'),))
    row = cur.fetchone()
    if row is None:
        raise ValueError("Invalid job priority")
    else:
        jobpriority = row[0]
    
    #check start boundary time
    if isStrEmptyNull(prRule.get('prStartBoundaryTime')):
        prRule['prStartBoundaryTime'] = None
    else:
        sbt = datetime.strptime(prRule.get('prStartBoundaryTime'),'%Y-%m-%d %H.%M.%S.%f')
        prRule['prStartBoundaryTime'] = sbt
        print(prRule.get('prStartBoundaryTime'))
    
    print("prProductCoverageInterval_DS", prRule.get('prProductCoverageInterval_DS'))
    if isStrEmptyNull(prRule.get('prProductCoverageInterval_DS')):
        prRule['prProductCoverageInterval_DS'] = "PT0S" #ISO8601 duration format for 0 sec
        
    print("prRunInterval_DS", prRule.get('prRunInterval_DS'))
    if isStrEmptyNull(prRule.get('prRunInterval_DS')):
        prRule['prRunInterval_DS'] = "PT0S" #ISO8601 duration format for 0 sec

    print("gzFeatureName", prRule.get('gzFeatureName'))
    if isStrEmptyNull(prRule.get('gzFeatureName')):
        prRule['gzFeatureName'] = None
        gzId = None
    else:
        cur.execute("SELECT gzid FROM gazetteer WHERE gzfeaturename =  %s", (prRule.get('gzFeatureName'),) )
        row = cur.fetchone()
        if row is None:
            raise ValueError("Invalid Gazetteer: " + prRule.get('gzFeatureName'))
        else:
            gzId = row[0]
    
    print("plOrbitName", prRule.get('plOrbitName'))
    if isStrEmptyNull(prRule.get('plOrbitName')):
        if prRule.get('prRuleType') == 'Orbital':
            raise ValueError("plOrbitName required for Orbital rule type")
        prRule['plOrbitName'] = None
        plOrbitId = None
    else:
        cur.execute("SELECT plorbittypeid FROM platformorbittype WHERE plorbitname =  %s", (prRule.get('plOrbitName'),) )
        row = cur.fetchone()
        if row is None:
            raise ValueError("Invalid Orbit name " + prRule.get('plOrbitName'))
        else:
            plOrbitId = row[0]
            
    print("TailoringSection", prRule.get('TailoringSection'))        
    if prRule.get('TailoringSection'):
        dssElem = ET.fromstring(prRule.get('TailoringSection'))
        for measure in dssElem.findall('.//select//measure'):
            print(measure.get('name'))
            print(measure.get('from'))
            cur.execute("SELECT count(*) FROM enterprisemeasure em, enterprisedimensionlist edl " +
                "WHERE em.e_dimensionlistid = edl.e_dimensionlistid AND em.measurename = %s "
                "AND edl.e_dimensionlistname = %s", (measure.get('name'), measure.get('from')))
            if cur.fetchone()[0] == 0:
                raise ValueError("Measure<=>Dimension Group combination not found: " +
                    measure.get('name') + "<=>" + measure.get('from'))
    else:
        prRule['TailoringSection'] = None
    
    data = {"jp": jobpriority, "ai": algoId, "gz": gzId, "jc": jobclass,
            "rn": prRule.get('prRuleName'), "rt": prRule.get('prRuleType'), 
            "ts": prRule.get('prTemporarySpaceMB'), "er": prRule.get('prEstimatedRAM_MB'), 
            "ec": prRule.get('prEstimatedCPU'), "ci": prRule.get('prProductCoverageInterval_DS'),
            "sb": prRule.get('prStartBoundaryTime'), "ri": prRule.get('prRunInterval_DS'),
            "wi": prRule.get('prWaitForInputInterval'), "po": plOrbitId, "ds": prRule['TailoringSection']}
    if not isUpdate:
        insStmt = ("INSERT INTO productionrule (prid, movcode, wedid, jobpriority, algorithmid, "
            "gzid, jobclass, prrulename, prruletype, practiveflag_nsof, practiveflag_cbu, "
            "prtemporaryspacemb, prnotifyopsseconds, prestimatedram_mb, prestimatedcpu, "
            "prproductcoverageinterval_ds, prproductcoverageinterval_ym, prstartboundarytime, "
            "prruninterval_ds, prruninterval_ym, prweathereventdistancekm, prorbitstartboundary, "
            "prproductorbitinterval, prwaitforinputinterval_ds, plorbittypeid, prdataselectionxml) "
            "VALUES (nextval('s_productionrule'), null, null, %(jp)s, %(ai)s, %(gz)s, %(jc)s, "
            "%(rn)s, %(rt)s, 0, 0, %(ts)s, '-1', %(er)s, %(ec)s, %(ci)s, null, %(sb)s, %(ri)s, "
            "null, null, null, null, %(wi)s, %(po)s, %(ds)s) RETURNING prid")
        cur.execute(insStmt, data)
        prRuleId = cur.fetchone()[0]
    else:
        updStmt = ("UPDATE productionrule SET jobpriority = %(jp)s, gzid = %(gz)s, jobclass = %(jc)s, "
            "prruletype = %(rt)s, practiveflag_nsof = 0, practiveflag_cbu = 0, prtemporaryspacemb = %(ts)s, "
            "prestimatedram_mb = %(er)s, prestimatedcpu = %(ec)s, prproductcoverageinterval_ds = %(ci)s, "
            "prstartboundarytime = %(sb)s, prruninterval_ds = %(ri)s, prwaitforinputinterval_ds = %(wi)s, "
            "plorbittypeid = %(po)s, prdataselectionxml = %(ds)s "
            "WHERE prrulename = %(rn)s AND algorithmid = %(ai)s")
        cur.execute(updStmt, data)
        
    cur.close()
    
    print(">> Finished doPrRule")
    return prRuleId
    

def doPrParameters(conn, prParams, prRuleId, isUpdate, algoId):
    print(">> Starting doPrParameters")
    
    cur = conn.cursor()
    for prParam in prParams:
        
        cur.execute("SELECT algoparameterid FROM algoparameters WHERE algoparametername = %s and algorithmid = %s",
            (prParam.get('algoParameterName'), algoId) )
        row = cur.fetchone()
        if row is None:
            raise ValueError(prParam.get('algoParameterName') + " PR Parameter not registered as an algorithm parameter")
        else:
            algoParamId = row[0]
        
        prParamVal = prParam.get('prParameterValue') \
                        .replace('@MODE@', os.environ['NDEMODE']).replace('@SANPATH@', os.environ['SANPATH'])
        
        doInsert = True
        if isUpdate:
            cur.execute("SELECT prparameterseqno FROM prparameter WHERE algoparameterid = %s AND prid = %s", 
                (algoParamId, prRuleId))
            row = cur.fetchone()
            if row is not None:
                cur.execute("UPDATE prparameter SET prparametervalue = %s WHERE algoparameterid = %s AND prid = %s",
                    (prParamVal, algoParamId, prRuleId) )
                print("Updated prParam:", prParamVal)
                doInsert = False
            # else this param doesn't exist yet, do insert
            
        if doInsert:
            cur.execute("INSERT INTO prparameter (prparameterseqno, algoparameterid, prid, prparametervalue) " +
                "VALUES (nextval('s_algoparameters'), %s, %s, %s)", (algoParamId, prRuleId, prParamVal) )
            print("Added prParam:", prParamVal)
            
    cur.close()
    print(">> Finished doPrParameters")
    

def doPrInputs(conn, prInputs, prRuleId, isUpdate, algoId):
    print(">> Starting doPrInputs")
    
    cur = conn.cursor()
    platforms = {}
    for prInput in prInputs:
        
        if prInput.get('prisFileHandleNumbering') is None:
            raise ValueError("Missing prisFileHandleNumbering in ProductionRuleInputs")
        elif prInput.get('prisFileHandleNumbering') not in ['Y', 'N']:
            raise ValueError("prisFileHandleNumbering must be 'Y' for 'N'")
        
        if prInput.get('PRInputProducts') is None:
            raise ValueError("Missing PRInputProducts in ProductionRuleInputs")
        
        if not isUpdate:
            cur.execute("INSERT INTO prinputspec (prisid, prid, prisfilehandle, prisneed, prisfilehandlenumbering, " +
                "pristest, prisleftoffsetinterval, prisrightoffsetinterval, prisfileaccumulationthreshold) " +
                "VALUES (nextval('s_prinputspec'), %s, %s, %s, %s, %s, %s, %s, %s) RETURNING prisid", 
                (prRuleId, prInput.get('prisFileHandle'), prInput.get('prisNeed'), prInput.get('prisFileHandleNumbering'),
                prInput.get('prisTest'), prInput.get('prisLeftOffsetInterval'), prInput.get('prisRightOffsetInterval'),
                prInput.get('prisFileAccumulationThreshold')) )
            prisId = cur.fetchone()[0]
            print("Added PR Input Spec, prisId:", prisId)
        
        for inputProd in prInput.get('PRInputProducts'):
            if inputProd.get('platformName') is None:
                raise ValueError("Missing platformName in PRInputProducts")
            
            if platforms.get(inputProd.get('platformName')) is None:
                cur.execute("SELECT platformid FROM platform WHERE platformname = %s",
                    (inputProd.get('platformName'),) )
                row = cur.fetchone()
                if row is None:
                    raise ValueError("No Platform registered with name: " + inputProd.get('Platformname'))
                else:
                    platforms[inputProd.get('platformName')] = row[0]
        
            cur.execute("SELECT pd.productid FROM productdescription pd, algoinputprod aip, " + 
                "productplatform_xref prx WHERE pd.productid = aip.productid AND pd.productid = prx.productid " +
                "AND aip.algorithmid = %s AND productshortname = %s AND prx.platformid = %s", 
                (algoId, inputProd.get('productShortName'), platforms[inputProd.get('platformName')]))
            row = cur.fetchone()
            
            if row is None:
                raise ValueError("Product has not been registered the algorithm or product is invalid:" + 
                    inputProd.get('productShortName') )
            else:
                productId = row[0]
                
            if isUpdate:
                cur.execute("SELECT pis.prisid, pip.pripid from prinputspec pis, prinputproduct pip " +
                    "WHERE pis.prid = %s AND pis.prisid = pip.prisid AND pip.productid = %s", (prRuleId, productId) )
                row = cur.fetchone()
                if row is None:
                    raise ValueError("Update failed; PrInputProd not previously registered for rule: " + 
                        inputProd.get('productShortName'))
                else:
                    prisId = row[0]
                    pripId = row[1]
                
                if inputProd.get('prInputPreference') == "1":
                    cur.execute("UPDATE prinputspec SET prisfilehandle = %s, prisneed = %s, " + 
                        "prisfilehandlenumbering = %s, pristest = %s, prisleftoffsetinterval = %s, " + 
                        "prisrightoffsetinterval = %s, prisfileaccumulationthreshold = %s WHERE prisid = %s",
                        (prInput.get('prisFileHandle'), prInput.get('prisNeed'), prInput.get('prisFileHandleNumbering'),
                        prInput.get('prisTest'), prInput.get('prisLeftOffsetInterval'), 
                        prInput.get('prisRightOffsetInterval'), prInput.get('prisFileAccumulationThreshold'), prisId) )
                    print("Added PR Input Spec, prisId:", prisId)
                    
                cur.execute("UPDATE prinputproduct SET prinputpreference = %s WHERE pripid = %s",
                    (inputProd.get('prInputPreference'), pripId) )
                print("Updated prInputProduct:", inputProd.get('productShortName') + " pripId:" + str(pripId))

            else:
                cur.execute("INSERT INTO prinputproduct (productid, algorithmid, prisid, pripid, " +
                    "prinputpreference) VALUES (%s, %s, %s, nextval('s_prinputproduct'), %s) RETURNING pripid", 
                    (productId, algoId, prisId, inputProd.get('prInputPreference')) )
                pripId = cur.fetchone()[0]
                print("Added prInputProduct:", inputProd.get('productShortName') + " pripId:" + str(pripId))
    
    cur.close()
    print(">> Finished doPrInputs")
    
    
def doPrOutputs(conn, prOutputs, prRuleId, algoId):
    print(">> Starting doPrOutputs")
    
    cur = conn.cursor()
    
    platforms = {}
    for outputProd in prOutputs:
        if outputProd.get('platformName') is None:
            raise ValueError("Missing platformName in ProductionRuleOutputs")
            
        if platforms.get(outputProd.get('platformName')) is None:
            cur.execute("SELECT platformid FROM platform WHERE platformname = %s",
                (outputProd.get('platformName'),) )
            row = cur.fetchone()
            if row is None:
                raise ValueError("No Platform registered with name: " + outputProd.get('Platformname'))
            else:
                platforms[outputProd.get('platformName')] = row[0]

        cur.execute("SELECT pd.productid FROM productdescription pd, algooutputprod aop, " + 
            "productplatform_xref prx  WHERE pd.productid = aop.productid AND pd.productid = prx.productid " +
            "AND aop.algorithmid = %s AND productshortname = %s AND prx.platformid = %s", 
            (algoId, outputProd.get('productShortName'), platforms[outputProd.get('platformName')]))
        row = cur.fetchone()
            
        if row is None:
            raise ValueError("Product has not been registered the algorithm or product is invalid:" + 
                outputProd.get('productShortName') )
        else:
            productId = row[0]
            
        cur.execute("INSERT INTO proutputspec (prosid, prid, productid, algorithmid) VALUES (" +
            "nextval('s_proutputspec'), %s, %s, %s) RETURNING prosid", (prRuleId, productId, algoId) )
        prosId = cur.fetchone()[0]
        print("Added prOutputProduct:", outputProd.get('productShortName') + " prosid:" + str(prosId))
    
    cur.close()
    print(">> Finished doPrOutputs")
    
    
def createElasticDoc(prRuleId, prRuleJson):
    print(">> Starting createElasticDoc")
    
    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port=os.environ['ES_PORT'],
        use_ssl=True,
        verify_certs=True,
        ca_certs=certifi.where())
    
    try:
        res = es.index(index='productionrule', doc_type='_doc', id=prRuleId, body=prRuleJson)
        if res['result'] == 'created' and res['_id'] == str(prRuleId):
            print("Successfully added to Index:", res)
            return True
        else:
            print("Failed to add to Index:", res)
            return False
    except Exception as e:
        print("ERROR: Failed to add to Elastic Search:", str(e))
        return False
    finally: 
        print(">> Finished createElasticDoc")