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


print('Loading registerProduct')

def lambda_handler(event, context):
    print(event)
    
    # get the msg body for proxy integration case
    if 'resource' in event:
        if event.get('body') is None:
            return createErrorResponse(400, "Validation error", 
                "body (product definition json) required")
        if isinstance(event.get('body'), dict):
            inputBody = event.get('body')
        else:
            inputBody = json.loads(event.get('body'))
    else:
        inputBody = event
    
    productDesc = inputBody.get('ProductDefinition')
    print("productDesc", productDesc)
    
    # did the client request a create or update
    isUpdate = False
    if 'resource' in event and event['resource'] == '/product/{productid}':
        isUpdate = True
        
    print('isUpdate?', isUpdate)
    
    try:
        conn = psycopg2.connect(
                host = os.environ['RD_HOST'],
                dbname = os.environ['RD_DBNM'],
                user = os.environ['RD_USER'],
                password = os.environ['RD_PSWD']
                )
        
        # check for existing
        cur = conn.cursor()
        
        if 'Platforms' in productDesc:
            cur.execute("SELECT platformid FROM platform WHERE platformname = %s", 
                            (productDesc.get('Platforms')[0].get('Platformname'),) )
            res = cur.fetchone()
            if res is not None: 
                platformId = res[0]
                print("platformId", platformId)
                cur.execute("SELECT pd.productid FROM productdescription pd, productplatform_xref prx "
                    "WHERE pd.productshortname = %s AND pd.productid = prx.productid AND prx.platformid = %s", 
                    (productDesc['PRODUCTSHORTNAME'], platformId,) )
            else:
                print("WARN: No Platform registered with name:", productDesc['Platforms'][0]['Platformname'])
                cur.execute("SELECT productid FROM productdescription WHERE productshortname = %s", 
                (productDesc['PRODUCTSHORTNAME'],) )
        else:
            cur.execute("SELECT productid FROM productdescription WHERE productshortname = %s", 
                (productDesc['PRODUCTSHORTNAME'],) )
                
        res = cur.fetchone()
        productId = res[0] if res is not None else None
        cur.close()
        
        print("productId", productId)
        
        # if isUpdate and {id} is None, proceed to register normally
        if not isUpdate and productId is not None:
            s = "Product already exists with short name: " + productDesc['PRODUCTSHORTNAME']
            print(s)
            return createErrorResponse(409, "Conflict: resource exists", s)
        elif isUpdate:
            if productId is None:
                s = "Unable to update; product with this short name NOT registered: " + productDesc['PRODUCTSHORTNAME']
                print(s)
                return createErrorResponse(404, "Not Found", s)
            elif productId != int(event['pathParameters']['productid']):
                s = "Unable to update; requested URI {productid} does not match existing Product ID: " + str(productId)
                print(s)
                return createErrorResponse(400, "Validation error", s)
        
        # start processing the input, rollback if any sections fail
        try:
            productId = doProductDesc(conn, productDesc, productId, isUpdate)
        except ValueError as e:
            print("Exception in doProductDesc:", e)
            return createErrorResponse(400, "Validation error", str(e))
            
        print("productId", productId)
        
        if 'ProducQualitySummarys' in productDesc:
            print(productDesc['ProductQualitySummarys'])
            try:
                doProductQualitySummarys(conn, productDesc['ProductQualitySummarys'], productId, isUpdate)
            except ValueError as e:
                print("Exception in doProductQualitySummarys:", e)
                return createErrorResponse(400, "Validation error", str(e))
        
        if 'IngestProcessSteps' in productDesc:
            print(productDesc['IngestProcessSteps'])
            try:
                doIngestProcessSteps(conn, productDesc['IngestProcessSteps'], productId, isUpdate)
            except ValueError as e:
                print("Exception in doIngestProcessSteps:", e)
                return createErrorResponse(400, "Validation error", str(e))
                
        if 'HOSTNAME' in productDesc:
            try:
                doProductDataSource(conn, productDesc, productId, isUpdate)
            except ValueError as e:
                print("Exception in doProductDataSource:", e)
                return createErrorResponse(400, "Validation error", str(e))
        
        if 'Platforms' in productDesc:
            try:
                doPlatforms(conn, productDesc['Platforms'], productId, isUpdate)
            except ValueError as e:
                print("Exception in doPlatforms:", e)
                return createErrorResponse(400, "Validation error", str(e))
        
        # Save the entire JSON doc (including ProductDescription root) to ES
        esSuccess = createElasticDoc(productId, json.dumps({"ProductDefinition": productDesc}))
        
        if esSuccess:
            conn.commit()
        else:
            conn.rollback()
            return createErrorResponse(500, "Error encountered updating ElasticSearch", str(e))
        
        response = {
            "isBase64Encoded": False,
            "statusCode": 200 if isUpdate else 201,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": '*'
            },
            "body": json.dumps({
                "message": "Product was " + ("updated" if isUpdate else "registered") + " successfully",
                "productId": productId
            })
        }
        return response
    except Exception as e:
        print("ERROR: " + str(e))
        return createErrorResponse(500, "Internal error", "An exception was encountered, please try again later")
    finally:
        conn.rollback()
        conn.close()


def doProductDesc(conn, productDesc, productId, isUpdate):
    print(">> Starting doProductDesc")
    
    if productDesc.get('PRODUCTSTATUS') == "": productDesc['PRODUCTSTATUS'] = 'NOP'
    if productDesc.get('PRODUCTSTATUS') not in ['OP','TEST','NOP']:
        raise ValueError('PRODUCTSTATUS value is invalid. Valid values: [OP, TEST, NOP]')
    
    if productDesc.get('PRODUCTARCHIVE') == "": productDesc['PRODUCTARCHIVE'] = '0'
    if productDesc.get('PRODUCTARCHIVE') not in ['0','1']:
        raise ValueError('PRODUCTARCHIVE value is invalid. Valid values: [0, 1]')
    
    if "PRODUCTDISTFLAG" not in productDesc or productDesc.get('PRODUCTDISTFLAG') not in ['TRUE','FALSE']:
        productDesc['PRODUCTDISTFLAG'] = 'TRUE'
    
    print(productDesc.get('PRODUCTCOVERAGEGAPINTERVAL_DS'))
    if isStrEmptyNull(productDesc.get('PRODUCTCOVERAGEGAPINTERVAL_DS')):
        productDesc['PRODUCTCOVERAGEGAPINTERVAL_DS'] = "PT0S" #ISO8601 duration format for 0 sec
    
    if productDesc.get('PRODUCTCIPPRIORITY') == "":
        productDesc['PRODUCTCIPPRIORITY'] = None
    
    if productDesc.get('PRODUCTAVAILABILITYDATE') == "":
        productDesc['PRODUCTAVAILABILITYDATE'] = None
    
    if productDesc.get('PRODUCTSPATIALAREA') == "":
        productDesc['PRODUCTSPATIALAREA'] = None
            
    productDesc['PRODUCTESTAVGFILESIZE'] = math.ceil(float(productDesc.get('PRODUCTESTAVGFILESIZE')))
    
    cur = conn.cursor()
    try:
        cur.execute("SELECT ingestincomingdirectoryid FROM ingestincomingdirectory "
            "WHERE ingestdirectoryname = %s", (productDesc['INGESTDIRECTORYNAME'],) )
        ingestDirId = cur.fetchone()[0]
    except Exception as e:
        print('ERROR: Invalid INGESTDIRECTORYNAME:', productDesc['INGESTDIRECTORYNAME'])
        raise ValueError('Invalid INGESTDIRECTORYNAME')
    
    productGroupId = None
    try:
        if productDesc['PRODUCTGROUPNAME'] != "" and productDesc['PRODUCTGROUPNAME'] != "null":
            cur.execute("SELECT productgroupid FROM productgroup WHERE productgroupname = %s", 
                (productDesc['PRODUCTGROUPNAME'],) )
            productGroupId = cur.fetchone()[0]
    except Exception as e:
        print('WARN: No product group with name:', productDesc['PRODUCTGROUPNAME'])
        
    if not isUpdate:
        cur.execute("INSERT INTO productdescription (productid) "
            "VALUES (nextval('s_productdescription\')) RETURNING productid")
        productId = cur.fetchone()[0]
        print("new productId", productId)
        if productDesc['PRODUCTFILENAMEEXTENSION'] == "h5":
            cur.execute("INSERT INTO hdf5_structure values (%s)", (productId,) )
        elif productDesc['PRODUCTFILENAMEEXTENSION'] == "nc":
            cur.execute("INSERT INTO netcdf4_structure values (%s)", (productId,) )
            
    excludeFields = ['ProductQualitySummarys', 'IngestProcessSteps', 'Platforms',
        'INGESTDIRECTORYNAME', 'PRODUCTGROUPNAME','HOSTNAME','PRODUCTPROVIDERDIRECTORY',
        'PRODUCTPOLLINGFREQUENCY','PRODUCTPROVIDERPOLLINGRETRIES', 'PRODUCTSPATIALAREA']
    fieldsSnippet = ', '.join([f + "=%(" + f + ")s" for f in list(productDesc.keys()) if f not in excludeFields])
    print(fieldsSnippet)
    
    cur.execute("UPDATE productdescription SET " + fieldsSnippet + " WHERE productid = %(productid)s",
        {'productid': productId, 'INGESTINCOMINGDIRECTORYID': ingestDirId, 'PRODUCTGROUPID': productGroupId, **productDesc})
        
    cur.close()
    
    print(">> Finished doProductDesc")
    return productId
    

def doProductQualitySummarys(conn, pqsObj, productId, isUpdate):
    print(">> Starting doProductQualitySummarys")
    
    cur = conn.cursor()
    for pqs in pqsObj:
        doInsert = True
        if isUpdate:
            cur.execute("SELECT productqualitysummaryname FROM productqualitysummary WHERE productid = %s "
            "AND productqualitysummaryname = %s", (productId, pqs['PRODUCTQUALITYSUMMARYNAME'],))
            row = cur.fetchone()
            if row is not None:
                doInsert = False
        if doInsert:
            cur.execute("INSERT INTO productqualitysummary VALUES (%s, %s, %s, %s)", (productId, 
                pqs['PRODUCTQUALITYSUMMARYNAME'], pqs['PRODUCTQUALITYSUMMARYTYPE'], pqs['PRODUCTQUALITYDESCRIPTION'],))
            print("Added PQS: ", pqs['PRODUCTQUALITYSUMMARYNAME'])
    
    cur.close()
    print(">> Finished doProductQualitySummarys")


def doIngestProcessSteps(conn, ipsObj, productId, isUpdate):
    print(">> Starting doIngestProcessSteps")
    
    cur = conn.cursor()
    if isUpdate:
        cur.execute("DELETE FROM ingestprocessstep WHERE productID = %s", (productId,))
    
    for ips in ipsObj:
        if (ips['IPSFAILSINGEST'] == "" or ips['IPSENABLE'] == "" or ips['IPSENABLE'] == ""):
            raise ValueError('Missing IPSFAILSINGEST or IPSENABLE or IPSENABLE for IPS', ips['NSFDESCRIPTION'])
        
        cur.execute("SELECT nsfid FROM nde_supportfunction WHERE nsfdescription = %s", (ips['NSFDESCRIPTION'],))
        row = cur.fetchone()
        if row is None: 
            raise ValueError('Unregistered NSFDescription:', ips['NSFDESCRIPTION'])
        
        cur.execute("INSERT INTO ingestprocessstep (productid, nsfid, ipsoptionalparameters, ipsfailsingest, "
            "ipsenable, ipsdoretransmit) VALUES (%s, %s, %s, %s, %s, %s)", (productId, row[0],
            ips['IPSOPTIONALPARAMETERS'], ips['IPSFAILSINGEST'], ips['IPSENABLE'], ips['IPSDORETRANSMIT']))
            
        print("Added IPS: ", ips['NSFDESCRIPTION'])
    
    cur.close()
    print(">> Finished doIngestProcessSteps")
    


def doProductDataSource(conn, productDesc, productId, isUpdate):
    print(">> Starting doProductDataSource")
    
    cur = conn.cursor()
    try:
        cur.execute("SELECT hostid FROM externaldatahost WHERE hostname = %s", (productDesc['HOSTNAME'],))
        hostId = cur.fetchone()[0]
    except Exception as e:
        print('ERROR: External Host not registered:', productDesc['HOSTNAME'])
        raise ValueError('External Host not registered')
    
    if isUpdate:
        cur.execute("DELETE FROM externaldatahost WHERE productid = %s AND hostid = %s", (productId, hostId))
    
    cur.execute("INSERT INTO productdatasource (productid, hostid, productproviderdirectory, productpollingfrequency, "
        "productproviderpollingretries ) values ( %s, %s, %s, %s, %s)", (productId, hostId, 
        productDesc['PRODUCTPROVIDERDIRECTORY'], productDesc['PRODUCTPOLLINGFREQUENCY'], 
        productDesc['PRODUCTPROVIDERPOLLINGRETRIES']))
    
    cur.close()
    
    print(">> Finished doProductDataSource")
    

def doPlatforms(conn, platformObj, productId, isUpdate):
    print(">> Starting doPlatforms")
    
    cur = conn.cursor()
    if isUpdate:
        cur.execute("DELETE FROM productplatform_xref WHERE productid = %s", (productId,))
    
    for platform in platformObj:
        cur.execute("SELECT platformid FROM platform WHERE platformname = %s", (platform['Platformname'],) )
        row = cur.fetchone()
        if row is not None: 
            platformId = row[0]
            cur.execute("INSERT INTO productplatform_xref VALUES ( %s, %s )", (platformId, productId));
            print("Added Platform XREF: ", platformId, "<=>", productId)
        else:
            print("ERROR: No Platform registered with name: " + platform['Platformname'])
            raise ValueError("No Platform registered with name: " + platform['Platformname'])
    
    cur.close()
    
    print(">> Finished doPlatforms")


def createElasticDoc(productId, productJson):
    print(">> Starting createElasticDoc")
    
    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port = os.environ['ES_PORT'],
        use_ssl = True,
        verify_certs = True,
        ca_certs = certifi.where()
        )
    
    try:
        res = es.index(
            index = "product", 
            doc_type = '_doc', 
            id = productId, 
            body = productJson
        )
        
        if res['result'] == 'created' and res['_id'] == str(productId):
            print("Successfully added to Index:", res)
            return True
        elif res['result'] == 'updated' and res['_id'] == str(productId):
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
    