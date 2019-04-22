"""
    Author: Hieu Phung, Peter MacHarrie, Jonathan Hansford; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-08
    Python Version: 3.6
"""

import sys
import os
import json
import re
import boto3
import botocore
from datetime import datetime
import psycopg2
import elasticsearch
import certifi

from lambda_functions.extractors import edm_idpsH5  as idpsExtr
from lambda_functions.extractors import edm_nupNc4  as nupExtr
from lambda_functions.extractors import edm_goesCgs as cgsExtr
from lambda_functions.extractors import edm_eme     as emeExtr

from ndeutil import *


print('Loading ingestFile')

irTimings = {}

ingestReqObj = {}

conn = psycopg2.connect(
    host = os.environ['RD_HOST'],
    dbname = os.environ['RD_DBNM'],
    user = os.environ['RD_USER'],
    password = os.environ['RD_PSWD']
    )

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
sns = boto3.client('sns')


def lambda_handler(event, context):
    
    # Inputs:
    #   Request Context
    #   Request Event (fileName and location of file)
    # Outputs:
    #   No Errors:
    #      file in its home
    #      postgreSQL inserts
    #      ElasticSearch Document created
    #      New File Message
    #   If Errors:
    #       Error Response
    #   In Either Case:
    #   Response to client
    
    print(datetime.now(), event)
    global irTimings
    irTimings = {
        "startTs" : datetime.now(),
        "prevTs" : datetime.now(), 
        "log": [ {"msg": "start", "dur": "0", "ts": str(datetime.now())} ] 
    }

    response = {}
    
    body = None
    localFilePath = None

    # Check input
    if 'resource' in event:
        if event.get('body') is None:
            return createAndLogError(400, "Validation error", "body json required", False)
        if isinstance(event.get('body'), dict):
            inputBody = event.get('body')
        else:
            inputBody = json.loads(event.get('body'))
    else:
        inputBody = event
    
    if inputBody.get('filename') is None:
        return createAndLogError(400, "Validation error", "Missing 'filename' in body", False)
    if inputBody.get('filestoragereference') is None:
        return createAndLogError(400, "Validation error", "Missing 'filestoragereference' in body", False)
    if inputBody.get('filestoragereference').get('bucket') is None:
        return createAndLogError(400, "Validation error", "Missing 'bucket' in filestoragereference", False)
    if inputBody.get('filestoragereference').get('key') is None:
        return createAndLogError(400, "Validation error", "Missing 'key' in filestoragereference", False)

    try:
        filename = inputBody.get('filename')
        s3Bucket = inputBody.get('filestoragereference').get('bucket')
        s3Key = inputBody.get('filestoragereference').get('key')
        
        global ingestReqObj
        ingestReqObj = {'fileName': filename}
        
        cur = conn.cursor()
        # Check the file's product definition.
        cur.execute("SELECT JSON_AGG(t2) FROM (SELECT productId, productShortName, productHomeDirectory, " +
            "(SELECT JSON_AGG(t1) FROM " +
                "(SELECT ipsoptionalparameters, ipsenable, nsfdescription, nsftype, nsfpathorclassname, " +
                "nsfmethodorexecutablename FROM ingestprocessstep s, nde_supportfunction f " +
                "WHERE s.productId = p.productId AND s.nsfId = f.nsfId) t1) AS ips " +
            "FROM productdescription p WHERE %s LIKE productfilenamepattern) t2", (filename, ))
        row = cur.fetchone()
        cur.close()
        
        if row[0] is None:
            return createAndLogError(400, "Validation error", "No product definition found", True)
        print(datetime.now(), "File is a recognized product.")
        
        ingestReqObj.update(row[0][0])
        print("ingestReqObj", ingestReqObj)
    
        # Check if the passed file is present in storage.
        try:
            s3ObjMeta = s3.head_object(
                Bucket = s3Bucket, 
                Key = s3Key
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                return createAndLogError(404, "Validation error", "Specified file not found in S3", True)
            else:
                print("ERROR:", e)
                return createAndLogError(500, "Internal error", "Exception validating S3 location: " + str(e), True)
    
        print(datetime.now(), "fileSize = " + str(s3ObjMeta['ContentLength']) + " bytes")
        ingestReqObj['fileSize'] = s3ObjMeta['ContentLength']
        ingestReqObj['filestoragereference'] = inputBody.get('filestoragereference')
        
        updateIrTimings("check inputs")
        
        if s3ObjMeta['ContentLength'] > 268435456: #256MB
            print('Sending Message to BigIngestQ')
            try:
                snsResponse = sns.publish(
                    TopicArn = 'arn:aws:sns:us-east-1:784330347242:BigIngest',
                    Message = json.dumps(inputBody),
                    Subject = 'Validated Big File',
                )
                updateIrTimings("sns to big ingest")
                return {
                    "isBase64Encoded": False,
                    "statusCode": 200,
                    "headers": {
                        "Content-Type": "application/json",
                    },
                    "body": "Routed request to BigIngestSNS: " + str(snsResponse)
                }
            except botocore.exceptions.ClientError as e:
                print ("SNS error:", e.response)
                return createAndLogError(500, "Internal error", "Exception publishing SNS message to BigIngest", True)
                
        localFilePath = "/tmp/" + filename
    
        s3.download_file(s3Bucket, s3Key, localFilePath)
        updateIrTimings("file copy to /tmp")
        
        # print(os.listdir("/tmp/"))
        # os.remove(localFilePath)
        # print(os.listdir("/tmp/"))

        try:
            extractMetadata(localFilePath)
        except Exception as e:
            print("Error encountered extracting metadata: " + str(e))
            return createAndLogError(500, "Internal error", "Error extracting metadata: " + str(e), True)
        
        print("ingestReqObj", ingestReqObj)

        try:
            persistIngestReqObj(localFilePath)
        except Exception as e:
            if 'Duplicate file' in str(e):
                print("Duplicate file during persistIngestReqObj: " + str(e))
                return createAndLogError(407, "Duplicate error", str(e), True)
            else:
                print("Error persisting IngestReqObj: " + str(e))
                return createAndLogError(500, "Internal error", "Error persisting IngestReqObj: " + str(e), True)
                
        broadcastIngest(sns)
        
        persistIrTimings("FINI", "")
        
        conn.commit()
        
        response = {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "message": "success",
                "fileId": ingestReqObj['fileId']
            })
        }
        return response
        
    except Exception as bigE:
        # conn.rollback()
        eType, eValue, eTraceback = sys.exc_info()
        print("Exception in lambda_handler:", eType, eValue, "line " + str(eTraceback.tb_lineno))
        createAndLogError(500, "Internal error", "Error encountered: " + str(bigE), True)
    finally:
        if localFilePath is not None and os.path.exists(localFilePath):
            os.remove(localFilePath)
        conn.rollback()
        # conn.close()
    

    
def extractMetadata(localFilePath):
    print(datetime.now(), "Extracting metadata")
    
    ingestReqObj['metadata'] = {}
    for step in ingestReqObj['ips']:

        print(datetime.now(), "ips: " + str(step))

        if step.get('nsfdescription') == 'SHA384 checksum class':
            # skip for now
            print("Skipping " + step.get('nsfpathorclassname') + " IPS")
            
        elif step.get('nsfdescription') == "Metadata extractor bean (h5 files)":
            if step.get('nsftype') == 'H5DUMP':
                stepMetadataDict = idpsExtr.main(localFilePath)
                ingestReqObj['metadata'].update(stepMetadataDict)
            else:
                raise ValueError("'Metadata extractor bean (h5 files)' but nsfType not H5DUMP: " + step.get('nsfType'))
            
        elif step.get('nsfdescription') == 'NDE NUP Extractor Bean':
            if step.get('nsftype') == 'NCDUMP':
                stepMetadataDict = nupExtr.main(localFilePath)
                ingestReqObj['metadata'].update(stepMetadataDict)
            else:
                raise ValueError("'NDE NUP Extractor Bean' but nsfType not NCDUMP: " + step.get('nsfType'))
            
        elif step.get('nsfdescription') == "GOES CG Metadata Extractor":
            stepMetadataDict = cgsExtr.main(localFilePath)
            ingestReqObj['metadata'].update(stepMetadataDict)

            # Cleanup following if
            # if ingestReqObj['productHomeDirectory']=='External':
            #     persistJSON['sqlData']['SQL']['fileMetadata']['filemetadataxml'] = body['filestoragereference']
                
        elif step.get('nsftype') == "EME": # "External" metadata extractor
            stepMetadataDict = emeExtr.main(step.get("nsfmethodorexecutablename"), 
                                        step.get('ipsoptionalparameters'), ingestReqObj.get('fileName'))
            ingestReqObj['metadata'].update(stepMetadataDict)
                
        else:
            raise ValueError("Unsupported Ingest Process Step extractor: " + step.get('nsfmethodorexecutablename'))
        
        if step.get('nsfdescription') != 'SHA384 checksum class':
            ingestReqObj['supMetadata'] = getFileProductSupplementMetadata(
                                            step.get('ipsoptionalparameters'), ingestReqObj.get('fileName'))
        
        updateIrTimings("ips " + step.get('nsfdescription'))
        
        

def getFileProductSupplementMetadata(optionalParameter, fileName):
    """ 
    Expects the IpsOptionalParameters and the file name.
    Returns a string, the value to place in the fileProductSupplementMetadata column. 
    If there is no FileProductSupplementMetadata, the string returned is "n/a".
    Null values are not considered equal for the purposes of checking a unique constraint 
        [ref: https://www.postgresql.org/docs/11/ddl-constraints.html#DDL-CONSTRAINTS-UNIQUE-CONSTRAINTS],
    and this column is part of a unique constraint, so it can never be null.
    """
     # The '-subString' parameter, if provided, specifies a substring of the filename to add to the 
     # fileProductSupplementMetadata column of the database
    supplementalMetadata = "n/a"
    if '-subString' in optionalParameter:
        matchObject = re.search(r'-subString\s+(\d+)\s+(\d+)', optionalParameter)
        if matchObject is not None:
            beginIndex = int(matchObject.group(1))
            endIndex = int(matchObject.group(2))
            
            supplementalMetadata = (fileName.split("/")[-1])[beginIndex:endIndex]
        else:
            failureReason = "Invalid -subString option in optionalParameter: %s" % optionalParameter
            print ("Status : FAIL", "Reason : ", failureReason)
            raise Exception(failureReason)
    
    return supplementalMetadata
    
    
def updateIrTimings(msg):
    
    if msg == "Total":
        newTiming = {"msg": msg, "dur": (irTimings['prevTs'] - irTimings['startTs']).total_seconds(), 
                        "et": str(irTimings['prevTs']) }
    else:
        newTs = datetime.now()
        newTiming = {"msg": msg, "dur": (newTs - irTimings['prevTs']).total_seconds(), "et": str(newTs) }
        irTimings['prevTs'] = newTs
    
    irTimings['log'].append(newTiming)
    # print(irTimings)
    

def persistIrTimings(status, failureReason):
    cur = conn.cursor()

    updateIrTimings("Total")
    
    if('violates unique constraint "fm_unique"' in failureReason):
        failureReason = failureReason.split('unique"')[0] + 'unique"'
    
    productId = None
    if 'productid' in ingestReqObj:
        productId = ingestReqObj['productid']
        
    fileId = None
    if 'fileId' in ingestReqObj:
        fileId = ingestReqObj['fileId']

    cur.execute("INSERT INTO ingestlog (il_id, productid, fileid, filename, ilstatus, ilfailurereason, ilmetrics_json) " +
            "VALUES (NEXTVAL('S_INGESTREQUESTLOG'), %s, %s, %s, %s, %s, %s)",
            (productId, fileId, ingestReqObj['fileName'], status, failureReason, json.dumps(irTimings['log'])) )
    conn.commit()
    cur.close()


def createAndLogError(code, error, msg, logSql):
    if logSql:
        updateIrTimings(error + ": " + msg)
        persistIrTimings("FAIL", error + ": " + msg)
        
    errorResponse = {
        "isBase64Encoded": False,
        "statusCode": code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": '*'
        },
        "body": json.dumps({
            "error": error,
            "message": msg
        })
    }
    print(errorResponse)
    return errorResponse
    
    
def productIsTrigger(productId):
    isTrigger = False
    cur = conn.cursor()
    cur.execute("SELECT COUNT(DISTINCT prId) FROM prInputSpec s, prInputProduct i WHERE " +
        "s.prisId = i.prIsId AND i.productId = %s AND prIsNeed = 'TRIGGER'", (productId,) )
    row = cur.fetchone()
    cur.close()

    if row[0] > 0:
        isTrigger = True
    return isTrigger
    

def broadcastIngest(sns):
    print(datetime.now(), "Starting SNS broadcast")
    
    triggerFlag = "false"
    if productIsTrigger(ingestReqObj['productid']):
        triggerFlag = "true"
    
    # print("ingestReqObj", ingestReqObj)
    snsMsg = {
        "fileId"            : { "DataType" : "String", "StringValue" : str(ingestReqObj['fileId']) },
        "productId"         : { "DataType" : "String", "StringValue" : str(ingestReqObj['productid']) },
        'productShortname'  : { 'DataType' : 'String', 'StringValue' : ingestReqObj['productshortname']},
        'fileName'          : { 'DataType' : 'String', 'StringValue' : ingestReqObj['fileName']},
        'fileStartTime'     : { 'DataType' : 'String', 'StringValue' : ingestReqObj['metadata']['edmCore']['fileStartTime']},
        'fileEndTime'       : { 'DataType' : 'String', 'StringValue' : ingestReqObj['metadata']['edmCore']['fileEndTime']},
        'productHomeDirectory' : { 'DataType' : 'String', 'StringValue' : ingestReqObj['producthomedirectory'] },
        "triggerFlag"       : { "DataType" : "String", "StringValue" : triggerFlag }
    }
    
    if "fileSpatialArea" in ingestReqObj['metadata']['edmCore']:
        if ingestReqObj['metadata']['edmCore']["fileSpatialArea"] is not None:
            snsMsg["fileSpatialArea"] = {
                                            "DataType": "String", 
                                            "StringValue": ingestReqObj['st_geoSpatialArea'] }
            
    if "fileDayNightFlag" in ingestReqObj['metadata']['edmCore']:
        if ingestReqObj['metadata']['edmCore']["fileDayNightFlag"] is not None:
            snsMsg["fileDayNightFlag"] = { "DataType": "String", 
                                            "StringValue": ingestReqObj['metadata']['edmCore']["fileDayNightFlag"] }
    
    print("snsMsg", snsMsg)
    try:
        snsResponse = sns.publish(
            TopicArn = 'arn:aws:sns:us-east-1:784330347242:ValidatedProduct',
            Message = snsMsg['fileName']['StringValue'],
            Subject = 'Validated File',
            MessageAttributes = snsMsg
        )
        updateIrTimings("Finished SNS to ValidatedProduct")
        
    except botocore.exceptions.ClientError as e:
        print ("SNS error:", e.response)
        updateIrTimings("Error sending SNS to ValidatedProduct: " + str(e.response))
    
    
def persistIngestReqObj(localFilePath):
    
    print(datetime.now(), "Starting persist")
    cur = conn.cursor()

    esDoc = ingestReqObj['metadata']
    edmCore = ingestReqObj['metadata']['edmCore']

    # Move source to target

    if ingestReqObj['producthomedirectory'] != 'External':
        cpDest = ingestReqObj['producthomedirectory'] + '/' + ingestReqObj['fileName']
        # cpSrc = {
        #     'Bucket': ingestReqObj.get('filestoragereference').get('bucket'),
        #     'Key': ingestReqObj.get('filestoragereference').get('key')
        # }
        try:
            s3.upload_file(localFilePath, "ndepg", cpDest)
#           client.copy_object(CopySource = copy_source, Bucket="ndepg", Key = destKey)
            print(datetime.now(), "Done S3 product home")
            updateIrTimings("File copy to S3 product")
        except botocore.exceptions.ClientError as e:
            print ("Exception s3 move", e)
            raise Exception('Exception during S3 product home: ' + e)

    
    # Update SQL db
    insStmt = """
        INSERT INTO filemetadata(
            fileid, productid, pgdatapartitionid, fileinserttime, filesize, filestarttime, 
            fileendtime, filebeginorbitnum, fileendorbitnum, fileidpsgranuleversion, filedaynightflag, 
            filespatialarea, filename, filemetadataxml, filevalidationresults, fileascdescindicator, 
            filedeletedflag, fileproductsupplementmetadata)
        VALUES (
            NEXTVAL('S_FILEMETADATA'), %(pi)s, %(pgdpi)s, now(), %(fs)s, to_timestamp(%(fst)s, 'YYYYMMDD HH24MISS.US'), 
            to_timestamp(%(fet)s, 'YYYYMMDD HH24MISS.US'), %(fbon)s, %(feon)s, %(figv)s, %(fdnf)s, 
            st_geogfromtext(%(fsa)s), %(fn)s, %(fmx)s, %(fvr)s, %(fadi)s, %(fdf)s, %(fpsm)s)
        RETURNING fileid, fileinserttime
    """
    
    if ingestReqObj['producthomedirectory'] == 'External':
        filemetadataxml = json.dumps(ingestReqObj['filestoragereference'])
    else: 
        filemetadataxml = None
    
    tempFst = edmCore.get('fileStartTime').replace('T', ' ').replace('Z', '').replace('-', '').replace(':', '')
    tempFet = edmCore.get('fileEndTime').replace('T', ' ').replace('Z', '').replace('-', '').replace(':', '')
    
    if len(tempFst) > 15 and tempFst[15] != '.':
        tempFst = tempFst[:15] + '.' + tempFst[15:]
    if len(tempFet) > 15 and tempFet[15] != '.':
        tempFet = tempFet[:15] + '.' + tempFet[15:]     
    
    # print(tempFst)
    # print(tempFet)

    data = {
        "pi": ingestReqObj.get('productid'), 
        "pgdpi": None, 
        "fs": ingestReqObj.get('fileSize'), 
        # "fst": edmCore.get('fileStartTime').replace('T', ' ').replace('Z', '').replace('-', '').replace(':', ''),
        # "fet": edmCore.get('fileEndTime').replace('T', ' ').replace('Z', '').replace('-', '').replace(':', ''), 
        "fst": tempFst,
        "fet": tempFet, 
        "fbon": edmCore.get('fileBeginOrbitNum'), 
        "feon": edmCore.get('fileEndOrbitNum'), 
        "figv": None,
        "fdnf": edmCore.get('fileDayNightFlag'), 
        "fsa": edmCore.get('fileSpatialArea'),
        "fn": ingestReqObj.get('fileName'),
        "fmx": filemetadataxml,
        "fvr": None,
        "fadi": edmCore.get('fileAscDescIndicator'),
        "fdf": 'N',
        "fpsm": ingestReqObj.get('supMetadata')
    }
    
    # print(cur.mogrify(insStmt, data))

    try:
        cur.execute(insStmt, data)
        fileId = cur.fetchone()[0]
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        if 'violates unique constraint "fm_unique"' in e.pgerror:
            print("Failed fm_unique on SQL insert");
            updateIrTimings("Failed fm_unique on SQL insert");
            deleteS3IncomingInputFile(ingestReqObj['filestoragereference'])
            raise Exception("Duplicate file: " + e.pgerror)
        else:
            raise Exception("Database error: " + e.pgerror)
            
    ingestReqObj['fileId'] = fileId
    updateIrTimings("SQL insert")
    print(datetime.now(), "Done SQL insert, fileId:", fileId)

    # SQL insert done first to get fileId and fileInsertTime

    # Update elasticsearch
    # Format time to es timestamp
    # sql = """select
    #     fileId,
    #     productShortName,
    #     replace(to_char(fileStartTime, 'YYYY-MM-DD HH24:MI:SS.MSZ'), ' ', 'T'),
    #     replace(to_char(fileEndTime, 'YYYY-MM-DD HH24:MI:SS.MSZ'), ' ', 'T'),
    #     replace(to_char(fileInsertTime, 'YYYY-MM-DD HH24:MI:SS.MSZ'), ' ', 'T'),
    #     st_asGeoJSON(fileSpatialArea),
    #     st_asGeoJSON(st_reverse(fileSpatialArea::geometry)),
    #     st_isValid(fileSpatialArea::geometry) validSpatial,
    #     st_isValid(st_reverse(fileSpatialArea::geometry)) validReverseSpatial,
    #     st_AsGeoJSON(st_reverse(st_Envelope(fileSpatialArea::geometry))) reverseBB
    #             from filemetadata f, productDescription p
    #     where fileId = %s and f.productId = p.productId"""
    sql = """select
        fileId,
        productShortName,
        replace(to_char(fileStartTime, 'YYYY-MM-DD HH24:MI:SS.MSZ'), ' ', 'T'),
        replace(to_char(fileEndTime, 'YYYY-MM-DD HH24:MI:SS.MSZ'), ' ', 'T'),
        replace(to_char(fileInsertTime, 'YYYY-MM-DD HH24:MI:SS.MSZ'), ' ', 'T'),
        st_asGeoJSON(fileSpatialArea)
                from filemetadata f, productDescription p
        where fileId = %s and f.productId = p.productId"""
    cur.execute(sql, (fileId,))
    row = cur.fetchone()
    
    # conn.commit()
    #print ("row=", row)
    
    esDoc['edmCore']['fileId'] = row[0]
    esDoc['edmCore']['fileName'] = ingestReqObj['fileName'];
    esDoc['edmCore']['productShortName'] = row[1]
    esDoc['edmCore']['fileStartTime'] = row[2]
    esDoc['edmCore']['fileEndTime'] = row[3]
    esDoc['edmCore']['fileInsertTime'] = row[4]
    esDoc['edmCore']['fileSize'] = ingestReqObj['fileSize']
    
    #print ("Source polygon=", row[5])
    #print ("Reversed polygon=", row[6])
    
    #print ("Type of 7=", type(row[7]), row[7])
    
    # 9/24/2018 ElasticSearch Performance issue with JPSS polygons, use reverse envelope
    
    if row[5] == None:
        esDoc['edmCore']['fileSpatialArea'] = None
    else:
        # Save off the ST_GEO string so it can be used for broadcast SNS later
        ingestReqObj['st_geoSpatialArea'] = esDoc['edmCore']['fileSpatialArea']
        
        # if row[7] == False and row[8] == True:
        #     Reverse the polygon points
        #     nosqlData['edmCore']['fileSpatialArea']=json.loads(row[6])
        # else:
        #     if row[7] == False and row[8] == False:
        #         Attempt to get minimum bounding rectangle crossing with East longitudes to the west.
        fileSpatialAreaObj = json.loads(row[5])
        fileSpatialAreaObj['orientation'] = "ccw"
        # print(fileSpatialAreaObj)
        esDoc['edmCore']['fileSpatialArea'] = fileSpatialAreaObj
        # esDoc['edmCore']['fileSpatialArea'] = json.loads(row[9])
    #         else:
    #             nosqlData['edmCore']['fileSpatialArea']=json.loads(row[5])

    # print ("fsafp type=", type(row[6]), "fsa from postgres=", row[6])
    # print ("fsain type=", type(nosqlData['edmCore']['fileSpatialArea']), "fsa in noSQL=", nosqlData['edmCore']['fileSpatialArea'])

    # print (row[5])
    # tmp = json.loads(row[5])
    # print ("tmp=",tmp)
    # fileSpatialArea = {
    #     "type" : "polygon",
    #     "coordinates" : [
    #         [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
    #     ]
    # }
    
    
    try:
        cur.execute('INSERT INTO esbatch VALUES(%s, %s)', (fileId, json.dumps(esDoc)))
        cur.close()
        conn.commit()
        updateIrTimings("insert into esbatch")
        print(datetime.now(), "Done insert into esbatch")
    except psycopg2.Error as e:
        conn.rollback()
        updateIrTimings("Failed to insert into esbatch");
        raise Exception("Database error: " + e.pgerror)
    
    # try:
    #     es = elasticsearch.Elasticsearch(
    #         [os.environ['ES_HOST']],
    #         port = os.environ['ES_PORT'],
    #         use_ssl = True,
    #         verify_certs = True,
    #         ca_certs = certifi.where())
        
    #     res = es.index(
    #         index = "file", 
    #         doc_type = "_doc", 
    #         id = fileId, 
    #         body = esDoc, 
    #         request_timeout = 20
    #     )
    #     if res['result'] == 'created' and res['_id'] == str(fileId):
    #         print(datetime.now(), "elasticSearch file:", res['result'])
    #         updateIrTimings("elasticSearch file " + res['result'])
    #     elif res['result'] == 'updated' and res['_id'] == str(fileId):
    #         print(datetime.now(), "elasticSearch file:", res['result'])
    #         updateIrTimings("elasticSearch file " + res['result'])
            
    # except elasticsearch.ElasticsearchException as ese:
    #     print(datetime.now(), "ElasticSearch ERROR:", ese)
    #     updateIrTimings("ElasticSearch ERROR: " + str(ese))
        
        
        
#        print ("es1=", es1)
#        for attr, value in dir(es1.__dict__.items():
#            print (attr, value)
#        if "fileSpatialArea" in str(es1):  # Attempt using Bounding Box
#            print (time.strftime("%Y%m%d %H%M%S", time.gmtime(time.time())), "ElasticSearch Error attempting Bounding Box Spatial Representation")
#            nosqlData['edmCore']['fileSpatialArea']=json.loads(row[9])
#            try:
#                res = es['conn'].index(index="file", doc_type="_doc", id = row[0], body = nosqlData)
#            except elasticsearch.ElasticsearchException as es2:
#                    print (time.strftime("%Y%m%d %H%M%S", time.gmtime(time.time())), "ElasticSearch Error", es1)
#            else:
#                print(time.strftime("%Y%m%d %H%M%S", time.gmtime(time.time())), "ElasticSearch Update Response=", res['result'])


    # es_dur = (time.time() - es_t1) * 1000.0
    # print(time.strftime("%Y%m%d %H%M%S", time.gmtime(time.time())), "ElasticSearch duration", es_dur, "milliseconds.")

#    res = es['conn'].get(index="file", doc_type='satobs', id = 1)
#    print(res['_source'])

#    es['conn'].indices.refresh(index="file")

#    res = es['conn'].search(index="file", body={"query": {"match_all": {}}})
#    print("Got %d Hits:" % res['hits']['total'])
#    for hit in res['hits']['hits']:
#        print(hit["_source"])

#    print("Done ES")

    deleteS3IncomingInputFile(ingestReqObj['filestoragereference'])
    

def deleteS3IncomingInputFile(fileStorageReferenceDict):
    if fileStorageReferenceDict['bucket'] == 'ndepg':
        try:
            s3.delete_object(Bucket="ndepg", Key = fileStorageReferenceDict['key'])
            updateIrTimings("S3 incoming_input file delete")
            print(datetime.now(), "Done S3 incoming_input delete")
            
        except botocore.exceptions.ClientError as e:
            print("Exception S3 incoming_input delete:", e)
            updateIrTimings("Exception S3 incoming_input delete: " + str(e))
