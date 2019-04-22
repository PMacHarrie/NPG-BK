"""
    Author: Hieu Phung, Peter MacHarrie; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import os
import re
import json
import psycopg2
import elasticsearch
import certifi
import boto3
import h5py
import netCDF4

from ndeutil import *


print('Loading get_file_array')

def lambda_handler(event, context):
    
    print(event)
    
    try:
        fileId = int(event['pathParameters']['fileid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "{fileid} missing or incorrectly formatted")
        
    arrayName = event['pathParameters'].get('arrayname')
    if arrayName is None:
        return createErrorResponse(400, "Validation Error", "{arrayname} parameter is missing")
    
    es = elasticsearch.Elasticsearch(
        [os.environ['ES_HOST']],
        port = os.environ['ES_PORT'],
        use_ssl = True,
        verify_certs = True,
        ca_certs = certifi.where()
    )
    
    # check if arrayname exists in file
    try:
        res = es.get(
            index = "file", 
            doc_type = "_doc", 
            id = fileId, 
            _source_include = ['objectMetadata']
        )
        objMdStr = json.dumps(res['_source'])
        
        r = re.search("\"" + arrayName + "\":", objMdStr)
        if not r:
            return createErrorResponse(400, "Validation Error", "{arrayname} " + arrayName + " not found in file: ")
        
    except elasticsearch.exceptions.NotFoundError as e:
        s = "No files found for File ID: " + str(fileId)
        print(s)
        return createErrorResponse(404, "Not Found", s)
    except Exception as e:
        print("ERROR while querying:", str(e))
        return createErrorResponse(500, "Error while querying", str(e))
    
    try:
        conn = psycopg2.connect(
                host = os.environ['RD_HOST'],
                dbname = os.environ['RD_DBNM'],
                user = os.environ['RD_USER'],
                password = os.environ['RD_PSWD']
                )
        cur = conn.cursor()
        
        body = {}
        cur.execute("SELECT fileName, filemetadataxml, producthomedirectory, productShortName, productFileFormat "
            "FROM fileMetadata f, productDescription p WHERE fileId = %s AND p.productId = f.productId", (fileId,) )
        row = cur.fetchone()
        
        cur.close()
        conn.rollback()
    finally:
        conn.close()
    
    if row is None:
        print("ERROR Did not find record in Postgres but found in Elasticsearch, fileId: " + fileId)
        return createErrorResponse(500, "Internal error", "No SQL record found for File ID: " + str(fileId))
        
    print("row", row)
    
    fileName = row[0]
    fileMetadataXml = row[1]
    productHomeDirectory = row[2]
    productShortName = row[3]
    productFileFormat = row[4]
    
    if productHomeDirectory == 'External':
        externalRes = json.loads(fileMetadataXml)
        print("externalRes", externalRes)
        s3Bucket = externalRes.get('bucket')
        s3Key = externalRes.get('key')
    else:
        s3Bucket = "ndepg"
        s3Key = productHomeDirectory + "/" + fileName
    print ("s3Bucket", s3Bucket)
    print ("s3Key", s3Key)
    
    s3 = boto3.client('s3')
    
    # Coppy file from s3 into temp dir and extract array
    localFileName = "/tmp/" + fileName
    s3.download_file(s3Bucket, s3Key, localFileName)

    if os.path.isfile(localFileName):
        print ("file copied, size=", os.path.getsize(localFileName))
    else:
        return createErrorResponse(500, "Internal error", "S3 copy failed: " + fileName)
    
    try:
        try:
            if "hdf5" in productFileFormat.lower():
                file = h5py.File(localFileName, 'r')
                arrayStr = '/All_Data/' + productShortName + '_All/' + arrayName
                print('arrStr=', arrayStr)
                arrayData = file[arrayStr]
            elif "netcdf" in productFileFormat.lower():
                file = netCDF4.Dataset(localFileName, 'r')
                arrayData = file[arrayName]
            else:
                return createErrorResponse(400, "Validation error", productFileFormat + " format not supported")
        except IndexError as ie:
            return createErrorResponse(400, "Validation error", "Did not find array name: " + arrayName + " in file")

        outFile = open("/tmp/out.bin", "wb")
        outFile.write(arrayData[:].tobytes())
        outFile.close()
        
        outFileName = fileName.split(".")[0] + "_" + arrayName + ".bin"
        s3KeyOut = "outgoing/" + outFileName 
        s3BucketOut = "ndepg"
        
        print ("s3KeyOut", s3KeyOut)
        
        s3.upload_file("/tmp/out.bin", s3BucketOut, s3KeyOut)

        presigned_url = s3.generate_presigned_url(
            ClientMethod = 'get_object',
            Params = {
                'Bucket': s3BucketOut, 
                'Key': s3KeyOut
            }
        )
        print(presigned_url)
    
        response = {
            "isBase64Encoded": False,
            "statusCode": 307,
            "headers": { "Location" : presigned_url},
            "body": presigned_url
        }
        
        return response
    
    except Exception as e:
        return createErrorResponse(500, "Internal error",  "An exception was encountered, please try again later.")
    finally:
        print("removing local temp file: " + localFileName)
        os.remove(localFileName)
        if os.path.isfile('/tmp/out.bin'):
            print("removing local temp file: /tmp/out.bin")
            os.remove('/tmp/out.bin')