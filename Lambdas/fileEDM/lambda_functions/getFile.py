"""
    Author: Hieu Phung, Peter MacHarrie; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import os
import json
import psycopg2
import boto3

from ndeutil import *


print('Loading getFile')

def lambda_handler(event, context):

    print(event)
    
    try:
        if 'id' in event['pathParameters']:
            fileId = int(event['pathParameters']['id'])
        else:
            fileId = int(event['pathParameters']['fileid'])
    except Exception as e:
        print("ERROR:", str(e))
        return createErrorResponse(400, "Validation Error", "{fileid} missing or incorrectly formatted")
    
    conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
    cur = conn.cursor()

    cur.execute("SELECT filename, filemetadataxml, producthomedirectory FROM fileMetadata f, productDescription p " +
            "WHERE p.productId = f.productId AND fileId = %s", (fileId, ) )
    row = cur.fetchone()
    print("row=", row)
    
    cur.close()
    conn.rollback()
    conn.close()
    
    if row is None:
        return createErrorResponse(404, "Not Found", "No file found for File ID: " + str(fileId))
    
    # Both lambda and API Gateway have a limitation of 10MB for payload (file)
    # For now, returning signedURL to S3 object.
    
    if row[2] == "External":  # Bucket is public, return public URL
        if row[1] is None:
            return createErrorResponse(400, "Internal Error", "external resource key missing for fileId: " + str(fileId))
        externalRes = json.loads(row[1])
        print("externalRes", externalRes)
        fileUrl = "https://" + externalRes.get('bucket') + ".s3.amazonaws.com/" + externalRes.get('key')
    else:
        s3 = boto3.client('s3')
        s3FileKey = row[2] + '/' + row[0]
        fileUrl = s3.generate_presigned_url(
            ClientMethod = 'get_object',
            Params = {
                'Bucket': 'ndepg', 
                'Key' : s3FileKey
            }
        )
    
    print("fileUrl", fileUrl)

    response = {
        "isBase64Encoded": False,
        "statusCode": 307,
        "headers": {
            # "Content-Type": "application/json",
            "Location" : fileUrl
        },
        "body": fileUrl
    }
    
    return response