'''
File name: retryGfs.py
Author: Hieu Phung
Date created: 2018-12-20
Python Version: 3.6
'''

import boto3
import json
import os
import sys
from datetime import datetime
import re
import time

s3Resource = boto3.resource('s3')
s3 = boto3.client('s3')

sns = boto3.client('sns', region_name='us-east-1')
newProdTopic = 'arn:aws:sns:us-east-1:784330347242:NewProduct'

def myMain():

    while(1):
        iiLs = s3Resource.Bucket('ndepg').objects.filter(Prefix='i/')
        print(datetime.now(), "Listing (i/) incoming_input")
    
        for iiObj in iiLs:
            # print(iiObj.key)
            filename = iiObj.key.rsplit('/', 1)[-1]
            # print(filename)
            r = re.search('(gfs\..*)\.test', filename)
            if r:
                print(datetime.now(), "Matched file: " + r.group(0))
                newFilename = r.group(1)
    
                print(datetime.now(), "Renaming S3 file to: " + newFilename)
                s3.copy_object(Bucket="ndepg", CopySource="ndepg/i/" + filename, Key="i/" + newFilename)
    
                print(datetime.now(), "Deleting old file")
                s3.delete_object(Bucket="ndepg", Key="i/" + filename)
    
                print(datetime.now(), "Publishing to SNS")
                response = sns.publish(
                    TopicArn = newProdTopic,
                    Message = json.dumps({ "filename" : newFilename }),
                    Subject = "GFS_Retry"
                )

        print(datetime.now(), "Sleeping for 10s")
        time.sleep(10)

if __name__ == "__main__":

    myPID = str(os.getpid())

    thisScriptName = sys.argv[0][:-3]

    logFileName = thisScriptName + "." + myPID + '.log'
    logFile = open(logFileName, 'w')
    sys.stdout = sys.stderr = logFile

    myMain()



