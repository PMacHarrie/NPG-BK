import boto3
import json
import os
import sys
import requests
from datetime import datetime

sqs = boto3.client('sqs', region_name='us-east-1')
# sns = boto3.client('sns', region_name='us-east-1')
# s3 = boto3.resource('s3')

bigIngestQ = 'https://sqs.us-east-1.amazonaws.com/784330347242/BigIngestQ'
bigIngestDLQ = 'https://sqs.us-east-1.amazonaws.com/784330347242/BigIngestDLQ'
completeJobSpecQ = 'https://sqs.us-east-1.amazonaws.com/784330347242/PGFactory_CompleteJobSpec'
completeJobSpecDLQ = 'https://sqs.us-east-1.amazonaws.com/784330347242/PGFactory_CompleteJobSpec_DLQ'

def myMain():

    srcQ = bigIngestDLQ
    dstQ = bigIngestQ
    
    count = 400
    while count > 0:

        print(datetime.now(), '===== Getting messages from ' + srcQ)
        
        response = sqs.receive_message(
            QueueUrl = srcQ,
            MaxNumberOfMessages=1,
            AttributeNames = ['All'],
            MessageAttributeNames = ['All'],
            WaitTimeSeconds = 10
        )
    
        if 'Messages' in response:
            queueMsg = response['Messages'][0]
            
            msgBodyStr = queueMsg['Body']
            receipt_handle = queueMsg['ReceiptHandle']
            
            # msgBody = json.loads(msgBodyStr)
    
            # msgStr = msgBody.get('Message')
            
            print(datetime.now(), msgBodyStr)
            
            print(datetime.now(), 'Sending message from ' + dstQ)
            dstSqsResponse = sqs.send_message(
                QueueUrl = dstQ ,
                MessageAttributes = {},
                MessageBody = (msgBodyStr)
            )
            
            # print(datetime.now(), "DestQ response:", dstSqsResponse)
            
            if dstSqsResponse.get('ResponseMetadata').get('HTTPStatusCode') == 200:
                print(datetime.now(), "Deleting message from", srcQ)
                sqs.delete_message(
                    QueueUrl = srcQ,
                    ReceiptHandle = receipt_handle
                )
            else:
                print(datetime.now(), "Got non-200 response from SQS dest", dstSqsResponse)
            
        else:
            print(datetime.now(), 'No Messages in Queue')
            count = 0
            
        count = count - 1

if __name__ == "__main__":
    myMain()
