import boto3
import sys
from datetime import datetime
import json

from lambda_bigingest import lambda_handler

def pollBigIngestSqsQ():
    print("starting pollSqs BigIngestQ")

    sqs = boto3.client('sqs', region_name='us-east-1')

    queueUrl = 'https://sqs.us-east-1.amazonaws.com/784330347242/BigIngestQ'

    while(1):
        print(datetime.now(), 'Getting messages')
        response = sqs.receive_message(
            QueueUrl = queueUrl,
            MaxNumberOfMessages=1,
            AttributeNames = [
                'All'
            ],
            MessageAttributeNames = [
                'All'
            ],
            #VisibilityTimeout = 10,
            WaitTimeSeconds = 10
        )

        # print(response)

        if 'Messages' in response:
            msg = response['Messages'][0]
            print(datetime.now(), "Got Message from Queue")

            # print(msg)
            snsMsgBodyStr = msg['Body']
            receipt_handle = msg['ReceiptHandle']
            
            snsMsgBody = json.loads(snsMsgBodyStr)
            msgBody = snsMsgBody.get('Message')
            
            print(datetime.now(), "SNS Msg Body:", snsMsgBodyStr)

            print(datetime.now(), "Calling lambda_handler")
            try:
                ingestRes = lambda_handler(msgBody, None)
                print(datetime.now(), "lambdaResponse:", ingestRes)
                sqs.delete_message(
                    QueueUrl=queueUrl,
                    ReceiptHandle=receipt_handle
                )
                print("Processed and deleted message")
            except Exception as e:
                eType, eValue, eTraceback = sys.exc_info()
                print("Exception in lambda_handler:", eType, eValue, str(eTraceback.tb_lineno))
        else:
            print(datetime.now(), '>> No Messages in Queue')


if __name__ == "__main__":
    pollBigIngestSqsQ()
