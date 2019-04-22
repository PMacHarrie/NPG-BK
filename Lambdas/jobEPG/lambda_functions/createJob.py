"""
    Author: Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import os
import json
import psycopg2
import boto3
import botocore

from ndeutil import *


#                     Table "public.ondemandjob"
#          Column           |            Type             | Modifiers
#---------------------------+-----------------------------+-----------
# odjobid                   | bigint                      | not null
# odalgorithmname           | character varying(255)      |
# odalgorithmversion        | character varying(25)       |
# odjobspec                 | json                        |
# odjobenqueuetime          | timestamp without time zone |
# odjobstarttime            | timestamp without time zone |
# odjobcompletiontime       | timestamp without time zone |
# odjobstatus               | character varying(35)       |
# odalgorithmreturncode     | integer                     |
# oddataselectionreturncode | integer                     |
# odjobhighesterrorclass    | character varying(8)        |
# odjobpid                  | integer                     |
# odjobcpu_util             | double precision            |
# odjobmem_util             | double precision            |
# odjobio_util              | double precision            |
# odjoboutput               | json                        |

def lambda_handler(event, context):
    
    print ("event=", event)
    # get the msg body for proxy integration case
    if 'resource' in event:
        if event.get('body') is None:
            return createErrorResponse(400, "Validation error", 
                "body (job spec json) required")
        if isinstance(event.get('body'), dict):
            inputBody = event.get('body')
        else:
            inputBody = json.loads(event.get('body'))
    else:
        inputBody = event
    
    jobSpec = inputBody
    print ("jobSpec=", jobSpec)
    
    if jobSpec.get('jobType') is None or jobSpec.get('jobType') not in ['onDemand','production']:
        return createErrorResponse(400, "Validation error", "jobType missing or is invalid")
    
    algo = jobSpec.get('algorithm')
    if algo is None:
        return createErrorResponse(400, "Validation error", "No algorithm defined in job spec JSON")
    
    inputs = jobSpec.get('inputs')
    if inputs is None or not isinstance(inputs, list) or not len(inputs) > 0:
        return createErrorResponse(400, "Validation error", "inputs missing or is invalid")

    try:
        print('Connecting to Postgres...')
        conn = psycopg2.connect(
            host = os.environ['RD_HOST'],
            dbname = os.environ['RD_DBNM'],
            user = os.environ['RD_USER'],
            password = os.environ['RD_PSWD']
            )
        cur = conn.cursor()
        
        # validate algorithm info
        cur.execute("SELECT algorithmid FROM algorithm WHERE algorithmname = %s AND algorithmversion = %s", 
                (algo.get('name'), algo.get('version')) )
        row = cur.fetchone()
        if row is None:
            return createErrorResponse(400, "Validation error", 
                "Algorithm not registered: " + algo.get('name') + " " + algo.get('version'))
        else:
            algoId = row[0]
            algo['id'] = algoId #add algoid back into jobspec object
        
        # validate product shortnames for production jobs
        if jobSpec['jobType'] == "production":
            productSnIn = []    
            for input in inputs:
                productSnIn.append(input.get('productShortName'));
            
            cur.execute("SELECT JSON_AGG(DISTINCT(productshortname)) FROM productdescription " +
                "WHERE productshortname IN %s", (tuple(productSnIn),) )
            row = cur.fetchone()
            cur.close()
            
            if row is None:
                return createErrorResponse(400, "Validation error", "Invalid input productShortNames")
            else:
                prodNotFound = list(set(productSnIn) - set(row[0]))
                if len(prodNotFound) > 0:
                    return createErrorResponse(400, "Validation error", 
                        "The following product(s) are invalid or not registered: " + str(prodNotFound))
        
        # create job
        try:
            if jobSpec['jobType'] == "onDemand":
                jobId = createOnDemandJob(jobSpec, conn)
            elif jobSpec['jobType'] == "production":
                jobId = createProductionJob()
        except Exception as e:
            return createErrorResponse(500, "Internal error", 
                "An exception was encountered (" + str(e) + "), please try again later.")

        response = {
            "isBase64Encoded": False,
            "statusCode": 201,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": '*'
            },
            "body": json.dumps({ "jobId" : str(jobId)})
        }
    
        return response
    finally:
        conn.close()
    
    
def createOnDemandJob(jobSpec, conn):
    
    cur = conn.cursor()
    sql = """
        INSERT INTO ondemandjob (odJobId, odalgorithmname, odalgorithmversion, odJobSpec, odJobEnqueueTime,
        odJobStatus) VALUES (nextval('s_ondemandjob'), %s, %s, %s, now(), 'QUEUED') RETURNING odJobId
    """
    
    cur.execute(sql, (jobSpec['algorithm']['name'], jobSpec['algorithm']['version'], json.dumps(jobSpec)) )
    row = cur.fetchone()
    newJobId = row[0]
    
    conn.commit()
    
    print("newJobId", newJobId)

    jobMsg = {
        "algorithmName": { "DataType" : "String", "StringValue" : jobSpec['algorithm']['name'] },
        "jobType": { "DataType" : "String", "StringValue" : "onDemand" },
        "odJobId": { "DataType" : "String", "StringValue" : str(newJobId) }
    }

    sns = boto3.client('sns')

    try:
        snsResponse = sns.publish(
            TopicArn = 'arn:aws:sns:us-east-1:784330347242:ProductionJob',
            Message = str(newJobId),
            Subject = 'onDemandJob',
            MessageAttributes = jobMsg
        )
        print("published SNS msg")
    except botocore.exceptions.ClientError as e:
        print("SNS error:", e.response)
        print("Deleting odjobid (" + str(newJobId) + ") from ondemandjob")
        cur.execute("DELETE FROM ondemandjob WHERE odjobid = %s", (newJobId,) )
        conn.commit()

        raise RuntimeError("SNS error") from e
    
    return newJobId
    
    
def createProductionJob():
    return "whoaaaaaa there cowboy, production not yet supported"