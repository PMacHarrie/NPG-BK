# Original Auther: Peter MacHarrie

import time
import datetime
import subprocess as sp
import multiprocessing as mp
import os
import boto3
from botocore.exceptions import ClientError
import psycopg2
import json
import socket
import sys
import logging
from logging.handlers import TimedRotatingFileHandler

sqs = boto3.client('sqs', region_name='us-east-1')
#q_url='https://sqs.us-east-1.amazonaws.com/784330347242/jobOnDemand'
#q_url='https://sqs.us-east-1.amazonaws.com/784330347242/JobRegularAny'
#q_url='https://sqs.us-east-1.amazonaws.com/784330347242/JobSmallAny'
#q_url='https://sqs.us-east-1.amazonaws.com/784330347242/JobLargeAny'
#q_url='https://sqs.us-east-1.amazonaws.com/784330347242/JobSequential-1'
#q_url='https://sqs.us-east-1.amazonaws.com/784330347242/JobSequential-2'
#q_url='https://sqs.us-east-1.amazonaws.com/784330347242/JobRegular-HighMemoryAny'


# Get environment info

myPID = str(os.getpid())
hostname = socket.gethostname()


# Configure loggging

logFileName = 'nodeMgr' + myPID + ".log"
LOG = open(logFileName, 'a')

#spLogFileName = 'nodeMgr_subprocess.log'
#spLOG = open(spLogFileName, 'a')

# Redirect syserr to log
#sys.stderr = LOG

jb = {}

# Get job box configuration


# Need to add mandatory parameters <<<<--------

confFileName = sys.argv[1]
with open (confFileName) as f:
        jb = json.load(f)

numBoxes = sys.argv[2]

jb["numBoxes"] = int(numBoxes)
jb["free"]     = int(numBoxes)
jb["inuse"]    = 0

for i in range(1, jb["numBoxes"]+1):
        print "i=", i
        jb[str(i)] = { "p" : None, "pid" : None, "status": "free"}


print "jb=", json.dumps(jb, indent=2)

#jb = {
#               "qUrlPrefix" : "https://sqs.us-east-1.amazonaws.com/784330347242/JobSmall",
#               "numBoxes" : 2,
#               "free" : 2,
#               "inuse" : 0,
#               1 : {'p' : None, 'pid' : None, 'status' : 'free'},
#               2 : {'p' : None, 'pid' : None, 'status' : 'free'},
#               "jobPriority" : {
#                       5 : {
#                               "name" : "KPP",
#                               "q_url" : "https://sqs.us-east-1.amazonaws.com/784330347242/JobSmall-KPP"
#                       },
#                       10 : {
#                               "name" : "High",
#                               "q_url" : "https://sqs.us-east-1.amazonaws.com/784330347242/JobSmall-High"
#                       },
#                       20 : {
#                               "name" : "Medium",
#                               "q_url" : "https://sqs.us-east-1.amazonaws.com/784330347242/JobSmall-Medium"
#                       },
#                       30 : {
#                               "name" : "Low",
#                               "q_url" : "https://sqs.us-east-1.amazonaws.com/784330347242/JobSmall-Low"
#                       }
#               }
#}

# Dev
#jobStack = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

#  Log

def logit(x):
        myName = "nodeMgr"
        #log_dt=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        # print >>LOG, log_dt + " " + hostname + " " + myPID + " " + myName + " " + x
        logger.info(hostname + " " + myPID + " " + myName + " " + x)
        return None



def runJob(jobId):
        #print "running", jobId, "mypid = ", os.getpid()
        #sp.call(["python", "runJobv2.py", "onDemand", str(jobId)], stdout=LOG, stderr=LOG)
        sp.call(["python", "runJobv3.py", "production", str(jobId)], stdout=LOG, stderr=LOG)
        logMessage = str(jobId) + "run by PID " + str( os.getpid() )  + "completed."
        logit(logMessage)
        return 0

def runProductionJob(jobId):
        #print "mypid = ", os.getpid()
        logit( "prJobId: " + str(jobId) + " managed by PID: " + str(os.getpid() ) +  " spawned." )
        LOG.flush()
        #sp.call(["python", "runJobv2.py", "production", str(jobId)], stdout=LOG, stderr=LOG)
        sp.call(["python", "runJobv3.py", "production", str(jobId)], stdout=LOG, stderr=LOG)
        logit( "prJobId: " + str(jobId) + " managed by PID: " + str(os.getpid() ) +  " completed." )
        return 0


def getJobBox(jobId):

        #print "mypid = ", os.getpid()
        #print "getJobBox jb=", jb
        assignedJB= -1

        if jb['free'] > 0:
                i=1
                for x in range(1, jb['numBoxes']+1):
                        #print "x=", x
                        #print "getJB jb=", jb[str(x)]['p']
                        jdim = str(x)
                        if jb[jdim]['p'] == None:
                                jb['free'] -= 1
                                jb['inuse'] += 1
                                #print "returning x=", x, "i=", i
                                i+=1
                                jb[jdim]['status']="taken"
                                jb[jdim]['jobId']=jobId
                                assignedJB = jdim
                                return assignedJB
                                logit( "Job Box claimed by job:" + str(jobId))
                        #print "after if 1"
                #print "after for"
        #print "after if 2"

        return assignedJB

def releaseJB():
        for x in range(1, jb['numBoxes']+1):
                #print "releaseJB x=", x
                #print "jb=", jb
                jdim = str(x)
                if jb[jdim]['status'] == "taken":
                        if jb[jdim]['p'].is_alive():
                                continue
                                #print "jb x=", x, "is still running."
                        else: # free Willy!
                                logit("Job Box freed from job:" + str(jb[jdim]['jobId']))
                                jb['free'] += 1
                                jb['inuse'] -= 1
                                jb[jdim]['p'] = None
                                jb[jdim]['pid'] = None
                                jb[jdim]['status'] = 'free'
                                jb[jdim]['jobId'] = None
        return 0

def getJob():

        jobId=-1

#       print "jobStack=", jobStack
#       if len(jobStack) > 0:
#               jobId = jobStack.pop()

        response = {}
        q_url=None

        for x in sorted(jb["jobPriority"]):
        #       print "priority", x, jb["jobPriority"][x], datetime.datetime.now()
                q_url=jb["jobPriority"][x]["q_url"]
        #       print "polling:", q_url
                try:
                        response = sqs.receive_message(
                                QueueUrl=jb["jobPriority"][x]["q_url"],
                                MaxNumberOfMessages=1,
                                MessageAttributeNames=['All'],
                                VisibilityTimeout=30,
                                WaitTimeSeconds=0
                        )
                except ClientError as e:
                        logit("SQS Exception: " + str(e))
                        return -1
        #       print "response=", response
                if 'Messages' in response:
                        break

#       print "Done get message.", datetime.datetime.now()
#       exit()

        #print ('dq response=', response)

        if 'Messages' in response:
                noJobs = 0
                for message in response['Messages']:
                        msg=message
                #print "msg=", msg, "<<<"
                #print "type msg=", type(msg)
                #print "body=", msg['Body']
                        body = json.loads(msg['Body'])
                #print "type body=", type(body)
                #print "body=", body['MessageAttributes']['prJobId']
                        jobId = body['MessageAttributes']['prJobId']['Value']
                        receiptHandle = msg['ReceiptHandle']
                        logit("Got jobId=" + str( jobId) )
                        #print "rH=", receiptHandle
                        sqs.delete_message(QueueUrl=q_url, ReceiptHandle=receiptHandle)
                        logit("Job message dequeued:" + json.dumps(body['MessageAttributes']) )

        return jobId




if __name__ == '__main__':

        logger = logging.getLogger("Rotating Log")
        logger.setLevel(logging.INFO)
    
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    
        rfh = TimedRotatingFileHandler(
            logFileName,
            when = "midnight",
            interval = 1,
            backupCount = 30)
        rfh.setFormatter(formatter)
        rfh.setLevel(logging.DEBUG)
        logger.addHandler(rfh)
        
        LOG = open(rfh.baseFilename, 'a')
        
        # print "Main"
        logit("Starting nodeMgr.")

# Initialize Job Boxes


# Main loop

        i=0

        #print "loop through priorities"

#       print "jb=", json.dumps(jb, indent=2)
#       for x in sorted(jb["jobPriority"]):
#               print "priority", x, jb["jobPriority"][x]

#       exit(1)

        while (True):

                if(rfh.shouldRollover(rfh)):
                        logit('Log Roll Over!')
                        LOG = open(rfh.baseFilename, 'a')
        # If a job box is free, and there is a job to run, get it and run it.

                if jb['free'] > 0:
                        jobId = getJob()
                        if jobId > 0:
                                myjb = getJobBox(jobId)
                                #logit("Assigned Job:" + str(jobId))
                                #print "jb=", jb
                                jobIdStr = str(jobId)
                                jb[myjb]['p'] = mp.Process(target=runProductionJob, args=(jobId,))
                                #print "called Process"
                                jb[myjb]['p'].start()
                                jb[myjb]['pid']= jb[myjb]['p'].pid
                                logit("nodeMgr subprocess:" + str(jb[myjb]) + " will invoke job: " + jobIdStr)
                                #print "jb=", jb
                        else:
                                time.sleep(.1)

                i += 1
#                time.sleep(0.1)

#                print "i=", i

        # Reap Completed Jobs and free up the job box.

                if jb['inuse'] > 0: # See if a job terminated
                        releaseJB()
                        #logit( "done releaseJB" )

                #print "End Loop iteration=", i
#                if i >= 1:
#                        logit("nodeMgr shutting down, i==1.")
#                        exit()

                # Dump the job boxes every 60 iterations

                if i % 600 == 0 :
                #       print "jb=", jb
                        logit("Job Boxes current state: " + str(jb))

                sys.stdout.flush()
                time.sleep(0.1)

        ### End of while loop

        logit("nodeMgr shutting down, done loop.")



