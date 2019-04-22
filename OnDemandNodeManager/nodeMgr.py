import time
import datetime
import subprocess as sp
import multiprocessing as mp
import os
import boto3
import botocore
import psycopg2
import json
import socket
import sys

sqs = boto3.client('sqs', region_name='us-east-1')
q_url='https://sqs.us-east-1.amazonaws.com/784330347242/jobOnDemand'

# Get environment info

myPID = str(os.getpid())
hostname = socket.gethostname()


# Configure loggging

logFileName = 'nodeMgr' + myPID + ".log"
LOG = open(logFileName, 'a')

#spLogFileName = 'nodeMgr_subprocess.log'
#spLOG = open(spLogFileName, 'a')

    # Redirect syserr to log
sys.stderr = LOG

# Get job box configuration


jb = {
	"numBoxes" : 4,
	"free" : 4,
	"inuse" : 0,
	1 : {'p' : None, 'pid' : None, 'status' : 'free'},
	2 : {'p' : None, 'pid' : None, 'status' : 'free'},
	3 : {'p' : None, 'pid' : None, 'status' : 'free'},
	4 : {'p' : None, 'pid' : None, 'status' : 'free'}
}


# Dev
#jobStack = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]



#  Log

def logit(x):
	log_dt=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
	print >>LOG, log_dt + " " + hostname + " " + myPID + " " + x
	return None



def runJob(jobId):
	print "mypid = ", os.getpid()
	print "running", jobId
	sp.call(["python", "runJob.py", "onDemand", str(jobId)], stdout=LOG, stderr=LOG)
	print os.getpid(), "is done"
	return 0

def runProductionJob(jobId):
	print "mypid = ", os.getpid()
	print "running", jobId
	sp.call(["python", "runJob.py", "production", str(jobId)], stdout=LOG, stderr=LOG)
	print os.getpid(), "is done"
	return 0


def getJobBox(jobId):

	#print "mypid = ", os.getpid()
	#print "getJobBox jb=", jb
	assignedJB= -1

	if jb['free'] > 0:
		i=1
		for x in range(1, jb['numBoxes']+1):
#			x=x+1
			print "x=", x
			print "getJB jb=", jb[x]['p']
			if jb[x]['p'] == None:
				jb['free'] -= 1
				jb['inuse'] += 1
				#print "returning x=", x, "i=", i
				i+=1
				jb[x]['status']="taken"
				jb[x]['jobId']=jobId
				assignedJB = x
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
		if jb[x]['status'] == "taken":
			if jb[x]['p'].is_alive():
				continue
				#print "jb x=", x, "is still running."
			else: # free Willy!
				logit("Job Box freed from job:" + str(jb[x]['jobId']))
				jb['free'] += 1
				jb['inuse'] -= 1
				jb[x]['p'] = None
				jb[x]['pid'] = None
				jb[x]['status'] = 'free'
				jb[x]['jobId'] = None
	return 0

def getJob():

	jobId=-1
#	print "jobStack=", jobStack
#	if len(jobStack) > 0:
#		jobId = jobStack.pop()

	response = sqs.receive_message(
		QueueUrl=q_url,
		MaxNumberOfMessages=1,
		MessageAttributeNames=['All'],
		VisibilityTimeout=0,
		WaitTimeSeconds=0
	)

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
		#print "body=", body['MessageAttributes']['odJobId']
			jobId = body['MessageAttributes']['odJobId']['Value']
			receiptHandle = msg['ReceiptHandle']
			logit("Got jobId=" + str( jobId) )
			#print "rH=", receiptHandle
			sqs.delete_message(QueueUrl=q_url, ReceiptHandle=receiptHandle)
			logit("Job message dequeued:" + json.dumps(body['MessageAttributes']) )

	return jobId




if __name__ == '__main__':

	# print "Main"
	logit("Starting nodeMgr.")

# Initialize Job Boxes


# Main loop

	i=0

	while (True):

	# If a job box is free, and there is a job to run, get it and run it.

		if jb['free'] > 0:
			jobId = getJob()
			if jobId > 0:
				myjb = getJobBox(jobId)
				#logit("Assigned Job:" + str(jobId))
				#print "jb=", jb
				jobIdStr = str(jobId)
				jb[myjb]['p'] = mp.Process(target=runJob, args=(jobId,))
				#print "called Process"
				jb[myjb]['p'].start()
				#print "called start"
				#print "called join"
				jb[myjb]['pid']= jb[myjb]['p'].pid
				logit("nodeMgr subprocess:" + str(jb[myjb]) + " will invoke job: " + jobIdStr)
				#print "jb=", jb
			else:
				time.sleep(.1)

		i += 1
		time.sleep(0.2)

		print "i=", i

	# Reap Completed Jobs and free up the job box.

		if jb['inuse'] > 0: # See if a job terminated
			releaseJB()
			#logit( "done releaseJB" )

		#print "End Loop iteration=", i
		if i == 100000000:
			logit("nodeMgr shutting down, i==10000000.")
			exit()

		# Dump the job boxes every 60 iterations

		if i % 60 == 0 :
			print "jb=", jb
			#logit("Job Boxes current state: " + str(jb))

	### End of while loop

	logit("nodeMgr shutting down, done loop.")

