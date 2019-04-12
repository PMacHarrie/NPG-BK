import os
import sys
import subprocess
import psycopg2
import boto3
import json
import time
import datetime
import logging
from logging.handlers import RotatingFileHandler

opt_ftp = False
opt_ftps = True

# Amazon Service Config

dynamo = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamo.Table('Interface_Ingest_Request')

sqs = boto3.resource('sqs', region_name='us-east-1')
ingReq_q = sqs.get_queue_by_name(QueueName = 'InterfaceIR')

ifPoller_q_URL = 'https://sqs.us-east-1.amazonaws.com/784330347242/IfPoller'

irQueueLimit = 800

# Logging Stuff

myPID = os.getpid()

logFileName = './log/poller.' + str(myPID) + '.log'
logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(logFileName, maxBytes=25000000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

 #performance loggin db dw

dwconn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
dwconn = psycopg2.connect(dwconn_string)
dwcursor = dwconn.cursor()

nde_conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
ndeconn = psycopg2.connect(nde_conn_string)
ndecursor = ndeconn.cursor()
 # db dw log structure

if_object_event_row = {
	'if_filedetectedtime' : '',
	'if_filesourcecreationtime' : '',
	'if_filereceiptcompletiontime' : '',
	'if_endobservationtime' : '',
	'if_filename' : '',
	'if_filecompletionstatus' : ''
}

logger.info("This in interface ingestor for PDA poller worker")

# Poll as specified in the received message 

def poll(pollRequest):

 prodIngDict={}
 prodIngDict['1']=pollRequest

 for item in prodIngDict:
	i=0


	if getDirLock(prodIngDict[item]['productproviderdirectory']):
		logger.info("directory lock acquired for {0}".format(prodIngDict[item]['productproviderdirectory']))
	else:
		logger.info("directory lock failed for {0}".format(prodIngDict[item]['productproviderdirectory']))
		continue

	logger.info("polling: {0}".format(prodIngDict[item]))
	myfooFileName = './work/foo.' + str(myPID) + '.txt'
	fo = open(myfooFileName, "w")

	fo.write("open " + prodIngDict[item]['hostaccessloginid'] + "@" + prodIngDict[item]['hostaddress'] + " --password " + prodIngDict[item]['hostaccesspassword'] + "\n")
	fo.write("set net:timeout 630\n")
	fo.write("set net:max-retries 2\n")
	fo.write("cd " + prodIngDict[item]['productproviderdirectory'] + "\n")
	fo.write("ls" + "\n")
	fo.write("quit" + "\n")
	fo.flush()
	fo.close()

	res = ''

	try:
		res = subprocess.check_output(['lftp', '-f', myfooFileName], stderr=subprocess.STDOUT)
	except subprocess.CalledProcessError as e:

		logger.error("Caught lftp execption:".format(e.output))
		releaseDirLock()
		continue
		
	pollTimeStr = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
	logger.info("poll completed for {0}".format(prodIngDict[item]))

	if "Access failed: 550 No such directory" in res:
		logger.error("Error, directory not found: directory = {0}".format(prodIngDict[item]['productproviderdirectory']))
		releaseDirLock()
		continue

	#print (prodIngDict[item]['productproviderdirectory'], "Directory exists.")

	#if res == '':
	if not res.strip():
		releaseDirLock()
		continue

	#print (prodIngDict[item]['productproviderdirectory'], "Directory not empty.")

	list=res.split('\n')

	for x in list:
		#if x == '':
		if not x.strip():
			continue
		logger.info("x = ".format(x))
		line = x.split()
		#print("type of line", type(line))
		#print("file=", line[8])
		try:
			objectKey = line[8]
		except IndexError:
			logger.error("Unexpected data from lftp ls {0}".format(x))
			continue

		response = table.get_item(
		        Key = { 'objectKey' : objectKey }
		)

		if 'Item' in response:
			# We've already polled this file before, skip
		        logger.info("skipping previously polled file {0}".format(objectKey))
			continue

		# Add record to dynamo

		table.put_item(
		        Item={
		                'objectKey' : objectKey,
		                'Polled_Time'   : pollTimeStr
		        }
		)

		# Add messge to Q

		sqsMsgBody = {
			'hostaccessloginid' : prodIngDict[item]['hostaccessloginid'],
			'hostaddress' : prodIngDict[item]['hostaddress'],
			'hostaccesspassword' : prodIngDict[item]['hostaccesspassword'],
			'productproviderdirectory' : prodIngDict[item]['productproviderdirectory'],
			'objectKey' : objectKey
		}
		sMBstr = json.dumps(sqsMsgBody)
		irq_resp = ingReq_q.send_message(MessageBody=sMBstr)
		detectTimeStr = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
		logger.info("enqueue interface get for {0}".format(objectKey))

		if_object_event_row = {
			'if_filedetectedtime' : detectTimeStr,
			'if_filesourcecreationtime' : '',          # obtain this time during getting by retaining source creation time (set ftp:use-site-utime true)
			'if_filereceiptcompletiontime' : 'null',
			'if_fileobservationtime' : 'null',    
			'if_filename' : objectKey,
			'if_filecompletionstatus' : 'Detect'
		}
		wMetric(dwconn, dwcursor, if_object_event_row)

		# Check progress
		i += 1
		if i > 50:
			i=0
			logger.info("sleeping ..")
			while qDepth() > irQueueLimit:
				time.sleep(2)

	releaseDirLock()
	logger.info("directory lock released.")

def wMetric(dwconn, dwcursor, metric):
	sql="insert into if_objectevent (if_filedetectedtime, if_fileName, if_fileCompletionStatus) values (%s, %s, %s)"
	dwcursor.execute(sql, (metric['if_filedetectedtime'], metric['if_filename'], metric['if_filecompletionstatus']))
	dwconn.commit()


def wPollMetric(dwconn, dwcursor, metric):
	#print ("requestId =", metric['if_interfaceRequestId'])
	sql="update if_interfaceevent set if_interfaceRequestPollJobDequeue = %s, if_interfaceRequestPollJobComplete = %s, if_requestprocessedcnt = if_requestprocessedcnt + 1 where if_interfaceRequestId = %s"
	dwcursor.execute(sql, (metric['if_interfaceRequestPollJobDequeue'], metric['if_interfaceRequestPollJobComplete'], metric['if_interfaceRequestId']))
	dwconn.commit()

def qDepth():
	url='https://sqs.us-east-1.amazonaws.com/784330347242/InterfaceIR'
	sqs = boto3.client('sqs', region_name='us-east-1')	
	howMany = sqs.get_queue_attributes (
		QueueUrl = url,
		AttributeNames=[
			'ApproximateNumberOfMessages'
		]	
	)

	numMessages = howMany['Attributes']['ApproximateNumberOfMessages']
	logger.info("Interface IR SQS messages in Q {0}".format(numMessages))
	return int(numMessages)

def getDirLock(directoryName):
	sql = "select pda_directory from pda_dirlock where pda_directory = %s for update nowait"
	#print "before execute"
	try:
		ndecursor.execute(sql, (directoryName,))
	except psycopg2.Error as e:
		ndeconn.commit()
		#print "e=", e
		return False
		
	row = ndecursor.fetchone()
	#print "after execute"
	#print row
	if row[0] == directoryName:
		return True

	return True

def releaseDirLock():
	ndeconn.commit()

# Do stuff

logger.info("Start ...")

sqs2 = boto3.client('sqs', region_name='us-east-1')

while True:

	if qDepth() > irQueueLimit:
		time.sleep(1)
		continue

	response = sqs2.receive_message( 
		QueueUrl = ifPoller_q_URL,
		MaxNumberOfMessages=1,
		MessageAttributeNames=['All']
	)
	#print ("r=", response)

	if 'Messages' not in response:
		time.sleep(1)
		continue
	
	#print ("Polling.")

	pollStartTimeStr = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

	receiptHandle = response['Messages'][0]['ReceiptHandle']
	pollRequest = json.loads(response['Messages'][0]['Body'])

	sqs2.delete_message(QueueUrl= ifPoller_q_URL, ReceiptHandle=receiptHandle)
	logger.info("{0} poll q message acquired and deleted".format(pollRequest['productproviderdirectory'])) 

	poll(pollRequest)

	pollCompleteTimeStr = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
	#print ("r=", response['Messages'][0]['MessageId'])
	pollMetric = {
		'if_interfaceRequestPollJobDequeue' : pollStartTimeStr,
		'if_interfaceRequestPollJobComplete' : pollCompleteTimeStr,
		'if_interfaceRequestId'             : response['Messages'][0]['MessageId']	
	}
	wPollMetric(dwconn, dwcursor, pollMetric)


