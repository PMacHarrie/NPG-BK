import os
import sys
import subprocess
import psycopg2
import boto3
import json
import time
import datetime
import random
import logging
from logging.handlers import RotatingFileHandler

opt_ftp = True
opt_ftps = False

# Amazon service stuff

dynamo = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamo.Table('Interface_Ingest_Request')

sqs = boto3.client('sqs', region_name='us-east-1')
ingReq_q_URL = 'https://sqs.us-east-1.amazonaws.com/784330347242/InterfaceIR'

sns = boto3.client('sns', region_name='us-east-1')
topicARN = 'arn:aws:sns:us-east-1:784330347242:NewProduct'

#dmIncoming_q_URL = 'https://sqs.us-east-1.amazonaws.com/784330347242/testQueueJreddy'

s3 = boto3.client('s3')
bucket_name = 'ndepg'

# Logging stuff

myPID = os.getpid()

logFileName = './log/getter.' + str(myPID) + '.log'
logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(logFileName, maxBytes=5000000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

#performance loggin db dw

dwconn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
dwconn = psycopg2.connect(dwconn_string)
dwcursor = dwconn.cursor()

ndeconn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
ndeconn = psycopg2.connect(ndeconn_string)
ndecursor = ndeconn.cursor()

# exclude NDECLOUD01 from pulling since it is reserved for polling
pda = { 
 1 : {},
 2 : {},
 3 : {},
 4 : {},
 5 : {},
 6 : {},
 7 : {},
 8 : {},
 9 : {},
 10 : {},
 11 : {},
}

sql = "select hostid, hostaddress, hostaccessloginid, hostaccesspassword from externalDataHost where hostaccessloginid != 'NDECLOUD01' order by hostid"

ndecursor.execute(sql)

rows = ndecursor.fetchall()

i=0
for row in rows:
	i = i + 1
	pda[i] = { 'address' : row[1], 'login' : row[2], 'password' : row[3] }


myLoginId = random.randrange(1,i+1)
logger.debug("myLoginId randomly generated from 1 to {0}: initialized to {1}".format(i, myLoginId))
pdaLoginsNum = i
logger.debug("pdaLoginsNum = {0} first = {1} last = {2}".format(pdaLoginsNum, pda[1], pda[pdaLoginsNum]))
exit

 # db dw log structure

if_object_event_row = {
        'if_filedetectedtime' : '',
        'if_filesourcecreationtime' : '',
        'if_filereceiptcompletiontime' : '',
        'if_endobservationtime' : '',
        'if_filename' : '',
        'if_filecompletionstatus' : ''
}

logger.info("This is interface ingestor for PDA getter")

# Log stuff

def wMetric(dwconn, dwcursor, metric):

        sql="""
	update if_objectevent 
		set if_filesourcecreationtime = %s, 
			if_filepullstarttime = %s, 
			if_filereceiptcompletiontime = %s, 
			if_filecompletionstatus = %s,
			if_filesize = %s 
	where if_filename = %s"""

	logger.debug("sql={0}".format(sql))
	logger.debug("metric={0}".format(metric))

        dwcursor.execute(sql, (metric['if_filesourcecreationtime'], metric['if_filepullstarttime'], metric['if_filereceiptcompletiontime'], metric['if_filecompletionstatus'], metric['if_filesize'], metric['if_filename']))
        dwconn.commit()

# Just do it......

while True:
	currentTimeStr = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        logger.info("Start of loop.")

	response = sqs.receive_message( QueueUrl = ingReq_q_URL,
		MaxNumberOfMessages=1,
		MessageAttributeNames=['All']
	)

	logger.debug("resp={0}".format(response))

	if 'Messages' not in response:
		logger.info("Sleeping.")
		time.sleep(2)
		continue

	receiptHandle = response['Messages'][0]['ReceiptHandle']
	getJob = json.loads(response['Messages'][0]['Body'])
	logger.info("message dq->{0}".format(getJob))

	# Remove from Interfacte Ingest Request

	sqs.delete_message(QueueUrl= ingReq_q_URL, ReceiptHandle=receiptHandle)
	logger.info("{0} Interface q message deleted".format(getJob['objectKey'])) 

	lftpFileName = "./work/foo." + str(myPID) + ".txt"
	fo = open(lftpFileName, "w")

	if opt_ftps:
		fo.write("set ftps:initial-prot\n")
		fo.write("set ssl:verify-certificate false\n")
		fo.write("set ftp:ssl-force true\n")
		fo.write("set ftp:ssl-protect-data false\n")
		fo.write("set net:timeout 610\n")
		fo.write("set net:max-retries 2\n")


	if opt_ftp:
		fo.write("set ssl:verify-certificate false\n")
		fo.write("set net:timeout 610\n")
		fo.write("set net:max-retries 2\n")

	fo.write("set ftp:use-site-utime true\n")     # Try to retain source creation time
	fo.write("open " + pda[myLoginId]['login'] + "@" + pda[myLoginId]['address'] + " --password " + pda[myLoginId]['password'] + "\n")
	fo.write("mget " + getJob['productproviderdirectory'] + '/' + getJob['objectKey'] + "*\n")
	fo.write("quit" + "\n")
	fo.flush()
	fo.close()

	logger.info("Running lftp -f {0} using {1}".format(lftpFileName, pda[myLoginId]['login']))

	myLoginId += 1

	if myLoginId > pdaLoginsNum:
		myLoginId = 1

	pullStartTimeStr = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

	try:
		res = subprocess.check_output(['lftp', '-f', lftpFileName], stderr=subprocess.STDOUT)
	except subprocess.CalledProcessError as e:
		logger.error("Caught lftp exception: {0}".format(e.output))
		table.delete_item(Key={'objectKey' : lftpFileName,})
		logger.error("{0} dynamodb key deleted for retry".format(getJob['objectKey'])) 
		continue

	logger.info("lftp completed for {0}".format(getJob['objectKey']))

	if "Access failed: 550 No such directory" in res:
		logger.error("directory not found: {0}".format(prodIngDict[item]['productproviderdirectory']))
		continue

	if res == '':
		# File upload from PDA, copy to S3, message SQS, then cleanup (Dynamo, SQS, LocalFile)

		# write to s3

		fileName = getJob['objectKey']
		fileSize=os.path.getsize(fileName)
		s3.upload_file(fileName, bucket_name, 'i/' + fileName)
		logger.info("{0} copied to s3 incoming_input (i/), size = {1}".format(getJob['objectKey'], fileSize)) 

		# write to SQS Data Management Incoming 

		sqsKey=fileName + ", NA"

		response = sns.publish(
			TopicArn=topicARN,
			Message = json.dumps({ "filename" : fileName }),
			Subject = "Ingest"
		)
		logger.info("{0} message sent to NewFile topic".format(getJob['objectKey'])) 

		# Get the files creation time (times from source retained)

		fileCrTimeSourceStr = time.ctime(os.path.getmtime(fileName))
		fileSize = os.path.getsize(fileName)

		# Remove file from local directory

		os.remove(fileName)
		logger.info("{0} local file deleted".format(getJob['objectKey'])) 

		# set a ttl in the dynamodb
		table.update_item(
		        Key={
		                'objectKey' : fileName,
		        },
			UpdateExpression="set TTL_attr = :r",
			ExpressionAttributeValues={
				':r': long(time.time()) + 7200
			},
		)
		logger.info("{0} dynamodb given ttl".format(getJob['objectKey']))


		completedTimeStr = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

		ifObjEvent = {
		        'if_filesourcecreationtime' : fileCrTimeSourceStr,
		        'if_filereceiptcompletiontime' : completedTimeStr,
		        'if_filepullstarttime' : pullStartTimeStr,
		        'if_filename' : fileName,
			'if_filesize' : fileSize,
		        'if_filecompletionstatus' : 'PullDone'
		}
		#print(ifObjEvent)
		wMetric(dwconn, dwcursor, ifObjEvent)
		logger.info("Completed transport.")
	else:
                logger.error("Response from lftp = {0}".format(res))
		#table.delete_item(Key={'objectKey' : fileName,})
		#logger.error("{0} dynamodb key deleted for retry".format(getJob['objectKey'])) 
