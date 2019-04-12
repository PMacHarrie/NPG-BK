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

opt_ftp = True
opt_ftps = False

# amazon sqs stuff

sqs = boto3.resource('sqs', region_name='us-east-1')
ifPoller_q = sqs.get_queue_by_name(QueueName = 'IfPoller')

# appdb stuff

conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()

cursor.execute("""
select hostaddress, hostaccessloginid, hostaccesspassword, productproviderdirectory
from externalDataHost e,
(select distinct productproviderdirectory from productdatasource) s
where e.hostaccessloginid = 'NDECLOUD01'
""")

conn.commit()

#logging stuff

myPID = os.getpid()

logFileName = './log/main.' + str(myPID) + '.log'
logger = logging.getLogger("Rotating Log")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(logFileName, maxBytes=10000000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info("This in interface PDA poller main.")

 #performance loggin db dw

dwconn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
dwconn = psycopg2.connect(dwconn_string)
dwcursor = dwconn.cursor()

# Initialize polling configuration

prodIngDict = {}

rows = cursor.fetchall()

for row in rows:
	prodIngDict[row[3]] = {
		'hostaddress' : row[0],
		'hostaccessloginid' : row[1],
		'hostaccesspassword' : row[2],
		'productproviderdirectory' : row[3],
	}
	logger.info("dict = {0}".format(prodIngDict[row[3]]))

# Log stuff

def wMetric(dwconn, dwcursor, metric):

        sql = "insert into if_interfaceevent (if_interfaceRequestPollJobEnqueue, if_interfaceRequest, if_interfaceRequestId ) values (%s, %s, %s)"
        logger.debug("sql = {0}".format(sql))
        logger.debug("metric = {0}".format(metric))

        dwcursor.execute(sql, (metric['if_interfaceRequestPollJobEnqueue'], metric['if_interfaceRequest'], metric['if_interfaceRequestId']))
        dwconn.commit()

def putpollmsg():
 i=0

 logger.info("Starting create poll messages")
 for item in prodIngDict:
	time.sleep(.01)
	sqsMsgBody = json.dumps({
		'hostaccessloginid' : prodIngDict[item]['hostaccessloginid'],
		'hostaddress' : prodIngDict[item]['hostaddress'],
		'hostaccesspassword' : prodIngDict[item]['hostaccesspassword'],
		'productproviderdirectory' : prodIngDict[item]['productproviderdirectory']
	})
	ifq_resp = ifPoller_q.send_message(MessageBody=sqsMsgBody)
	logger.info("enqueue interface poll for {0}".format(prodIngDict[item]['productproviderdirectory']))
	#print ('ifq_id=',ifq_resp['ResponseMetadata']['RequestId'])
	#print ('ifq_resp=', ifq_resp)
	
 	currentTimeStr = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
	metric = {
		'if_interfaceRequestPollJobEnqueue' : currentTimeStr,
		'if_interfaceRequest'               : "Pull:" + prodIngDict[item]['hostaddress'] + ":" + prodIngDict[item]['productproviderdirectory'],
		'if_interfaceRequestId'             : ifq_resp['MessageId']
	}
	wMetric(dwconn, dwcursor, metric)

 currentTimeStr = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
 logger.info("Done create poll messages")

def qDepth():
	url='https://sqs.us-east-1.amazonaws.com/784330347242/IfPoller'
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

# Now do stuff

logger.info("Start ...")
#
# PDA - Pull Subscriptions
# PDA deletes if files after pull
# If files are not pulled, PDA deletes files after 48? hours
# Each product subscription place files in a separate PDA directory
# 

# PDA 140.x.x.x 
#  users: consumer1 - 6
#  Subscriptions consumern ( one sub for each product )
#   Ideal pull scenario, give all newest data first,
#     work off backlogs after.
#   Need to exclude files still in PDA if poller a second time (and get of file was not attempted (and failed))
#  

while True:
	if qDepth() <= 25:
		logger.info("Polling.")
		putpollmsg()

	time.sleep(1)

