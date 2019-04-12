import re
import os
import sys
import boto3
import subprocess
from subprocess import Popen, PIPE
import psycopg2
import time
import json
import datetime
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler

opt_ftp = True
opt_ftps = False

s3 = boto3.client('s3')
bucket_name = 'ndepg'

sns = boto3.client('sns', region_name='us-east-1')
topicARN = 'arn:aws:sns:us-east-1:784330347242:NewProduct'

logFileName = '/home/ec2-user/getIMS/log/getIMS.log'
logger = logging.getLogger()
handler = RotatingFileHandler(logFileName, maxBytes=100000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

dwconn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
dwconn = psycopg2.connect(dwconn_string)
dwcursor = dwconn.cursor()

pattern = "NIC\.IMS_v3_\d{9}_4km.asc.gz"

ndeconn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
ndeconn = psycopg2.connect(ndeconn_string)
ndecursor = ndeconn.cursor()

def wMetric(dwconn, dwcursor, metric):

        sql="update if_objectevent set if_filesourcecreationtime = %s, if_filereceiptcompletiontime = %s, if_filecompletionstatus = %s where if_filename = %s"

        logger.debug("sql={0}".format(sql))
        logger.debug("metric={0}".format(metric))

        dwcursor.execute(sql, (metric['if_filesourcecreationtime'], metric['if_filereceiptcompletiontime'], metric['if_filecompletionstatus'], metric['if_filename']))
        dwconn.commit()


# download the file, copy to S3, write to new message topic
def downloadFile(yr, fileName):
	logger.info("Downloading {0}".format(fileName))
	try:
                directory = "https://www.natice.noaa.gov/pub/ims/ims_v3/snow/6144asc/{0}/{1}".format(yr, fileName)
		res = Popen(["curl", "-kO", directory], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		logger.info("download completed for {0}".format(fileName))
		res.communicate()
	
		s3.upload_file(fileName, bucket_name, 'i/' + fileName)
                logger.info("copied {0} to S3 bucket".format(fileName))
	
		fileCrTimeSourceStr = time.ctime(os.path.getmtime(fileName))
		os.remove(fileName)

                response = sns.publish(
                        TopicArn=topicARN,
                        Message = json.dumps({ "filename" : fileName }),
                        Subject = "Ingest"
                )
                logger.info("{0} message sent to NewFile topic".format(fileName))


		completedTimeStr = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                ifObjEvent = {
                        'if_filesourcecreationtime' : fileCrTimeSourceStr,
                        'if_filereceiptcompletiontime' : completedTimeStr,
                        'if_filename' : fileName,
                        'if_filecompletionstatus' : 'PullDone'
                }
                wMetric(dwconn, dwcursor, ifObjEvent)
		  
        except Exception as e:
                logger.error("Caught exception: {0}".format(e))

# check directory for files we do not have
def checkDirectory(dateStr):

	directory = "https://www.natice.noaa.gov/ims/asc_v3_6144_{0}.html".format(dateStr)
	try:
		logger.info("Curling: {0}".format(directory))
		res = Popen(["curl", "-k", directory], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                result, err = res.communicate()
		logger.info("Curling completed")
		list = re.findall(pattern, result)
		files = set(list)
		for imsFile in files:
			logger.info("Checking {0}".format(imsFile))  
                        nameTuple = (imsFile,)
                        if nameTuple in files_ims:
                        	logger.info("{0} already have".format(nameTuple))
                        else:
				downloadFile(dateStr, imsFile)

	except Exception as e:
		logger.error("Caught exception: {0}".format(e.output))

while True:

        logger.info("Start of polling cycle")
        
	# get the lists of files we already have

	sql = "select filename from filemetadata where productId = 3495"
	ndecursor.execute(sql)
	files_ims = ndecursor.fetchall()
	logger.debug("IMS files we already have: {0}".format(files_ims))

	date1 = datetime.now() - timedelta(1)
        year1 = date1.year
	dateStr1 = datetime.strftime(date1, '%Y')
	checkDirectory(dateStr1)
	
	date2 = datetime.now() - timedelta(2)
        year2 = date2.year
	if year1 != year2:
		dateStr2 = datetime.strftime(date2, '%Y')
		checkDirectory(dateStr2)
	
        logger.info("Going to sleep")
        time.sleep(3600)
