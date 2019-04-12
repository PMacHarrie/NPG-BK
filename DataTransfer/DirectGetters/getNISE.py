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

logFileName = '/home/ec2-user/getNICE/log/getNICE.log'
logger = logging.getLogger()
handler = RotatingFileHandler(logFileName, maxBytes=100000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

dwconn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
dwconn = psycopg2.connect(dwconn_string)
dwcursor = dwconn.cursor()

pattern = "NIC\.NICE_v3_\d{9}_4km.asc.gz"

ndeconn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
ndeconn = psycopg2.connect(ndeconn_string)
ndecursor = ndeconn.cursor()

def wMetric(dwconn, dwcursor, metric):

        sql="update if_objectevent set if_filesourcecreationtime = %s, if_filereceiptcompletiontime = %s, if_filecompletionstatus = %s where if_filename = %s"

        logger.debug("sql={0}".format(sql))
        logger.debug("metric={0}".format(metric))

        dwcursor.execute(sql, (metric['if_filesourcecreationtime'], metric['if_filereceiptcompletiontime'], metric['if_filecompletionstatus'], metric['if_filename']))
        dwconn.commit()

# check if we have a file
def checkFile(fileDate):
        fileYear = datetime.strftime(fileDate, '%Y')
	fileMonth = datetime.strftime(fileDate, '%m')
	fileDay = datetime.strftime(fileDate, '%d')
	fileName = "NISE_SSMISF18_{0}{1}{2}.HDFEOS".format(fileYear, fileMonth, fileDay)
	url = "https://n5eil01u.ecs.nsidc.org/DP4/OTHR/NISE.005/{0}.{1}.{2}/{3}".format(fileYear, fileMonth, fileDay, fileName)
        nameTuple = (fileName,)
	if nameTuple in nise_files:
		logger.info("{0} already have".format(nameTuple))
                return
        else:
		fo.write("{0}\n".format(url))
		return fileName

while True:

        logger.info("Start of polling cycle")
        
	# get the lists of files we already have

	sql = "select filename from filemetadata where productId = 3492"
	ndecursor.execute(sql)
	nise_files = ndecursor.fetchall()
	logger.debug("NICE files we already have: {0}".format(nise_files))

	inputFile = "input.txt"
	fo = open(inputFile, "w")
	fileList = []

	date0 = datetime.now()
        result = checkFile(date0)
	if result is not None:
		fileList.append(result)

	date1 = datetime.now() - timedelta(1)
        result = checkFile(date1)
	if result is not None:
		fileList.append(result)
 	
	date2 = datetime.now() - timedelta(2)
        result = checkFile(date2)
	if result is not None:
		fileList.append(result)

	fo.flush()
        fo.close()

	try:
		if fileList:
			result = Popen(["/bin/bash", "download.sh"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			result.communicate()

			for fileName in fileList:
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
                logger.error("Caught exception: {0}".format(e.output))

        logger.info("Going to sleep")
        time.sleep(3600)
