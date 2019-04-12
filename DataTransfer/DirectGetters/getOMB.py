import re
import os
import sys
import boto3
import subprocess
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

logFileName = '/home/ec2-user/getOMB/log/getOMB.log'
logger = logging.getLogger()
handler = RotatingFileHandler(logFileName, maxBytes=100000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

dwconn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
dwconn = psycopg2.connect(dwconn_string)
dwcursor = dwconn.cursor()

pattern = "^seaice\.t\d\dz\.5min\.grb\.grib2$"

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
def downloadFile(srcDir, srcFileName, modFileName):
        logger.info("Downloading {0} to {1}".format(srcFileName, modFileName))

        mgetFileName = "/home/ec2-user/getOMB/work/ombMget.txt"
        fo = open(mgetFileName, "w")

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
	fo.write("open {0}\n".format(srcDir)) 
        fo.write("mget {0}*\n".format(srcFileName))
        fo.write("quit" + "\n")
        fo.flush()
        fo.close()

	try:
		res = subprocess.check_output(['lftp', '-f', mgetFileName], stderr=subprocess.STDOUT)
		logger.info("lftp completed for {0}".format(srcFileName))
		
		s3.upload_file(srcFileName, bucket_name, 'i/' + modFileName)
                logger.info("copied {0} to S3 bucket".format(modFileName))
		
		fileCrTimeSourceStr = time.ctime(os.path.getmtime(srcFileName))
		os.remove(srcFileName)

                sqsKey = modFileName + ", NA"
                response = sns.publish(
                        TopicArn=topicARN,
                        Message = json.dumps({ "filename" : modFileName }),
                        Subject = "Ingest"
                )
                logger.info("{0} message sent to NewFile topic".format(modFileName))

		completedTimeStr = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
                ifObjEvent = {
                        'if_filesourcecreationtime' : fileCrTimeSourceStr,
                        'if_filereceiptcompletiontime' : completedTimeStr,
                        'if_filename' : modFileName,
                        'if_filecompletionstatus' : 'PullDone'
                }
                wMetric(dwconn, dwcursor, ifObjEvent)
		  
        except subprocess.CalledProcessError as e:
                logger.error("Caught lftp exception: {0}".format(e.output))

# check directory for files we do not have
def checkDirectory(directory, dateStr):

	listFileName = "/home/ec2-user/getOMB/work/ombListing.txt"
	fo = open(listFileName, "w")

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
	fo.write("open {0}\n".format(directory)) 
	fo.write("ls\n")
	fo.write("quit" + "\n")
	fo.flush()
	fo.close()

	try:
		logger.info("Polling {0}".format(directory))
	        res = subprocess.check_output(['lftp', '-f', listFileName], stderr=subprocess.STDOUT)
                logger.info("lftp completed for {0}".format(directory))
		list = res.split('\n')
		for ombList in list:
			if ombList == '':
				continue
			fields = ombList.split()
                	fieldLen = len(fields)
                	if fieldLen < 9:
				continue
                	origFileName = fields[fieldLen - 1]
                        modFileName = origFileName + "." + dateStr
			nameTuple = (modFileName,)
                	if re.match(pattern, origFileName):
                        	if nameTuple in files_omb:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)

	except subprocess.CalledProcessError as e:
		logger.error("Caught lftp exception: {0}".format(e.output))

while True:

	# get the lists of files we already have

	sql = "select filename from filemetadata where productId = 3498"
	ndecursor.execute(sql)
	files_omb = ndecursor.fetchall()
	logger.debug("0p50_f000 files we already have: {0}".format(files_omb))

	today = datetime.now().strftime('%Y%m%d')
	yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')

	tDir = "ftp.ncep.noaa.gov:/pub/data/nccf/com/omb/prod/sice.{0}".format(today)
	yDir = "ftp.ncep.noaa.gov:/pub/data/nccf/com/omb/prod/sice.{0}".format(yesterday)

	checkDirectory(tDir, today)
	checkDirectory(yDir, yesterday)

        logger.info("Going to sleep")
        time.sleep(3600)

