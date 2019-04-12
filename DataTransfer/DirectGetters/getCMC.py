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

logFileName = '/home/ec2-user/getCMC/log/getCMC.log'
logger = logging.getLogger()
handler = RotatingFileHandler(logFileName, maxBytes=100000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

dwconn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
dwconn = psycopg2.connect(dwconn_string)
dwcursor = dwconn.cursor()

pattern = ".*?-CMC-L4_GHRSST-SSTfnd-CMC0.1deg-GLOB-v02.0-fv03.0.nc$"

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
def downloadFile(dir, fileName):
	logger.info("Downloading {0}".format(fileName))

        mgetFileName = "/home/ec2-user/getCMC/work/cmcMget.txt"
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
	fo.write("open {0}\n".format(dir)) 
        fo.write("mget {0}*\n".format(fileName))
        fo.write("quit" + "\n")
        fo.flush()
        fo.close()

	try:
		res = subprocess.check_output(['lftp', '-f', mgetFileName], stderr=subprocess.STDOUT)
		logger.info("lftp completed for {0}".format(fileName))
		
		s3.upload_file(fileName, bucket_name, 'i/' + fileName)
                logger.info("copied {0} to S3 bucket".format(fileName))
		
		fileCrTimeSourceStr = time.ctime(os.path.getmtime(fileName))
		os.remove(fileName)

                sqsKey = modFileName + ", NA"
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
		  
        except subprocess.CalledProcessError as e:
                logger.error("Caught lftp exception: {0}".format(e.output))

# check directory for files we do not have
def checkDirectory(directory):

	listFileName = "/home/ec2-user/getCMC/work/cmcListing.txt"
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
		for cmcList in list:
			if cmcList == '':
				continue
			fields = cmcList.split()
                	fieldLen = len(fields)
                	if fieldLen < 9:
				continue
                	cmcFileName = fields[fieldLen - 1]
                        nameTuple = (cmcFileName,)
                	if re.match(pattern, cmcFileName):
                        	if nameTuple in files_cmc:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, cmcFileName)

	except subprocess.CalledProcessError as e:
		logger.error("Caught lftp exception: {0}".format(e.output))

while True:

        logger.info("Start of polling cycle")
        
	# get the lists of files we already have

	sql = "select filename from filemetadata where productId = 3491"
	ndecursor.execute(sql)
	files_cmc = ndecursor.fetchall()
	logger.debug("CMC files we already have: {0}".format(files_cmc))

	day2 = datetime.strftime(datetime.now() - timedelta(2), '%j')
	year2 = datetime.strftime(datetime.now() - timedelta(2), '%Y')
	dir2 = "ftp://podaac-ftp.jpl.nasa.gov/allData/ghrsst/data/GDS2/L4/GLOB/CMC/CMC0.1deg/v3/{0}/{1}".format(year2, day2)
	checkDirectory(dir2)

	day1 = datetime.strftime(datetime.now() - timedelta(1), '%j')
	year1 = datetime.strftime(datetime.now() - timedelta(1), '%Y')
	dir1 = "ftp://podaac-ftp.jpl.nasa.gov/allData/ghrsst/data/GDS2/L4/GLOB/CMC/CMC0.1deg/v3/{0}/{1}".format(year1, day1)
	checkDirectory(dir1)

        logger.info("Going to sleep")
        time.sleep(3600)


