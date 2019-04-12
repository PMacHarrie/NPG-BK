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

logFileName = '/home/ec2-user/getGFS/log/getGFS.log'
logger = logging.getLogger()
handler = RotatingFileHandler(logFileName, maxBytes=100000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

dwconn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
dwconn = psycopg2.connect(dwconn_string)
dwcursor = dwconn.cursor()

pattern_0p50_f000 = "^gfs\..*?\.pgrb2\.0p50\.f000$"
pattern_0p50_f003 = "^gfs\..*?\.pgrb2\.0p50\.f003$"
pattern_0p50_f006 = "^gfs\..*?\.pgrb2\.0p50\.f006$"
pattern_0p50_f009 = "^gfs\..*?\.pgrb2\.0p50\.f009$"
pattern_0p50_f012 = "^gfs\..*?\.pgrb2\.0p50\.f012$"
pattern_0p50_f015 = "^gfs\..*?\.pgrb2\.0p50\.f015$"

pattern_1p00_f000 = "^gfs\..*?\.pgrb2\.1p00\.f000$"
pattern_1p00_f003 = "^gfs\..*?\.pgrb2\.1p00\.f003$"
pattern_1p00_f006 = "^gfs\..*?\.pgrb2\.1p00\.f006$"
pattern_1p00_f009 = "^gfs\..*?\.pgrb2\.1p00\.f009$"
pattern_1p00_f012 = "^gfs\..*?\.pgrb2\.1p00\.f012$"
pattern_1p00_f015 = "^gfs\..*?\.pgrb2\.1p00\.f015$"

def wMetric(dwconn, dwcursor, metric):

        sql="update if_objectevent set if_filesourcecreationtime = %s, if_filereceiptcompletiontime = %s, if_filecompletionstatus = %s where if_filename = %s"

        logger.debug("sql={0}".format(sql))
        logger.debug("metric={0}".format(metric))

        dwcursor.execute(sql, (metric['if_filesourcecreationtime'], metric['if_filereceiptcompletiontime'], metric['if_filecompletionstatus'], metric['if_filename']))
        dwconn.commit()

ndeconn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
ndeconn = psycopg2.connect(ndeconn_string)
ndecursor = ndeconn.cursor()

# download the file, copy to S3, write to new message topic
def downloadFile(srcDir, srcFileName, modFileName):
	logger.info("Downloading {0} to {1}".format(srcFileName, modFileName))

        mgetFileName = "/home/ec2-user/getGFS/work/gfsMget.txt"
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
        fo.write("get {0}\n".format(srcFileName))
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

	listFileName = "/home/ec2-user/getGFS/work/gfsListing.txt"
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
	        res = subprocess.check_output(['lftp', '-f', listFileName], stderr=subprocess.STDOUT)
                logger.info("lftp completed for {0}".format(directory))
		list = res.split('\n')
		for gfsList in list:
			if gfsList == '':
				continue
			fields = gfsList.split()
                	fieldLen = len(fields)
                	if fieldLen < 5:
				continue
                	origFileName = fields[fieldLen - 1]
			modFileName = origFileName + "." + dateStr
                        nameTuple = (modFileName,)

                	if re.match(pattern_0p50_f000, origFileName):
                        	if nameTuple in files_0p50_f000:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
			elif re.match(pattern_0p50_f003, origFileName):
                        	if nameTuple in files_0p50_f003:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
			elif re.match(pattern_0p50_f006, origFileName):
                        	if nameTuple in files_0p50_f006:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
			elif re.match(pattern_0p50_f009, origFileName):
                        	if nameTuple in files_0p50_f009:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
			elif re.match(pattern_0p50_f012, origFileName):
                        	if nameTuple in files_0p50_f012:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
			elif re.match(pattern_0p50_f015, origFileName):
                        	if nameTuple in files_0p50_f015:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)

                	elif re.match(pattern_1p00_f000, origFileName):
                        	if nameTuple in files_1p00_f000:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
			elif re.match(pattern_1p00_f003, origFileName):
                        	if nameTuple in files_1p00_f003:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
			elif re.match(pattern_1p00_f006, origFileName):
                        	if nameTuple in files_1p00_f006:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
			elif re.match(pattern_1p00_f009, origFileName):
                        	if nameTuple in files_1p00_f009:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
			elif re.match(pattern_1p00_f012, origFileName):
                        	if nameTuple in files_1p00_f012:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
			elif re.match(pattern_1p00_f015, origFileName):
                        	if nameTuple in files_1p00_f015:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)

	except subprocess.CalledProcessError as e:
		logger.error("Caught lftp exception: {0}".format(e.output))

while True:

        logger.info("Start of polling cycle")
	# get the lists of files we already have

	sql = "select filename from filemetadata where productId = 3504"
	ndecursor.execute(sql)
	files_0p50_f000 = ndecursor.fetchall()
	logger.debug("0p50_f000 files we already have: {0}".format(files_0p50_f000))

	sql = "select filename from filemetadata where productId = 3506"
	ndecursor.execute(sql)
	files_0p50_f003 = ndecursor.fetchall()
	logger.debug("0p50_f003 files we already have: {0}".format(files_0p50_f003))

	sql = "select filename from filemetadata where productId = 3507"
	ndecursor.execute(sql)
	files_0p50_f006 = ndecursor.fetchall()
	logger.debug("0p50_f006 files we already have: {0}".format(files_0p50_f006))

	sql = "select filename from filemetadata where productId = 3501"
	ndecursor.execute(sql)
	files_0p50_f009 = ndecursor.fetchall()
	logger.debug("0p50_f009 files we already have: {0}".format(files_0p50_f009))

	sql = "select filename from filemetadata where productId = 3500"
	ndecursor.execute(sql)
	files_0p50_f012 = ndecursor.fetchall()
	logger.debug("0p50_f012 files we already have: {0}".format(files_0p50_f012))

	sql = "select filename from filemetadata where productId = 5146"
	ndecursor.execute(sql)
	files_0p50_f015 = ndecursor.fetchall()
	logger.debug("0p50_f015 files we already have: {0}".format(files_0p50_f015))


	sql = "select filename from filemetadata where productId = 3502"
	ndecursor.execute(sql)
	files_1p00_f000= ndecursor.fetchall()
	logger.debug("1p00_f000 files we already have: {0}".format(files_1p00_f000))

	sql = "select filename from filemetadata where productId = 3505"
	ndecursor.execute(sql)
	files_1p00_f003 = ndecursor.fetchall()
	logger.debug("1p00_f003 files we already have: {0}".format(files_1p00_f003))

	sql = "select filename from filemetadata where productId = 3503"
	ndecursor.execute(sql)
	files_1p00_f006 = ndecursor.fetchall()
	logger.debug("1p00_f006 files we already have: {0}".format(files_1p00_f006))

	sql = "select filename from filemetadata where productId = 3499"
	ndecursor.execute(sql)
	files_1p00_f009 = ndecursor.fetchall()
	logger.debug("1p00_f009 files we already have: {0}".format(files_1p00_f009))

	sql = "select filename from filemetadata where productId = 3508"
	ndecursor.execute(sql)
	files_1p00_f012 = ndecursor.fetchall()
	logger.debug("1p00_f012 files we already have: {0}".format(files_1p00_f012))

	sql = "select filename from filemetadata where productId = 5147"
	ndecursor.execute(sql)
	files_1p00_f015 = ndecursor.fetchall()
	logger.debug("1p00_f015 files we already have: {0}".format(files_1p00_f015))

	today = datetime.now().strftime('%Y%m%d')
	yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')

	today_directory_00 = "http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.{0}00/".format(today)
	today_directory_06 = "http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.{0}06/".format(today)
	today_directory_12 = "http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.{0}12/".format(today)
	today_directory_18 = "http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.{0}18/".format(today)
	yesterday_directory_00 = "http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.{0}00/".format(yesterday)
	yesterday_directory_06 = "http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.{0}06/".format(yesterday)
	yesterday_directory_12 = "http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.{0}12/".format(yesterday)
	yesterday_directory_18 = "http://www.ftp.ncep.noaa.gov/data/nccf/com/gfs/prod/gfs.{0}18/".format(yesterday)

	checkDirectory(today_directory_00, today)
	checkDirectory(today_directory_06, today)
	checkDirectory(today_directory_12, today)
	checkDirectory(today_directory_18, today)
	checkDirectory(yesterday_directory_00, yesterday)
	checkDirectory(yesterday_directory_06, yesterday)
	checkDirectory(yesterday_directory_12, yesterday)
	checkDirectory(yesterday_directory_18, yesterday)

	logger.info("Going to sleep")
	time.sleep(3600)
