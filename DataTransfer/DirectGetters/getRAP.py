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

logFileName = '/home/ec2-user/getRAP/log/getRAP.log'
logger = logging.getLogger()
handler = RotatingFileHandler(logFileName, maxBytes=100000, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

dwconn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
dwconn = psycopg2.connect(dwconn_string)
dwcursor = dwconn.cursor()

pattern_alaska_00hr = "^rap\..*?awp242f00\.grib2$"
pattern_alaska_01hr = "^rap\..*?awp242f01\.grib2$"
pattern_alaska_02hr = "^rap\..*?awp242f02\.grib2$"
pattern_alaska_03hr = "^rap\..*?awp242f03\.grib2$"
pattern_alaska_04hr = "^rap\..*?awp242f04\.grib2$"
pattern_alaska_05hr = "^rap\..*?awp242f05\.grib2$"
pattern_alaska_06hr = "^rap\..*?awp242f06\.grib2$"

pattern_conus_00hr = "^rap\..*?awp130pgrbf00\.grib2$"
pattern_conus_01hr = "^rap\..*?awp130pgrbf01\.grib2$"
pattern_conus_02hr = "^rap\..*?awp130pgrbf02\.grib2$"
pattern_conus_03hr = "^rap\..*?awp130pgrbf03\.grib2$"
pattern_conus_04hr = "^rap\..*?awp130pgrbf04\.grib2$"
pattern_conus_05hr = "^rap\..*?awp130pgrbf05\.grib2$"
pattern_conus_06hr = "^rap\..*?awp130pgrbf06\.grib2$"

pattern_noamhi_00hr = "^rap\..*?awip32f00\.grib2$"
pattern_noamhi_01hr = "^rap\..*?awip32f01\.grib2$"
pattern_noamhi_02hr = "^rap\..*?awip32f02\.grib2$"
pattern_noamhi_03hr = "^rap\..*?awip32f03\.grib2$"
pattern_noamhi_04hr = "^rap\..*?awip32f04\.grib2$"
pattern_noamhi_05hr = "^rap\..*?awip32f05\.grib2$"
pattern_noamhi_06hr = "^rap\..*?awip32f06\.grib2$"

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

        mgetFileName = "/home/ec2-user/getRAP/work/rapMget.txt"
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

	listFileName = "/home/ec2-user/getRAP/work/rapListing.txt"
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
		for rapList in list:
			if rapList == '':
				continue
			fields = rapList.split()
                	fieldLen = len(fields)
                	if fieldLen < 5:
				continue
                	origFileName = fields[fieldLen - 1]
			modFileName = origFileName + "." + dateStr
                        nameTuple = (modFileName,)

                	if re.match(pattern_alaska_02hr, origFileName):
                        	if nameTuple in files_alaska_02hr:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
                	elif re.match(pattern_alaska_03hr, origFileName):
                        	if nameTuple in files_alaska_03hr:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
                	elif re.match(pattern_conus_02hr, origFileName):
                        	if nameTuple in files_conus_02hr:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
                	elif re.match(pattern_conus_03hr, origFileName):
                        	if nameTuple in files_conus_03hr:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
                	elif re.match(pattern_noamhi_02hr, origFileName):
                        	if nameTuple in files_noamhi_02hr:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)
                	elif re.match(pattern_noamhi_03hr, origFileName):
                        	if nameTuple in files_noamhi_03hr:
                                        logger.info("{0} already have".format(nameTuple))
                                else:
					downloadFile(directory, origFileName, modFileName)

	except subprocess.CalledProcessError as e:
		logger.error("Caught lftp exception: {0}".format(e.output))

while True:

        logger.info("Start of polling cycle")
        # get the lists of files we already have

	# get the lists of files we already have - currently only getting 2 and 3 hour files but may change in future

	sql = "select filename from filemetadata where productId = 4854"
	ndecursor.execute(sql)
	files_alaska_02hr = ndecursor.fetchall()
	logger.debug("alaska_02hr files we already have: {0}".format(files_alaska_02hr))

	sql = "select filename from filemetadata where productId = 4840"
	ndecursor.execute(sql)
	files_alaska_03hr = ndecursor.fetchall()
	logger.debug("alaska_03hr files we already have: {0}".format(files_alaska_03hr))

	sql = "select filename from filemetadata where productId = 4853"
	ndecursor.execute(sql)
	files_conus_02hr = ndecursor.fetchall()
	logger.debug("conus_02hr files we already have: {0}".format(files_conus_02hr))

	sql = "select filename from filemetadata where productId = 4843"
	ndecursor.execute(sql)
	files_conus_03hr = ndecursor.fetchall()
	logger.debug("conus_03hr files we already have: {0}".format(files_conus_03hr))

	sql = "select filename from filemetadata where productId = 4848"
	ndecursor.execute(sql)
	files_noamhi_02hr = ndecursor.fetchall()
	logger.debug("noamhi_02hr files we already have: {0}".format(files_noamhi_02hr))

	sql = "select filename from filemetadata where productId = 4849"
	ndecursor.execute(sql)
	files_noamhi_03hr = ndecursor.fetchall()
	logger.debug("noamhi_03hr files we already have: {0}".format(files_noamhi_03hr))

	today = datetime.now().strftime('%Y%m%d')
	yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')

	today_directory = "http://www.ftp.ncep.noaa.gov/data/nccf/com/rap/prod/rap.{0}/".format(today)
	yesterday_directory = "http://www.ftp.ncep.noaa.gov/data/nccf/com/rap/prod/rap.{0}/".format(yesterday)

	checkDirectory(today_directory, today)
	checkDirectory(yesterday_directory, yesterday)

        logger.info("Going to sleep")
        time.sleep(3600)

