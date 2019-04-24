from datetime import datetime
from dateutil import tz
import psycopg2
import os
import sys
import json

#Original Author: Peter G. MacHarrie

# Redirect sysout/syserr to log file
myPID = str(os.getpid())

installedDirectory = '/home/ec2-user/ndeMetrics/'
cfgFileName = installedDirectory + 'ingmetrics.cfg'
cfgf = open (cfgFileName, 'r+')

startRange = cfgf.read()

logFileName = installedDirectory + 'dtMetrics.log'
logf = open (logFileName, "a")
sys.stdout = sys.stderr = logf

conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()


to_zone = tz.tzlocal()
utc  = datetime.now()
utc=utc.replace(tzinfo=tz.tzutc())
currentDT = utc.astimezone(to_zone)

endRange=currentDT.strftime("%Y-%m-%d %H:%M")+":00"
#print startRange, endRange

sql = """
select fileEndTime, fileInsertTime, fileName, fileSize, platformName, productShortName, productType, productSubType
from fileMetadata f, productDescription p, productPlatform_xref x, Platform m
where f.productId = p.productId
 and p.productId = x.productId
 and x.platformId = m.platformId
 and fileInsertTime >= %s and fileInsertTime < %s
order by fileInsertTime
"""


#print sql

cursor.execute(sql, (startRange,endRange) )
conn.commit()
res=cursor.fetchall()

for row in res:
	#print row
	ndeMetrics = {
		"MetricName" : "ingest",
		"fileEndTime"    : row[0].strftime("%Y-%m-%d %H:%M:%S"),
		"fileInsertTime" : row[1].strftime("%Y-%m-%d %H:%M:%S,%f"),
		"fileName" : row [2],
		"fileSize" : row[3],
		"platformName" : row[4],
		"productShortName" : row[5],
		"productType" : row[6],
		"productSubType" : row[7],
		"Count" : 1
	}

	print row[1].strftime("%Y-%m-%d %H:%M:%S"), json.dumps(ndeMetrics, separators=(',',':'))

# Update cfg

cfgf.seek(0)
cfgf.write(endRange)
cfgf.truncate()
cfgf.close()
