from datetime import datetime
from dateutil import tz
import psycopg2
import os
import sys
import json
# Original Author: Peter MacHarrie

# Redirect sysout/syserr to log file
myPID = str(os.getpid())

installedDir = '/home/ec2-user/ndeMetrics/'

cfgFileName = installedDir + 'metrics.cfg'
cfgf = open (cfgFileName, 'r+')

startRange = cfgf.read()

logFileName = installedDir + 'dtMetrics.log'
logf = open (logFileName, "a")
sys.stdout = sys.stderr = logf

conn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()


to_zone = tz.tzlocal()
utc  = datetime.now()
utc=utc.replace(tzinfo=tz.tzutc())
currentDT = utc.astimezone(to_zone)

endRange=currentDT.strftime("%Y-%m-%d %H:%M")+":00"
#print ">>>" + startRange + "||||" +  endRange + "<<<<"

sql = """
select 
        product,
	ObservationEndTime,
	IDPS_CrTime,
	if_fileSourceCreationTime,
	if_fileDetectedTime,
	if_fileReceiptCompletionTime,
        extract(epoch from IDPS_CrTime - ObservationEndTime) IDPS_LatencyS,
        extract(epoch from if_fileSourceCreationTime - IDPS_CrTime) ESPDS_LatencyS,
        extract(epoch from if_fileDetectedTime - if_fileSourceCreationTime) Detect_LatencyS,
        extract(epoch from if_fileReceiptCompletionTime - if_fileDetectedTime) Pull_LatencyS,
	if_fileName

from
(select
        split_part(if_filename, '_', 1) || '_' || split_part(if_filename, '_', 2) product,
        to_timestamp( substr(split_part(if_filename, '_', 3),2,8) || substr(split_part(if_filename, '_', 5),2,6) , 'YYYYMMDDHH24MISS') ObservationEndTime,
        to_timestamp(substr(split_part(if_filename, '_', 7),2,14), 'YYYYMMDDHH24MISS') IDPS_CrTime,
        if_fileSourceCreationTime,
        if_fileDetectedTime,
        if_fileReceiptCompletionTime,
	if_fileName
from if_objectEvent
where
        if_fileName like '%%h5'
	and if_fileName not like 'OMPS-NPP_LP-DRK%%'
	and if_fileName not like 'RAM@M_gw1%%'
        and if_fileReceiptCompletionTime >=  %s and if_fileReceiptCompletionTime < %s
order by
        if_fileReceiptCompletionTime
) t
"""


#print sql

cursor.execute(sql, (startRange,endRange) )
conn.commit()
res=cursor.fetchall()

for row in res:
	#print row
	dT_Metrics = {
		"MetricName" : "DT_Latency",
		"Product" : row[0],
		"IDPS_Lat_S" : row[6],
		"ESPDS_Lat_S" : row [7],
		"Detect_Lat_S" : row[8],
		"Pull_Lat_S" : row[9],
		"if_fileName" : row[10],
		"Count" : 1
	}
	
	print row[5].strftime("%Y-%m-%d %H:%M:%S"), json.dumps(dT_Metrics, separators=(',',':'))

# Update cfg
cfgf.seek(0)
cfgf.write(endRange)
cfgf.truncate()
cfgf.close()
