from datetime import datetime
from dateutil import tz
import psycopg2
import os
import sys
import json


# Original Author: Peter MacHarrie

# Removed output bytes (replaced with 0, not worth db query workload)

# Redirect sysout/syserr to log file
myPID = str(os.getpid())

installedDirectory = '/home/ec2-user/ndeMetrics/'
cfgFileName = installedDirectory + 'pg_metrics.cfg'
cfgf = open (cfgFileName, 'r+')

#print "debug" 

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
select  
	prJobCompletionTime,
	algoSuite,
	prJobCPU_Util,
	pgObsLatencyS,
	prJobIO_Util / 2,
	prJobIO_Util / 2,
	prJobMem_Util
from (
select 
	prJobCompletionTime, substr(algorithmName, 1, 4) algoSuite, prJobCPU_Util, extract( epoch from prJobCompletionTime - pjsObsEndTime) pgObsLatencyS, prJobId, prJobIO_Util, prJobMem_Util
from productionJob j, productionJobSpec s, productionRule r, algorithm a
where j.prodPartialJobId = s.prodPartialJobId
 and s.prId = r.prId
 and r.algorithmId = a.algorithmId
 and prJobCompletionTime >= %s and prJobCompletionTime < %s
 and prJobStatus = 'COMPLETE'
 and prJobCPU_Util is not null
 and prJobIO_Util is not null
 and prJobMem_Util is not null
order by prJobId
) t1
order by
	t1.prJobCompletionTime
"""


#print sql

cursor.execute(sql, (startRange,endRange) )
conn.commit()
res=cursor.fetchall()

for row in res:
#	print row
	cpuFloat = 0.0
	if row[2]==None:
		pass
	else:
		cpuFloat=row[2]

	ndeMetrics = {
		"MetricName"     : "pg",
		"prJobCompletionTime" : row[0].strftime("%Y-%m-%d %H:%M:%S"),
		"algorithmSuite" : row[1],
		"prJobCPU_Util"  : "{0:0.3f}".format(cpuFloat), 
		"pgObsLatencyS"  : row[3],
		"jobInBytes"     : str(row[4]),
		"jobOutBytes"    : str(row[5]),
		"jobMemoryKB"      : "{0:0.3f}".format(float(float(row[6])/1024.0)),
		"Count" : 1
	}
	print "ndeMetrics=", ndeMetrics
	print row[0].strftime("%Y-%m-%d %H:%M:%S"), json.dumps(ndeMetrics, separators=(',',':'))

# Update cfg

cfgf.seek(0)
cfgf.write(endRange)
cfgf.truncate()
cfgf.close()
