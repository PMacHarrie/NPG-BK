"""
	Run job python equivalent of action pipeline
"""

# Passed JSON Structure of Job Object (Caller handles DB)
# Create Working Directory
# Create PCF
# Create PSF
# Copy Inputs to the Working Directory
# Invoke Process
# Handle Return Codes and Log messages
# Validate PSF listed outputs
# Copy Outputs to Ingest
# Return Output JSON to caller (Caller handles DB)


# Logging handled by nodeMgr. Log to stdout

# Dec. 4, 2018, pgm, modified for inputSpec[fileids] = "*" to allow algorithms (MATCHUP_LSR.py) to handle inputs


import datetime
import sys
import os
import shutil
import psycopg2
import boto3
from botocore.exceptions import ClientError
import json
from subprocess import Popen, STDOUT, PIPE
import re
import time
import socket
import EDMClient as edm

myPID = str(os.getpid())
hostname = socket.gethostname()

logdir = os.environ['im'] + '/logs/pgs/'

childOfMineLog =    logdir + 'childOf.' + myPID + '.log'
childOfMineLogErr = logdir + 'childOf.' + myPID + '.logerr'

logf = open (childOfMineLog, 'w')
loge = open (childOfMineLogErr, 'w')

# Resources

mode = "NDE_DEV1"
workingStorage = "/opt/data/nde/" + mode + "/pgs/working"

algorithmBaseDir = "/opt/apps/nde/" + mode + "/algorithms"

conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
dbconn = psycopg2.connect(conn_string)



# Functions

def logit(x):
	log_dt=datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
	print log_dt + " " + hostname + " " + myPID + " " + x
	return None


def createPCF(jobObject, algoObject):

	#print "jobObject=", jobObject

	f=open(algoObject['pcf_filename'], 'w')
	#print "pcfName=", algoObject['pcf_filename']

	# Write static PCF data

	f.write("#\n")
	f.write("# onJobId:" + str(jobObject['jobID']) + "\n")
	f.write("# working directory:\n")
	f.write("# ProdRuleName: null\n")
	f.write("#\n")
	f.write("working_directory=" + jobObject['workingDirectory'] + "\n")
	f.write("nde_mode=" + os.environ['m'] + "\n")
	f.write("job_coverage_start=" + jobObject['obsStartTime'] + "\n")
	f.write("job_coverage_end=" +    jobObject['obsEndTime'] + "\n")


	for x in jobObject['jobSpec']['parameters']:
		print x, jobObject['jobSpec']['parameters'][x]
		f.write(x + '=' + jobObject['jobSpec']['parameters'][x] + '\n')

	for x in jobObject['pcf']:  # If an algorithm that handles its own inputs, null is okay.
		#print "x=", x
		for key, value in x.items():
			#print key,value
			f.write(key + "=" + value + "\n")
		

	f.close()

# Create empty PSF 

	f=open(algoObject['psf_filename'], 'w')
	f.close()

	return


def persistJobStatus(jobObject):

	#print jobObject

	sql = """update onDemandJob 
		set 
		odjobStartTime = %s,
		odjobCompletionTime = %s,
		odJobStatus = %s,
		odalgorithmReturnCode = %s,
		odjobHighestErrorClass = %s,
		odjobPID = %s,
		odjobCPU_Util = %s,
		odjobOutput = %s
		where odJobId = %s
"""

	#print "sql=", sql

	row=cursor.execute(sql, ( 
		jobObject["jobStartTime"],
		jobObject["jobCompletionTime"],
		jobObject["jobStatus"],
		jobObject["executableRC"],
		jobObject["jobHighestErrorClass"],
		jobObject["jobPID"],
		jobObject["jobCPU_Util"],
		json.dumps(jobObject["jobOutput"]),
		jobObject["jobID"]
		))

	dbconn.commit()




logit("Started runJob")

# Get Job Type and  ID:

if len(sys.argv) != 3:
	logit( "Invalid arguments")
	exit(1)

if not os.path.exists(workingStorage):
	os.makedirs(workingStorage)

jobType = sys.argv[1]
jobID   = sys.argv[2]

if not(jobType == 'onDemand' or jobType == 'production'):
	logit("Invalid job type")
	exit(2)

if jobType == 'production':
	print "not yet"
	exit(3)


#print "jobType=", jobType
#print "jobID=", jobID

# Making sure a cancellation request has been sent before the job in executed.

# Get Job Info


sql = "select odJobStatus, substr(to_char(to_timestamp(odjobSpec->>'obsStartTime', 'YYYY-MM-DD HH24:MI:SS'), 'YYYYMMDDHH24MISSMS'), 1, 15), substr(to_char(to_timestamp(odjobSpec->>'obsEndTime', 'YYYY-MM-DD HH24:MI:SS'), 'YYYYMMDDHH24MISSMS'), 1, 15), odJobSpec from onDemandJob where odJobId = %s"

cursor = dbconn.cursor()

cursor.execute(sql, (jobID,))
dbconn.commit()

row = cursor.fetchone()

jobObject = {
	"jobID" : jobID,
	"jobStatus" : row[0],
	"obsStartTime" : row[1],
	"obsEndTime" : row[2],
	"jobSpec" : row[3],
	"jobOutput" : {},
	"jobStartTime" : time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
	"jobCompletionTime" : None,
	"executableRC" : None,
	"jobHighestErrorClass" : None,
	"jobPID" : os.getpid(),
	"jobCPU_Util" : None
}


sql = "select * from algorithm where algorithmName = %s and algorithmVersion = %s"
row=cursor.execute(sql, ( 
	jobObject["jobSpec"]["algorithm"]["name"],
	jobObject["jobSpec"]["algorithm"]["version"] 
	))

row = cursor.fetchone()
dbconn.commit()

algoObject = {
	"id" : row[0],
	"name" : row[1],
	"version" : row[2],
	"notifyopsseconds" : row[3],
	"type" : row[4],
	"pcf_filename" : row[5],
	"psf_filename" : row[6],
	"logfilename"  : row[7],
	"commandprefix" : row[8],
	"executablelocation" : row[9],
	"logmessagecontext" : row[10],
	"logmessagewarn" : row[11],
	"logmessageerror" : row[12],
	"executablename" : row[13],
	"parameters" : [],
	"inputs" : [],
	"outputs" : [] 
}

sql = "select * from algoParameters where algorithmId = %s"
row=cursor.execute(sql, ( algoObject['id'],))

rows = cursor.fetchall()
dbconn.commit()

for row in rows:
#	print row
	algoObject['parameters'].append(row[2])

sql = "select * from algoInputProd where algorithmId = %s"
row=cursor.execute(sql, ( algoObject['id'],))

rows = cursor.fetchall()
dbconn.commit()

for row in rows:
#	print row
	algoObject['inputs'].append(row[0])


sql = "select * from algoOutputProd where algorithmId = %s"
row=cursor.execute(sql, ( algoObject['id'],))

rows = cursor.fetchall()
dbconn.commit()

for row in rows:
#	print row
	algoObject['outputs'].append(row[0])


#print "jo=", json.dumps(jobObject, indent=1)
#print "ao=", json.dumps(algoObject, indent=1)

# Validate onDemand Job Spec options against the algorithm definition.
# Example onDemand Job Spec (oDJS)
#{
#        "algorithm" : {
#                "name" : "dss ad-hoc",
#                "version" : "1.0"
#        },
#        "inputs" : {
#                "VIIRS-I4-SDR" : [34862634,34874969,34873514],
#                "VIIRS-IMG-GEO-TC" : [34864710,34865199,34865021]
#        },
#        "outputs" : {
#                "fileNamePrefix" : "myProduct"
#        },
#        "parameters" : {
#                "GeogCitationGeoKey" : "a",
#                "dataType" : "huh?",
#                "file_file" : "binary"
#        },
#        "jobSpec" : {
#                "obsStartTime" : "2018-09-25T15:05:24",
#                "obsEndTime"   : "2018-09-25T15:09:39"
#        }
#}

# Parameters Valid?

validoDJS = True
invalidoDJSReason=""

for inputSpec in jobObject['jobSpec']['inputs']:
	#print "is=", inputSpec
	for fileId in inputSpec['fileIds']:
#	for fileId in jobObject['jobSpec']['inputs'][inputSpec]:
		if str(inputSpec['fileIds'][0]) == "*": # algorithm will handle inputs
			pass
		else:
		#print "id=", fileId
			sql = "select count(*) from fileMetadata f, algoInputProd a where fileId = %s and f.productId = a.productId and a.algorithmId = %s"
			cursor.execute(sql, (fileId, algoObject['id']))
			row = cursor.fetchone()
			if row == 0:
				validoDJS=False
				invalidoDJSReason="Invalid Input File. inputSpec =" + inputSpec + "fileId="+ fileId

for parameterName in jobObject['jobSpec']['parameters']:
	#print "parameterName=", parameterName
	if not(parameterName in algoObject['parameters']):
		validoDJS=False
		invalidoDJSReason="Invalid Parameter Name: " + parameterName


if not(validoDJS):
	logit( "Error in onDemand job Spec. " + invalidoDJSReason + " exiting.....")
	exit(-1)

logit("Valid job Spec, Executing Job.")

jobObject['jobStatus']='RUNNING'
persistJobStatus(jobObject)

#Validate Executable

algoExec=algorithmBaseDir + '/' + algoObject['executablelocation'] + '/' + algoObject['executablename']
if not os.path.exists(algoExec):
	logit( "Algorithm " + algoExec + " not found.")
	exit(-1)


# Create Working Directory

workingDirectory = workingStorage + '/' + str(jobID)

if not os.path.exists(workingDirectory):
	os.makedirs(workingDirectory)
else:
	shutil.rmtree(workingDirectory)
	os.makedirs(workingDirectory)

jobObject['workingDirectory']=workingDirectory

# Copy inputs to working directory

os.chdir(workingDirectory)


#print "wd=", workingDirectory



# Get Inputs

jobObject['jobSpec']['fileName']=[]
jobObject['pcf'] = []
for inputSpec in jobObject['jobSpec']['inputs']:
	if str(inputSpec['fileIds'][0]) == "*": # algorithm will handle inputs
		pass
	else:
	#print "is=", inputSpec
		fileHandle=inputSpec['prisFileHandle']
		for fileId in inputSpec['fileIds']:
			logit( "Getting fileId: " + str(fileId))
			fileName=edm.file_get(fileId)
			jobObject['pcf'].append({ fileHandle : fileName } )
			logit( "Got fileId: " + str(fileId) + " " + fileName)


# Create PCF and PSF

createPCF(jobObject, algoObject)



# Invoke "Algorithm"

ndeCodeStorage = os.environ['dm'] + "/algorithms/"

#executableName = algoObject['commandprefix'] + ndeCodeStorage + algoObject['executablelocation'] + '/' + 'dss_badrc.pl'
executableName = algoObject['commandprefix'] + " " + ndeCodeStorage + algoObject['executablelocation'] + '/' + algoObject['executablename']

runThis = executableName.strip(' ')
logit(">>>>" + runThis + "<<<<")

#executableRC=""
#try:
#	algoSTDOUT = sp.check_output([runThis], stderr=sp.STDOUT)
#	executableRC = 0
#except sp.CalledProcessError as e:
#	executableRC = e.returncode
#	print "Exception raised " + str(e.returncode) + "<<<<"
#	print "Exception output " + str(e.output) + "<<<<"

#jobObject['executableRC']=executableRC


myJob = [ '/usr/bin/time']

myArgs = runThis.split(' ')
for x in myArgs:
	myJob.append(x)

print "myJob=", myJob

p = Popen (myJob, stderr=loge, stdout=logf)

jobLoop=1
ctr = 0
jobPid = p.pid

logit("executing algorithm pid = " + str(jobPid))


while (jobLoop):
	ctr += 1
	logit("pid = " + str(jobPid) + " " + str(ctr))
	time.sleep(2)
	x=p.poll()
	if x == None:
		continue
	else:
		jobLoop=0

logit( "algorithm return code =" + str(p.returncode) )


jobObject['logfilename']=workingDirectory + '/' + algoObject['logfilename']

logMessages = []
highestErrorClass = None

if os.path.isfile(jobObject['logfilename']):
	f=open(jobObject['logfilename'], 'r')
	for line in f:
		if re.match(algoObject['logmessagecontext'], line):
			if re.match(algoObject['logmessagewarn'], line):
				highestErrorClass == "WARN"
				print line
				logMessages.append(line)
			if re.match(algoObject['logmessageerror'], line):
				print line
				highestErrorClass == "ERROR"
				logMessages.append(line)
	

if p.returncode <> 0:
	jobObject['jobStatus']='FAILED'
else:
	jobObject['jobStatus']='COMPLETE'
	
jobObject['psf']=workingDirectory + '/' + algoObject['psf_filename']

certs = []
if os.path.isfile(jobObject['psf']):
	f=open(jobObject['psf'], 'r')
	for line in f:
		line=line.rstrip()
		if not(line == '#END-of-PSF'):
			logit("output " +  line)
			if jobType == "onDemand":
		                s3 = boto3.resource('s3')
				fileName = workingDirectory + '/' + line
				key = "outgoing/" + str(jobObject['jobID']) + '/' + line
				logit( "key="+ key + "<<<<" )
				try:
					s3.meta.client.upload_file(fileName, "ndepg", key)
				except ClientError as e:
					logit( "s3 error= " + e)
				else:
					s3c = boto3.client('s3')
					url = s3c.generate_presigned_url( ClientMethod='get_object', Params={ 'Bucket': 'ndepg', 'Key': key }, ExpiresIn=345600)

				certs.append(url)
					
				logit(line + " " + "copied to outgoing. cert=" + " " + url)
			else:
				print "do edm ingest"

#shutil.rmtree(workingDirectory)

jobObject["jobCompletionTime"]=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
jobObject["jobOutput"]["s3_signedCerts"] = certs
persistJobStatus(jobObject)

logf.close()
loge.close()

#os.remove( childOfMineLog )
#os.remove( childOfMineLogErr )
	
logit("job " + str(jobID) + " done status = " + jobObject['jobStatus'])

exit(0)

