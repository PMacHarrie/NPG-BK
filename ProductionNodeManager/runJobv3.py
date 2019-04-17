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

import datetime
import sys
import os
import shutil
import psycopg2
import boto3
from botocore.exceptions import ClientError
import json
from subprocess import Popen, STDOUT, PIPE
import subprocess
import re
import time
import socket
#import EDMClient as edm
from nderest import NdeRestClient

myPID = str(os.getpid())
hostname = socket.gethostname()

logdir = os.environ['im'] + '/logs/pgs/'

childOfMineLog =    logdir + 'childOf.' + myPID + '.log'
childOfMineLogErr = logdir + 'childOf.' + myPID + '.logerr'

logf = open (childOfMineLog, 'w')
loge = open (childOfMineLogErr, 'w')

nrc = NdeRestClient("https://vunlor9i2m.execute-api.us-east-1.amazonaws.com/nde-rest")

# Resources

mode = "NDE_DEV1"
workingStorage = "/opt/data/nde/" + mode + "/pgs/working"

algorithmBaseDir = "/opt/apps/nde/" + mode + "/algorithms"

conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
dbconn = psycopg2.connect(conn_string)

cursor = dbconn.cursor()


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
                #print x, jobObject['jobSpec']['parameters'][x]
                f.write(x + '=' + jobObject['jobSpec']['parameters'][x] + '\n')

        for x in jobObject['pcf']:
                #print "x=", x
                for key, value in x.items():
                        #print key,value
                        f.write(key + "=" + value + "\n")


        f.close()

# Create empty PSF

        f=open(algoObject['psf_filename'], 'w')
        f.close()

        return




def createProductionPCF(jobObject, algoObject):

# For now, production rule PCFs need to be created from jobObject.parameters as array object to maintain parameter order for the MiRS algorithm

        #print "jobObject=", jobObject

        f=open(algoObject['pcf_filename'], 'w')
        print "pcfName=", algoObject['pcf_filename']

        # Write static PCF data

        f.write("#\n")
        f.write("# onJobId:" + str(jobObject['jobID']) + "\n")
        f.write("# working directory:\n")
        f.write("# ProdRuleName: null\n")
        f.write("#\n")
        f.write("working_directory=" + jobObject['workingDirectory'] + "\n")
        f.write("nde_mode=" + os.environ['m'] + "\n")
        f.write("job_coverage_start=" + jobObject['obsStartTime_PCF'] + "\n")
        f.write("job_coverage_end=" +    jobObject['obsEndTime_PCF'] + "\n")
        f.write("production_site=proving\n")
        f.write("production_environment=dev1\n")


        for x in jobObject['jobSpec']['parameters']:
                #print x, jobObject['jobSpec']['parameters'][x]
                f.write(x['parameterName'] + '=' + x['parameterValue'] + '\n')

        for x in jobObject['pcf']:
                #print "x=", x
                for key, value in x.items():
                        #print key,value
                        f.write(key + "=" + value + "\n")


        f.close()

# Create empty PSF

        f=open(algoObject['psf_filename'], 'w')
        f.close()

        return None


def persistJobStatus(jobObject):

#        print "persisting=", jobObject

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
                )
        )

        dbconn.commit()

def persistProductionJobStatus(jobObject):

#        logit( "persisting=" + json.dumps(jobObject) )

        sql = """update productionJob
                set
                prjobStartTime = %s,
                prjobCompletionTime = %s,
                prJobStatus = %s,
                pralgorithmReturnCode = %s,
                prjobHighestErrorClass = %s,
                prjobPID = %s,
                prjobCPU_Util = %s,
                prjobMem_Util = %s,
                prjobIO_Util = %s,
                CLOud_Worker_Node = %s
                where prJobId = %s
"""

        #print "sql=", sql

        row=cursor.execute(sql, (
                jobObject["jobStartTime"],
                jobObject["jobCompletionTime"],
                jobObject["jobStatus"],
                jobObject["executableRC"],
                jobObject["jobHighestErrorClass"],
                jobObject["jobPID"],
                jobObject["cpuutil"],
                jobObject["memutil"],
                jobObject["ioutil"],
                jobObject["hostName"],
                jobObject["jobID"]
                )
        )
        dbconn.commit()

def persistJobOutputFiles(jobObject):

#        print "persisting job output filenames"
        sql = "delete from productionJobOutputFiles where prJobId = %s"
        cursor.execute(sql, (jobObject['jobID'],) )

        sql = "insert into productionJobOutputFiles values (%s, %s, %s)"
        #print "sql=", sql

        i=0
        for fileName in jobObject['outputFiles']:
#               print "inserting into productionjoboutputfiles", jobObject['jobID'], i, fileName
                i+=1
                row=cursor.execute(sql, (jobObject['jobID'], i, fileName))

        dbconn.commit()


def createWorkingDirectory(jobID):

        # Create Working Directory

        workingDirectory = workingStorage + '/' + str(jobID)

#        print "wd=", workingDirectory

        if not os.path.exists(workingDirectory):
                os.makedirs(workingDirectory)
        else:
                shutil.rmtree(workingDirectory)
                os.makedirs(workingDirectory)

        return workingDirectory




def executableExists(algoObject):

        algoExec=algorithmBaseDir + '/' + algoObject['executablelocation'] + '/' + algoObject['executablename']
        # print logit("prJobId: " str(jobObject['jobID'] ) + " invoking algoExec=" + algoExec)
        if os.path.exists(algoExec):
                return True

        return False


def invokeExecutable(jobObject, algoObject):

        # Invoke "Algorithm"


        #executableName = algoObject['commandprefix'] + ndeCodeStorage + algoObject['executablelocation'] + '/' + 'dss_badrc.pl'

        runThis = [ '/usr/bin/time' ]

#        print "commandprefix=" + algoObject['commandprefix'] + "<<<<"

        # Empty commandprefix is a single space character??????

        if algoObject['commandprefix'] == " ":
                pass
        else:
                # if the command prefix has multiple arguments split
                cpArgs = algoObject['commandprefix'].split()
                for arg in cpArgs:
                        runThis.append(arg)


        executableName = algorithmBaseDir + '/' + algoObject['executablelocation'] + '/' + algoObject['executablename']

        runThis.append(executableName)

        runThis.append(jobObject['workingDirectory'])

#executableRC=""
#try:
#       algoSTDOUT = sp.check_output([runThis], stderr=sp.STDOUT)
#       executableRC = 0
#except sp.CalledProcessError as e:
#       executableRC = e.returncode
#       print "Exception raised " + str(e.returncode) + "<<<<"
#       print "Exception output " + str(e.output) + "<<<<"

#jobObject['executableRC']=executableRC


#        print "runThis=", runThis
        runMessage = ', '.join([str(x) for x in runThis])

        logit("prJobId: " + str(jobObject['jobID']) + " invoking " + runMessage + ".")
        #myJob = runThis

        p = Popen (runThis, stderr=loge, stdout=logf)

        jobLoop=1
        ctr = 0
        jobPid = p.pid

#        logit("executing algorithm pid = " + str(jobPid))


        while (jobLoop):
                ctr += 1
                if ctr % 100 == 0:
                        logit("prJobId: " + str(jobObject['jobID'] ) + " executing. pid = " + str(jobPid) )
                time.sleep(.1)
                x=p.poll()
                if x == None:
                        continue
                else:
                        jobLoop=0

        return p.returncode







def getJobOutputs(jobObject, algoObject):
        logMessages = []
        highestErrorClass = None


        logit("prJobId: " + str(jobObject['jobID']) + " logfilename=" + jobObject['logfilename'] )

        if os.path.isfile(jobObject['logfilename']):
#                print "opening file"
                f=open(jobObject['logfilename'], 'r')
                for line in f:
                        if re.match(algoObject['logmessagecontext'], line):
                                if re.match(algoObject['logmessagewarn'], line):
                                        highestErrorClass == "WARN"
#                                        print logit(line)
                                        logMessages.append(line)
                                if re.match(algoObject['logmessageerror'], line):
#                                        print logit(line)
                                        highestErrorClass == "ERROR"
                                        logMessages.append(line)
                f.close()

        return { "log" : logMessages, "highestErrorClass" : highestErrorClass }








def do_OnDemandJob(jobID):

        sql = "select odJobStatus, substr(to_char(to_timestamp(odjobSpec->>'obsStartTime', 'YYYY-MM-DD HH24:MI:SS'), 'YYYYMMDDHH24MISSMS'), 1, 15), substr(to_char(to_timestamp(odjobSpec->>'obsEndTime', 'YYYY-MM-DD HH24:MI:SS'), 'YYYYMMDDHH24MISSMS'), 1, 15), odJobSpec from onDemandJob where odJobId = %s"


        cursor.execute(sql, (jobID,))
        dbconn.commit()

        row = cursor.fetchone()

        jobObject = {
                "jobID" : jobID,
                "jobStatus" : row[0],
                "obsStartTime" : row[1],
                "obsEndTime" : row[2],
                "jobSpec" : row[3],
                "jobOutput" : None,
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
        #       print row
                algoObject['parameters'].append(row[2])

        sql = "select * from algoInputProd where algorithmId = %s"
        row=cursor.execute(sql, ( algoObject['id'],))

        rows = cursor.fetchall()
        dbconn.commit()

        for row in rows:
        #       print row
                algoObject['inputs'].append(row[0])


        sql = "select * from algoOutputProd where algorithmId = %s"
        row=cursor.execute(sql, ( algoObject['id'],))

        rows = cursor.fetchall()
        dbconn.commit()

        for row in rows:
        #       print row
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
        #       for fileId in jobObject['jobSpec']['inputs'][inputSpec]:
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
                logit( "Error in onDemand job Spec.", invalidoDJSReason, "exiting.....")
                exit(-1)

        logit("Valid job Spec, Executing Job.")

        jobObject['jobStatus']='RUNNING'
        persistJobStatus(jobObject)

        print "Done persist running."

        #Validate Executable

        if executableExists(algoObject):
                pass
        else:
                logit( "Algorithm ", algoExec, " not found.")
                exit(-1)


        # Create Working Directory

        jobObject['workingDirectory']=createWorkingDirectory(jobID)

        #print "wd=", jobObject['workingDirectory']

        # Copy inputs to working directory

        os.chdir(jobObject['workingDirectory'])



        # Get Inputs

        #cpInputsToWD(jobObject)   --- Not sure about updates to jobObject in function

        jobObject['jobSpec']['fileName']=[]
        jobObject['pcf'] = []
        for inputSpec in jobObject['jobSpec']['inputs']:
                #print "is=", inputSpec
                fileHandle=inputSpec['prisFileHandle']
                for fileId in inputSpec['fileIds']:
                        logit( "prJobId: " + str(jobObject['jobID']) + " getting fileId: " + str(fileId))
                        #fileName=edm.file_get(fileId)
                        getFileRes = nrc.getFile(fileId = fileId, retryOnFailure = True, resultAsDict = True)
                        fileName = getFileRes.get('filename')
  			if fileName is None:
				print(getFileRes)
                        jobObject['pcf'].append({ fileHandle : fileName } )
                        logit( "prJobId: " + str(jobObject['jobID']) + " got fileId: " + str(fileId) + " " + fileName)




        # Create PCF and PSF

        createPCF(jobObject, algoObject)


#        print "jobObject", jobObject
#        print "algoObject", algoObject

        # Invoke "Algorithm"

        #executableName = algoObject['commandprefix'] + ndeCodeStorage + algoObject['executablelocation'] + '/' + 'dss_badrc.pl'
        jobObject['executableName'] = algoObject['commandprefix'] + algorithmBaseDir + algoObject['executablelocation'] + '/' + algoObject['executablename']


        jobObject['executableRC']=invokeExecutable(jobObject, algoObject)

#        logit("prJobId: " + str(jobObject['jobID']) + "algorithm return code: " + str(jobObject['executableRC']) )




        # Get Output Info (logs and PSF)

        jobObject['logfilename']=jobObject['workingDirectory'] + '/' + algoObject['logfilename']

        jobObject['joboutputs']=getJobOutputs(jobObject, algoObject)

#       logMessages = []
#       highestErrorClass = None

#       if os.path.isfile(jobObject['logfilename']):
#               f=open(jobObject['logfilename'], 'r')
#               for line in f:
#                       if re.match(algoObject['logmessagecontext'], line):
#                               if re.match(algoObject['logmessagewarn'], line):
#                                       highestErrorClass == "WARN"
#                                       print line
#                                       logMessages.append(line)
#                               if re.match(algoObject['logmessageerror'], line):
#                                       print line
#                                       highestErrorClass == "ERROR"
#                                       logMessages.append(line)


        if jobObject['executableRC'] <> 0:
                jobObject['jobStatus']='FAILED'
        else:
                jobObject['jobStatus']='COMPLETE'

        jobObject['psf']=jobObject['workingDirectory'] + '/' + algoObject['psf_filename']

        if os.path.isfile(jobObject['psf']):
                f=open(jobObject['psf'], 'r')
                for line in f:
                        line=line.rstrip()
                        if not(line == '#END-of-PSF'):
                                logit("output " +  line)
                                if jobType == "onDemand":
                                        s3 = boto3.resource('s3')
                                        fileName = jobObject['workingDirectory'] + '/' + line
                                        key = "outgoing/" + str(jobObject['jobID']) + '/' + line
                                        logit( "key="+ key + "<<<<" )
                                        try:
                                                s3.meta.client.upload_file(fileName, "ndepg", key)
                                        except ClientError as e:
                                                logit( "s3 error= " + e)
                                        else:
                                                s3c = boto3.client('s3')
                                                url = s3c.generate_presigned_url( ClientMethod='get_object', Params={ 'Bucket': 'ndepg', 'Key': key })

                                        jobObject['joboutputs']['s3_signedCERT']= url
                                        logit(line + " " + "copied to outgoing. cert=" + " " + url)
                                else:
                                        print "do edm ingest"

        #shutil.rmtree(jobObject['workingDirectory'])

        jobObject["jobCompletionTime"]=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        persistJobStatus(jobObject)

        logf.close()
        loge.close()

        os.remove( childOfMineLog )
        os.remove( childOfMineLogErr )

        logit("job " + str(jobID) + " done status = " + jobObject['jobStatus'])

        return None





def abort_production_job(jobObject, abortStatus):

        loge.close()
        logf.close()


        jobObject['jobStatus']=abortStatus
        jobObject["jobCompletionTime"]=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        persistProductionJobStatus(jobObject)
        shutil.rmtree(jobObject['workingDirectory'])

        logit("JobId: " + str(jobID) + " aborted.")
        exit(9)

        return True










def do_ProductionJob(jobID):

        sql="select prJobStatus, to_char(pjsObsStartTime, 'YYYY-MM-DD HH24:MI:SS'), to_char(pjsObsEndTime, 'YYYY-MM-DD HH24:MI:SS'), substr(to_char(pjsObsStartTime, 'YYYYMMDDHH24MISSMS'), 1, 15), substr(to_char(pjsObsEndTime, 'YYYYMMDDHH24MISSMS'), 1, 15), j.prodPartialJobId, s.prId, r.algorithmId from productionRule r, productionJob j,  productionJobSpec s where prJobId= %s and s.prodPartialJobId = j.prodPartialJobId and s.prId = r.prId"

        cursor.execute(sql, (jobID,))
        dbconn.commit()

        row = cursor.fetchone()

        if row == None:
                logit( "Job " +  jobID + " not found.... exiting.")
                return None

        jobObject = {
                "algorithmId" : row[7],
                "prodPartialJobId" : row[5],
                "prId" : row[6],
                "jobID" : jobID,
                "jobStatus" : row[0],
                "obsStartTime" : row[1],
                "obsEndTime" : row[2],
                "obsStartTime_PCF" : row[3],
                "obsEndTime_PCF" : row[4],
                "hostName" : hostname,
                "jobSpec" : {},
                "jobOutput" : None,
                "jobStartTime" : time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                "jobCompletionTime" : None,
                "executableRC" : None,
                "jobHighestErrorClass" : None,
                "invokerJobPID" : os.getpid(),
                "jobPID" : None,
                "cpuutil" : None,
                "ioutil" : None,
                "memutil" : None,
                "outputFiles" : []
        }

        prodPartialJobId = row[5]
        prId = row[6]
        algorithmId = row[7]


#        print "prodPartialJobId=", prodPartialJobId
#        print "prId = ", prId
#        print "algorithmId = ", algorithmId

        sql="select * from algorithm a where algorithmId = %s"

        cursor.execute(sql, (algorithmId,))
        dbconn.commit()

        row = cursor.fetchone()

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

        sql = "select algoParameterName, jsParameterValue from algoParameters a, prParameter p, jobSpecParameters j where a.algorithmId = %s and a.algoParameterId = p.algoParameterId and j.prodPartialJobId = %s and j.prParameterSeqNo = p.prParameterSeqNo and prId = %s order by p.prparameterseqno"
        row=cursor.execute(sql, ( algorithmId, prodPartialJobId, prId))

        rows = cursor.fetchall()
        dbconn.commit()

        jobObject['jobSpec']['parameters'] = []

        for row in rows:
        #       print row
        #       algoObject['parameters'].append(row[2])
                jobObject['jobSpec']['parameters'].append( { "parameterName" : row[0], "parameterValue" : row[1] } )

        sql = "select distinct productShortName, prisFileHandle, prisfilehandlenumbering, p.productId, s.prisId from jobspecinput i, prinputproduct ip, prInputSpec s, productDescription p where i.prodPartialJobId = %s and i.pripid = ip.pripid and ip.prisId = s.prisId and ip.productId = p.productId order by s.prisId"

#        print "sql=", sql

        row=cursor.execute(sql, ( prodPartialJobId,))

        rows = cursor.fetchall()
        dbconn.commit()

        jobObject['jobSpec']['inputs'] = []

        for row in rows:
#                print row
                jobObject['jobSpec']['inputs'].append(  {
                        "prisId"        : row[4],
                        "productId"        : row[3],
                        "productShortName" : row[0],
                        "prisFileHandle"   : row[1],
                        "prisFileHandleNumbering" : row[2],
                        "fileIds" : []
                }
                )


#        print "inputs=", jobObject['jobSpec']['inputs']


        for input in jobObject['jobSpec']['inputs']:
#                print input
                sql = "select f.fileId from fileMetadata f, jobSpecInput i, prinputproduct ip, prinputspec pris where i.fileId = f.fileId and i.prodPartialJobId = %s and i.pripid = ip.pripid and ip.prisid = pris.prisid and pris.prisid = %s order by f.fileEndTime"
#                print "sql=", sql
                row=cursor.execute(sql, ( prodPartialJobId, input['prisId'] ))

                rows = cursor.fetchall()
                dbconn.commit()
                for row in rows:
#                        print row[0]
                        input['fileIds'].append(str(row[0]))



        if executableExists(algoObject):
                pass
        else:
                logit( "Algorithm ", algoExec, " not found.")
                exit(-1)


        jobObject['jobStatus']='RUNNING'
        persistProductionJobStatus(jobObject)

        jobObject['workingDirectory']=createWorkingDirectory(jobID)

        os.chdir(jobObject['workingDirectory'])

        # Get Inputs

        #cpInputsToWD(jobObject)   --- Not sure about updates to jobObject in function

        jobObject['jobSpec']['fileName']=[]
        jobObject['pcf'] = []
        for inputSpec in jobObject['jobSpec']['inputs']:
                #print "is=", inputSpec
                fileHandle=inputSpec['prisFileHandle']
                prisFN_Ctr = 1
                for fileId in inputSpec['fileIds']:
                        logit( "prJobId: " + str(jobObject['jobID']) + " getting fileId: " + str(fileId))
                        # fileName=edm.file_get(fileId)
                        getFileRes = nrc.getFile(fileId = fileId, retryOnFailure = True, resultAsDict = True)
                        fileName = getFileRes.get('filename')
                        if fileName is None:
                                # File not found, set job status to fail, delete working directory and abort
                                logit( "prJobId: " + str(jobObject['jobID']) + " Could NOT get file: " + str(fileId) + " " + str(fileName) + " " + str(getFileRes))
				#logit( "prJobId: " + str(jobObject['jobID']) + "got fileId: " + str(fileId) + " " + str(fileName))
                                logit( "input not found, aborting job.")
                                abort_production_job(jobObject, 'FAILED-CPIN')
                        else:
                                if inputSpec['prisFileHandleNumbering'] == 'Y':
                                        jobObject['pcf'].append({ fileHandle+'_'+str(prisFN_Ctr) : fileName } )
                                else:
                                        jobObject['pcf'].append({ fileHandle : fileName } )
                                logit( "prJobId: " + str(jobObject['jobID']) + " got fileId: " + str(fileId) + " " + str(fileName))
                                prisFN_Ctr += 1


        # Create PCF and PSF

        createProductionPCF(jobObject, algoObject)


        # Invoke "Algorithm"

        #executableName = algoObject['commandprefix'] + ndeCodeStorage + algoObject['executablelocation'] + '/' + 'dss_badrc.pl'
        jobObject['executableName'] = algorithmBaseDir + algoObject['executablelocation'] + '/' + algoObject['executablename']

        logit("prJobId: " + str(jobObject['jobID']) + " job object prior to invocation: " + json.dumps(jobObject))

        jobObject['executableRC']=invokeExecutable(jobObject, algoObject)

        logit( "prJobId: " + str(jobObject['jobID']) + " algorithm return code =" + str(jobObject['executableRC']) )


        #print "jobObject", json.dumps(jobObject, indent=2)
        #print "algoObject", json.dumps(algoObject, indent=2)


        # Get Output Info (logs and PSF)

        # if algorithm doesn't invoke, then there is no logfileName

        jobObject['logfilename']=jobObject['workingDirectory'] + '/' + algoObject['logfilename']

#        print "before get outputs"

        jobObject['joboutputs']=getJobOutputs(jobObject, algoObject)

#        print "after get outputs"

        if jobObject['executableRC'] <> 0:
                jobObject['jobStatus']='FAILED'
        else:
                jobObject['jobStatus']='COMPLETE'

        jobObject['psf']=jobObject['workingDirectory'] + '/' + algoObject['psf_filename']

        logit("prJobId: " + str(jobObject['jobID']) + " return code: " +  str(jobObject['executableRC']) )
        logit("prJobId: " + str(jobObject['jobID']) + " prJobStatus: " +  str(jobObject['jobStatus']) )

        # For production jobs
        #       Copy the output file to (i/) incoming_input
        #       Send message to newfile Q

#       print "looking for PSF", jobObject['psf']

        if os.path.isfile(jobObject['psf']):
                logit( "prJobId: " + str(jobObject['jobID']) + " PSF found=" +  jobObject['psf'])
                f=open(jobObject['psf'], 'r')
                for line in f:
#                       print ">>>> line=", line, "<<<<<<"
                        line=line.rstrip()
                        if not(line.lower() == "#END-of-PSF".lower()) and not(line == ""):

                                # Some algorithm have the workingdirectory in the name, some don't
                                # Some have to have it included because some files are in subdirectories of the working directory
                                # Some algorithms include './' in front of the filename (technically they should not, but they do anyway)

                                fileFullName=""
                                if jobObject['workingDirectory'] in line:
                                        fileFullName=line
                                elif line.startswith('./'):
                                        # Replaces only the first '.' with the working directory.
                                        fileFullName=line.replace('.', jobObject['workingDirectory'], 1)
                                else:
                                        fileFullName = jobObject['workingDirectory'] + '/' + line

                                fileName = line.split('/')[-1]

                                logit("prJobId:" + str(jobObject['jobID']) + " output: " +  fileFullName)
                                jobObject['outputFiles'].append(fileName)
#                               print "---->>>>", jobObject['outputFiles'], "<<<<<<<<-------"
                                s3 = boto3.resource('s3')
#                                print fileFullName, fileName
                                key = "i/" + fileName
#                                logit( "key="+ key + "<<<<" )
                                try:
                                        s3.meta.client.upload_file(fileFullName, "ndepg", key)
                                except ClientError as e:
                                        logit("prJobId: " + str(jobObject['jobID']) + "s3 error= " + e)
                                else:
                                        sns_c = boto3.client('sns', aws_access_key_id = "AKIAIYFRXG3MWD5T6CKA", aws_secret_access_key = "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", region_name="us-east-1")
                                        sns_c.publish(TopicArn="arn:aws:sns:us-east-1:784330347242:NewProduct", Message=json.dumps({"filename" : fileName}))
                                        logit("prJobId: " + str(jobObject['jobID']) + " " + fileFullName + " " + "copied to (i/) incoming_input and sns message sent.")

        #shutil.rmtree(jobObject['workingDirectory'])

#        jobObject["jobCompletionTime"]=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        persistProductionJobStatus(jobObject)

        persistJobOutputFiles(jobObject)

#        print "jobObject", json.dumps(jobObject, indent=2)
        #print "algoObject", json.dumps(algoObject, indent=2)

        loge.close()
        logf.close()

        logM = open (childOfMineLogErr, 'r')

        userCPU = None
        systemCPU = None
        memory = None
        job_io = None

        for line in logM:
                line = line.rstrip()
#                print "line=", line
                if "elapsed" in line or "inputs" in line:
                        logit("job " + str(jobID) + " metrics: " + line)
                        metrics = line.split()
                        for metric in metrics:
#                                print "metric=", metric
                                if "user" in metric:
                                        userCPU=metric.replace("user", "")
#                                        print "user=", metric.replace("user", "")
                                if "system" in metric:
                                        systemCPU=metric.replace("system", "")
#                                        print "sys=", metric.replace("system", "")
                                if "maxresident" in metric:
                                        memory=int(metric.replace("maxresident)k", ""))
#                                        print "mem=", metric.replace("maxresident)k", "")
                                if "inputs" in metric and "outputs" in metric:
                                        job_input  = metric.split("+")[0]
                                        job_output = metric.split("+")[1]
                                        job_input  = int(job_input.replace("inputs", ""))
                                        job_output  = int(job_output.replace("outputs", ""))
                                        job_io = job_input + job_output * 512

#       print (userCPU, systemCPU, memory, job_io)

        logit("prJobId: " + str(jobID) + " metrics: userCPU: " + str(userCPU) + ",sysCPU:" + str(systemCPU) + ",maxresidentmemory(k):" + str(memory) + " io=" + str(job_io))

        if userCPU <> None or systemCPU <> None:
                jobObject['cpuutil'] = float(userCPU) + float(systemCPU)
        if memory <> None:
                jobObject['memutil'] = int(memory) * 1024
        if  job_io <> None:
                jobObject['ioutil'] = job_io


#        jobObject["jobCompletionTime"]=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
#        persistProductionJobStatus(jobObject)

# Get runtime metrics
# 44.40user 1.41system 0:45.79elapsed 100%CPU (0avgtext+0avgdata 987696maxresident)k
# 0inputs+1118464outputs (2major+237471minor)pagefaults 0swaps

        logM.close()


#       print ('Current directory content ====', os.listdir(os.getcwd()))

        if jobObject['jobStatus']=='FAILED':
                forensicsFileName = '../../forensics_data/' + str(jobID) + '.tar.gz'
                print ("fn=", forensicsFileName)
                subprocess.call(['tar', '-zcvf', forensicsFileName, jobObject['workingDirectory']] )
#tar -zcvf ../../forensics_data/8058040.tar.gz *


# Cleanup working directory

        shutil.rmtree(jobObject['workingDirectory'])

        jobObject["jobCompletionTime"]=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        persistProductionJobStatus(jobObject)


        return None





#
#
# Main
#
#

logit("Started runJobv3.py")

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

logit("*JobId: " + str(jobID) + " starting.")

if jobType == 'production':
        do_ProductionJob(jobID)

if jobType == 'onDemand':
        do_OnDemandJob(jobID)

#print "jobType=", jobType
#print "jobID=", jobID

# Making sure a cancellation request has been sent before the job in executed.

logit("*JobId: " + str(jobID) + " completed.")

logit("Completed runJobv3.py")

exit(0)


