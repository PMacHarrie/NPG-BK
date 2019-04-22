"""
    Author: Jonathan Hansford; SOLERS INC
    Contact: 
    Last modified: 2019-02-04
    Python Version: 3.6
"""

import sys
import os
import json
import re
import boto3
import botocore
from datetime import datetime
import psycopg2
import psycopg2.sql
import psycopg2.extras

sys.path.append(os.environ['LAMBDA_TASK_ROOT'] + '/lambda_functions')
import factoryEPGCommon

print('Loading completeJobSpec')
DATABASE_JOBSPECINPUT_ID_SEQUENCE = 's_jobspecinput'
DATABASE_PRODUCTIONJOB_ID_SEQUENCE = 's_productionjob'
PRODUCTION_JOB_TOPIC_ARN = 'arn:aws:sns:us-east-1:784330347242:ProductionJob'

# Database connection to be reused by subsequent Lambda executions in the same container.
conn = psycopg2.connect(
    host = os.environ['RD_HOST'],
    dbname = os.environ['RD_DBNM'],
    user = os.environ['RD_USER'],
    password = os.environ['RD_PSWD']
)

sns = boto3.resource('sns')
productionJobTopic = sns.Topic(PRODUCTION_JOB_TOPIC_ARN)

class InputFileAccumulator:
    def __init__(self, prodPartialJobId, pripId):
        self.prodPartialJobId = prodPartialJobId
        self.pripId = pripId
        self.totalFileAccumulation = 0
        self.filesList = []
        
    def addInputFile(self, fileId, fileJobCoverage):
        self.totalFileAccumulation += fileJobCoverage
        self.filesList.append(fileId)
        
    def getTotalFileAccumulation(self):
        return self.totalFileAccumulation
        
    def getJobSpecInputRows(self):
        """
        returns a list of dictionaries, which contain the following four keys: 'fileId', 'prodPartialJobId', 'pripId', and 'jsiIdSequence'.
        """
        jobSpecInputRows = []
        for fileId in self.filesList:
            jobSpecInputRows.append({
                'jsiIdSequence': DATABASE_JOBSPECINPUT_ID_SEQUENCE,
                'fileId': fileId,
                'prodPartialJobId': self.prodPartialJobId,
                'pripId': self.pripId
            })
        return jobSpecInputRows


def lambda_handler(event, context):
    # This lambda function is triggered by SQS.
    # This means that an array of messages will be added into 'Records' element of the event dictionary.
    # See: https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
    # The message body is expected to be in JSON format and contain the prodPartialJobId.
    for message in event['Records']:
        print('Processing message: %s' % str(message))
        
        if 'body' in message:
            completeJobSpec(conn, productionJobTopic, json.loads(message['body'])['prodPartialJobId'])
        else:
            raise Exception("Failed to obtain prodPartialJobId from message: %s" % message)
    
    # This lambda function has nothing to return.
    return None

  
def completeJobSpec(databaseConnection, productionJobTopic, prodPartialJobId):
    """
    Attempts to complete the job spec with the given prodPartialJobId. This job specification should already be in the 'COMPLETING' status.
    If the required inputs are available, then the job specification's status will be changed to COMPLETE, a new production job will be created, and this function will return True.
    If the required inputs are not available, then the job specification's status will be changed to COMPLETE-NOINPUT, and this function will return False.
    
    This function will also return True (or False) if the job specification is already in the COMPLETE (or COMPLETE-NOINPUT) status. 
    This situation can occur if the visibility timeout of the SQS message expires and the message is dequeued a second time before it is deleted.
    """
    print('Starting completeJobSpec on productionJobSpec with prodPartialJobId = %s' % prodPartialJobId)
    
    finishedMessage = 'Finished completeJobSpec on productionJobSpec with prodPartialJobId = %s' % prodPartialJobId
    
    # Need to cast prodPartialJobId to an integer because it is stored in the database as a bigint.
    prodPartialJobId = int(prodPartialJobId)
    
    prId = None
    jobClass = None # As an integer
    jobPriority = None # As an integer
    jobClassDescription = None # As a string
    jobPriorityDescription = None # As a string
    prRuleName = None
    algorithmId = None
    algorithmName = None
    prJobId = None
    
    acquireRowLockTimer = factoryEPGCommon.CodeSegmentTimer('Acquiring lock on PRODUCTIONJOBSPEC row and fetching it')

    with databaseConnection:
        with databaseConnection.cursor() as databaseCursor:
            # Lock the PRODUCTIONJOBSPEC row, to prevent this job specification from being completed twice (in the case that this function is called twice)
            # "NOWAIT" is used because the only situtations which could cause another user to be holding the lock on the row this function wants to acquire are:
            #   1. The visibility timeout of this SQS message expired before it could be deleted, so another execution of this Lambda is already processing the 
            #      same job spec as this one.
            #   2. A human is manually editing this row in the PRODUCTIONJOBSPEC table.
            # (jobSpecPoller commits its changes before sending the messages to the completeJobSpec SQS, so it cannot be holding the lock.)
            # In case 1, we might as well exit immediately, rather than wait and try to acquire the lock, only to find that the job specification is already 
            # complete. Case 2 should be rare, and if it does occur, the failed message will be automatically retried. 
            databaseCursor.execute("""
                SELECT
                    PRID,
                    JOBCLASS,
                    JOBPRIORITY,
                    PJSCOMPLETIONSTATUS
                FROM
                    PRODUCTIONJOBSPEC
                WHERE
                    PRODPARTIALJOBID = %(prodPartialJobId)s
                FOR NO KEY UPDATE NOWAIT
            """, {'prodPartialJobId': prodPartialJobId})
            
            results = databaseCursor.fetchall()
            
            acquireRowLockTimer.finishAndPrintTime()
            
            if len(results) == 1:
                prId, jobClass, jobPriority, pjsCompletionStatus = results[0]
                
                # Confirm that pjsCompletionStatus is 'COMPLETING' - otherwise, this function cannot continue.
                if pjsCompletionStatus == 'COMPLETE': 
                    print('The productionJobSpec %d was already in the COMPLETE status. completeJobSpec will not continue.' % prodPartialJobId)
                    return True
                elif pjsCompletionStatus == 'COMPLETE-NOINPUT':
                    print('The productionJobSpec %d was already in the COMPLETE-NOINPUT status. completeJobSpec will not continue.' % prodPartialJobId)
                    return False
                elif pjsCompletionStatus != 'COMPLETING':
                    # The job spec is in an unexcepted status, so raise an Exception.
                    raise Exception("The productionJobSpec %d is in status %s (must be in COMPLETING status to call completeJobSpec)." % (prodPartialJobId, pjsCompletionStatus))
            elif len(results) == 0:
                raise Exception("There is no PRODUCTIONJOBSPEC with prodPartialJobId = %d" % prodPartialJobId)
            else:
                raise Exception("Multiple rows with prodPartialJobId = %d found in the PRODUCTIONJOBSPEC table." % prodPartialJobId)
                
        # This stored procedure will return the file accumulation threshold percentage, and the expected total jobCoverage for each REQUIRED and TRIGGER input product.
        spGetFileAccumulationTimer = factoryEPGCommon.CodeSegmentTimer('Running SP_GET_FILE_ACCUMULATION and fetching results')

        fileAccumulationServerSideCursor = factoryEPGCommon.runStoredProcedureToGetServerSideCursor(databaseConnection, 'sp_get_file_accumulation', (prodPartialJobId,))
        fileAccumulation = fileAccumulationServerSideCursor.fetchall()
        fileAccumulationServerSideCursor.close()
        
        spGetFileAccumulationTimer.finishAndPrintTime()
        
        # This is a nested map of <prisId> -> <prInputPreference> -> <An InputFileAccumulator object>
        requiredInputFileAccumulators = {}
        
        # This is a map of <prisId> -> <minimum total of fileJobCoverage required to run the job (in milliseconds)>
        fileAccumulationTargets = {}
        
        for row in fileAccumulation:
            pripId, prisId, prInputPreference, prisFileAccumulationThreshold, jobCoverage = row
            
            # Initialize an InputFileAccumulator under the appropriate key in requiredInputFileAccumulators.
            if prisId not in requiredInputFileAccumulators:
                requiredInputFileAccumulators[prisId] = {}
            if prInputPreference not in requiredInputFileAccumulators[prisId]:
                requiredInputFileAccumulators[prisId][prInputPreference] = InputFileAccumulator(prodPartialJobId, pripId)
            else:
                raise Exception("Bad production rule (prId = %d): the pair (prisId = %d, prInputPreference = %d) occurred twice." % (prId, prisId, prInputPreference))
                
            # Compute the minimum amount of coverage needed for this prInputSpec. 
            fileAccumulationTargets[prisId] = jobCoverage * prisFileAccumulationThreshold / 100
        
        # This stored procedure will return the amount of time a each file overlaps with the job (fileJobCoverage), including OPTIONAL files.
        spGetJismoTimer = factoryEPGCommon.CodeSegmentTimer('Running SP_GET_JISMO and fetching results')
        
        jismoServerSideCursor = factoryEPGCommon.runStoredProcedureToGetServerSideCursor(databaseConnection, 'sp_get_jismo', (prodPartialJobId,))
        jismo = jismoServerSideCursor.fetchall()
        jismoServerSideCursor.close()
        
        spGetJismoTimer.finishAndPrintTime()
        
        # This is also a nested map of <prisId> -> <prInputPreference>  -> <An InputFileAccumulator object>
        # It stores the 'OPTIONAL' input files.
        optionalInputFileAccumulators = {}
        
        for row in jismo:
            fileId, prisId, pripId, productId, prInputPreference, pjsObsStartTime, pjsObsEndTime, pjsTimeoutTime, prisFileHandle, prisTest, fileJobCoverage = row
            
            if doesFilePassPrisTest(databaseConnection, fileId, prisTest):
                if prisId in requiredInputFileAccumulators:
                    # The file must be a REQUIRED (or TRIGGER) product, if its prisId is in requiredInputFileAccumulators. 
                    # These accumulators have already been initialized.
                    requiredInputFileAccumulators[prisId][prInputPreference].addInputFile(fileId, fileJobCoverage)
                else:
                    # The file is an optional input product.
                    
                    # Initialize an accumulator for this PrInputProduct, if one does not yet exist. 
                    if prisId not in optionalInputFileAccumulators:
                        optionalInputFileAccumulators[prisId] = {}
                    if prInputPreference not in optionalInputFileAccumulators[prisId]:
                        optionalInputFileAccumulators[prisId][prInputPreference] = InputFileAccumulator(prodPartialJobId, pripId)
                        
                    optionalInputFileAccumulators[prisId][prInputPreference].addInputFile(fileId, fileJobCoverage)
        
        newJobSpecInputRows = []
        # Check each required prInputSpec to see if the file accumulation threshold has been satisified for at least one of the prInputProducts tags under it.
        # Take the prInputProduct with the lowest value of prInputPreference.
        for prisId in requiredInputFileAccumulators.keys():
            
            isFileAccumulationThresholdMetForThisPrInputSpec = False
            
            for prInputPreference in sorted(requiredInputFileAccumulators[prisId].keys()):
                if requiredInputFileAccumulators[prisId][prInputPreference].getTotalFileAccumulation() >= fileAccumulationTargets[prisId]:
                    # This prInputProduct is the "winner". 
                    isFileAccumulationThresholdMetForThisPrInputSpec = True
                    
                    # Add the input files related to it to the new jobSpecInput rows 
                    for row in requiredInputFileAccumulators[prisId][prInputPreference].getJobSpecInputRows():
                        newJobSpecInputRows.append(row)
                    
                    # Don't add any other files for this prInputSpec.
                    break
            
            if not isFileAccumulationThresholdMetForThisPrInputSpec:
                print('For the productionJobSpec %d, there are not enough input files for prisId: %d.' % (prodPartialJobId, prisId))
                
                # We need to delete the rows related to this job spec from the JOBSPECPARAMETERS table, because they were already added 
                # by the SP_GET_COMPLETED_JOB_SPECS stored procedure.
                deleteJobSpecParameters(databaseConnection, prodPartialJobId)
                setProductionJobSpecCompletionStatus(databaseConnection, prodPartialJobId, 'COMPLETE-NOINPUT')
                print(finishedMessage)
                return False

        # If the execution has made it to this point, then enough TRIGGER/REQUIRED input files are present to start the job.
        
        # For each OPTIONAL prInputSpec - add the set of input files with the lowest prInputPreference to the new jobSpecInput rows.
        # (There will always be files associated with any InputFileAccumulator for an OPTIONAL input, since they are not initialized unless a file is found by SP_GET_JISMO)
        for prisId in optionalInputFileAccumulators.keys():
            lowestPrInputPreferenceAvailable = min(optionalInputFileAccumulators[prisId].keys())
            
            for row in optionalInputFileAccumulators[prisId][lowestPrInputPreferenceAvailable].getJobSpecInputRows():
                newJobSpecInputRows.append(row)
        
        print('Adding input files for productionJobSpec %d to the JOBSPECINPUT table.' % prodPartialJobId)
        insertJobSpecInputRowsTimer = factoryEPGCommon.CodeSegmentTimer('Inserting input files into JOBSPECINPUT')
        
        with databaseConnection.cursor() as databaseCursor:
            psycopg2.extras.execute_values(databaseCursor, """
                INSERT INTO
                    JOBSPECINPUT (JSIID, FILEID, PRODPARTIALJOBID, PRIPID)
                    VALUES %s
            """, newJobSpecInputRows, template='(nextval(%(jsiIdSequence)s), %(fileId)s, %(prodPartialJobId)s, %(pripId)s)')
        
        insertJobSpecInputRowsTimer.finishAndPrintTime()
        
        setProductionJobSpecCompletionStatus(databaseConnection, prodPartialJobId, 'COMPLETE')
        
        prJobId = createNewProductionJob(databaseConnection, prodPartialJobId)
        
        print('Querying database to obtain values of SNS message attributes.')
        
        queryForMessageAttributesTimer = factoryEPGCommon.CodeSegmentTimer('Querying database for SNS message attributes and fetching results')
        
        with databaseConnection.cursor() as databaseCursor:
            databaseCursor.execute("""
                SELECT
                    pr.PRRULENAME,
                    a.ALGORITHMID,
                    a.ALGORITHMNAME,
                    jcc.JOBCLASSDESCRIPTION,
                    jpc.JOBPRIORITYDESCRIPTION
                FROM
                    PRODUCTIONRULE pr,
                    ALGORITHM a,
                    JOBCLASSCODE jcc,
                    JOBPRIORITYCODE jpc
                WHERE
                    pr.PRID = %(prId)s and
                    pr.ALGORITHMID = a.ALGORITHMID and
                    jcc.JOBCLASS = %(jobClass)s and
                    jpc.JOBPRIORITY = %(jobPriority)s
            """, {'prId': prId, 'jobClass': jobClass, 'jobPriority': jobPriority})
            
            results = databaseCursor.fetchall()
            
            if len(results) == 1:
                prRuleName, algorithmId, algorithmName, jobClassDescription, jobPriorityDescription = results[0]
            else:
                raise Exception("Query to get SNS message attributes returned an unexpected number of results: %d" % len(results))
                
        queryForMessageAttributesTimer.finishAndPrintTime()
    # end with databaseConnection
    
    # At this point, the changes have been committed to Postgres. 
    # This means that if publishing the message to SNS fails, we must undo the changes we made to the database, in order to allow the job spec to be completed again.
    
    print('Publishing productionJob %d to ProductionJob SNS topic.' % prJobId)
    
    publishToSNSTimer = factoryEPGCommon.CodeSegmentTimer('Publishing production job message to SNS')
    try:
        productionJobTopic.publish(
            Message=str(prJobId),
            MessageAttributes={
                'algorithmDomain': createStringSnsMessageAttribute("Enterprise"),
                'algorithmId': createStringSnsMessageAttribute(algorithmId),
                'algorithmName': createStringSnsMessageAttribute(algorithmName),
                'prId': createStringSnsMessageAttribute(prId),
                'prJobClass': createStringSnsMessageAttribute(jobClassDescription),
                'prJobPriority': createStringSnsMessageAttribute(jobPriorityDescription),
                'prRuleName': createStringSnsMessageAttribute(prRuleName),
                'prodPartialJobId': createStringSnsMessageAttribute(prodPartialJobId),
                'prJobId': createStringSnsMessageAttribute(prJobId)
            }
        )
    except:
        print('Failed to publish productionJob %d to SNS. Will attempt to rollback database changes.' % prJobId) 
        
        with databaseConnection:
            # Delete the newly created production job
            deleteProductionJob(databaseConnection, prJobId)
            
            # Delete the job spec inputs that were added
            deleteJobSpecInputs(databaseConnection, prodPartialJobId)
            
            # Revert the status of the job specification to 'COMPLETING', so this Lambda function can attempt to complete it again.
            setProductionJobSpecCompletionStatus(databaseConnection, prodPartialJobId, 'COMPLETING')
            
        print('Database changes were successfully rolled back.')
        
        # Re-raise the exception thrown by SNS, so the Lambda execution will be marked as failed and the associated SQS messages not dequeued (so this invocation).
        raise
    
    publishToSNSTimer.finishAndPrintTime()
    print(finishedMessage)
    return True


def doesFilePassPrisTest(databaseConnection, fileId, prisTest):
    """
    Checks if the file with the given fileId passes the filtering test(s) given in prisTest.
    Returns true if the file passes all tests (or if there were no tests). Returns false if the file fails at least one test.
    
    databaseConnection is assumed to be an active database connection, with a transaction in progress (or not yet started). This function will not call commit on that databaseConnection, nor close it.
    """

    if prisTest is None or prisTest == "null":
        # If there is no test to execute, then the file automatically passes.        
        return True
    else:
        print('Begin filtering test on fileId = %d; prisTest = %s' % (fileId, prisTest))
        
        # There can be one or multiple tests in prisTest. (Assuming prisTest != 'null')
        # All tests, including the last test in the list, should end with a semicolon, so the last element in the 'tests' array should be empty.
        tests = prisTest.strip().split(';')
        for i in range(0, len(tests) - 1):
            if not doesFilePassTestCondition(databaseConnection, fileId, tests[i]):
                return False
        
        # Some production rules exist which have prisTests that do not end with a semicolon, violating the convention.
        # If that is the case, we process the last group of characters as if it were a test condition anyway.
        if len(tests[len(tests) - 1]) > 0:
            print ("Warning: bad prisTest: Either there are extra characters after the final semicolon, or there is no semicolon at all. prisTest was: \"%s\". Will attempt to interpret the extra characters as a test condition." % prisTest)
            if not doesFilePassTestCondition(databaseConnection, fileId, tests[len(tests) - 1]):
                return False

        print ('File with fileId = %d passed all filtering tests.' % fileId)
        return True


def doesFilePassTestCondition(databaseConnection, fileId, testCondition):
    """
    Tests whether the given file passes a single testCondition within a prisTest.
    Each individual test condition has the format: <TestCategory>:<Attribute>:<TestType>:<TestValue>
    (Each test condition is followed by a semicolon, but do not include the trailing semicolon in the testCondition argument.)
    databaseConnection is assumed to be an active database connection, with a transaction in progress (or not yet started). This function will not call commit on that databaseConnection, nor close it.
    
    Returns True if the file passes the filtering test. Returns False if the file fails the filtering test.
    """
    testParts = testCondition.split(':')

    if len(testParts) == 4:
        testCategory, attribute, testType, testValue = testParts
        
        # Note: 'testValue' is the range or list of acceptable values. 
        # 'testResult' is the actual value of the attribute in the database (which must be compared against the acceptable values)
        testResult = None
        
        # Obtain the value of 'testResult' according to the testCategory and attribute.
        # Note: the attribute must be passed as lower case, because the sql.Identifier(...) constructor will surround it in double quotes.
        # If the identifier is surrounded with double quotes, then the database will only recognize it if it is an exact match (and the column names are all lowercase).
        if testCategory == "fm":
            fmAttributeQuery = psycopg2.sql.SQL("""
                SELECT 
                    {} 
                FROM
                    FILEMETADATA
                WHERE
                    FILEID = %(fileId)s
                """).format(psycopg2.sql.Identifier(attribute.lower()))
            
            with databaseConnection.cursor() as databaseCursor:
                databaseCursor.execute(fmAttributeQuery, {'fileId': fileId})
                
                fmAttributeQueryResults = databaseCursor.fetchall()
                
                if len(fmAttributeQueryResults) == 1:
                    testResult = (fmAttributeQueryResults[0])[0]
                else:
                    raise Exception("prisTest failed: fmAttributeQuery returned an unexpected number of results (%d). Test condition was: %s" % (len(fmAttributeQueryResults), testCondition))
        elif testCategory == "fqs":
            # Notice: ingest is not currently populating this table, so any tests on this table will fail.
            with databaseConnection.cursor() as databaseCursor:
                databaseCursor.execute("""
                    SELECT
                        FILEQUALITYSUMMARYVALUE
                    FROM
                        FILEQUALITYSUMMARY
                    WHERE
                        FILEID = %(fileId)s and
                        FILEQUALITYSUMMARYNAME = %(fileQualitySummaryName)s
                """, {'fileId': fileId, 'fileQualitySummaryName': attribute})
                
                fqsAttributeQueryResults = databaseCursor.fetchall()
                
                if len(fqsAttributeQueryResults) == 1:
                    testResult = (fqsAttributeQueryResults[0])[0]
                elif len(fqsAttributeQueryResults) == 0:
                    print('File with fileId = %d did not pass filtering test. fqsAttributeQuery returned no results for fileQualitySummaryName = %s' % (fileId, attribute))
                    return False
                else:
                    raise Exception("prisTest failed: fqsAttributeQuery returned an unexpected number of results (%d). Test condition was: %s" % (len(fqsAttributeQueryResults), testCondition))
        else:
            raise Exception("Bad prisTest: invalid test category: %s" % testCategory)
        
        # Check whether or not testResult matches the acceptable list or range of values.
        if testType == "list":
            acceptableValues = testValue.split(",")
            
            for acceptableValue in acceptableValues:
                # Attempt to cast each element of acceptableValues to the same type as the testResult.
                # If the cast fails, then the result is not a match, but not necessary a misconfigured prisTest.
                castedAcceptableValue = None
                try:
                    castedAcceptableValue = type(testResult)(acceptableValue)
                except ValueError as err:
                    print("prisTest: Warning: attempt to cast the acceptableValue %s to the same type as the testResult %s (type %s) failed." % (str(acceptableValue), str(testResult), str(type(testResult))))
                else:
                    if castedAcceptableValue == testResult:
                        return True
            
            # If the code has reached this point, then the testResult did not match any of the acceptable values.
            print('File with fileId = %d did not pass filtering test. The value of %s:%s is %s, which did not match any of the acceptable values, %s' % (fileId, testCategory, attribute, str(testResult), testValue))
            return False
        elif testType == "range":
            rangeBounds = testValue.split(",")
            
            if len(rangeBounds) == 2:
                try:
                    lowerBound = type(testResult)(rangeBounds[0])
                    upperBound = type(testResult)(rangeBounds[1])
                except ValueError:
                    print("File with fileId = %d did not pass filtering test. At least one of the bounds of the range test condition \"%s\" could not be cast to the same type as the test result, which was %s (type %s)." % (fileId, testCondition, str(testResult), str(type(testResult))))
                    return False
                else:
                    if lowerBound <= testResult and testResult <= upperBound:
                        return True
                    else:
                        print('File with fileId = %d did not pass filtering test. The value of %s:%s is %s, which is outside the acceptable range, [%s, %s]' % (fileId, testCategory, attribute, str(testResult), str(lowerBound), str(upperBound)))
                        return False
            else:
                raise Exception("Bad prisTest: invalid number of values supplied for 'range' test. Test condition was: %s" % testCondition)
        else:
            raise Exception("Bad prisTest: invalid test type: %s" % testType)
    # end if len(testParts) == 4
    else:
        raise Exception("Bad prisTest: a test condition had wrong number of parts. Bad test condition was: %s" % testCondition)


def deleteJobSpecParameters(databaseConnection, prodPartialJobId):
    """
    Deletes all JOBSPECPARAMETERS related to the given production job spec.
    
    databaseConnection is assumed to be an active database connection, with a transaction in progress (or not yet started). This function will not call commit on that databaseConnection, nor close it.
    """
    print('Removing job spec parameters with prodPartialJobId = %d' % prodPartialJobId)
    
    removeJobSpecParametersTimer = factoryEPGCommon.CodeSegmentTimer('Removing job spec parameters')
    
    with databaseConnection.cursor() as databaseCursor:
        databaseCursor.execute("""
            DELETE FROM
                JOBSPECPARAMETERS
            WHERE
                PRODPARTIALJOBID = %(prodPartialJobId)s
        """, {'prodPartialJobId': prodPartialJobId})
        
    removeJobSpecParametersTimer.finishAndPrintTime()


def setProductionJobSpecCompletionStatus(databaseConnection, prodPartialJobId, newPjsCompletionStatus):
    """
    Sets the PJSCOMPLETIONSTATUS of the given production job spec to newPjsCompletionStatus.
    
    databaseConnection is assumed to be an active database connection, with a transaction in progress (or not yet started). This function will not call commit on that databaseConnection, nor close it.
    """
    print('Updating status of productionJobSpec %d to %s' % (prodPartialJobId, newPjsCompletionStatus))
    
    with databaseConnection.cursor() as databaseCursor:
        databaseCursor.execute("""
            UPDATE
                PRODUCTIONJOBSPEC
            SET
                PJSCOMPLETIONSTATUS = %(pjsCompletionStatus)s
            WHERE
                PRODPARTIALJOBID = %(prodPartialJobId)s
        """, {'prodPartialJobId': prodPartialJobId, 'pjsCompletionStatus': newPjsCompletionStatus})


def createNewProductionJob(databaseConnection, prodPartialJobId):
    """
    Inserts a new production job into the PRODUCTIONJOB table of the database.
    Returns the prJobId of the new job.
    
    databaseConnection is assumed to be an active database connection, with a transaction in progress (or not yet started). This function will not call commit on that databaseConnection, nor close it.
    """
    print('Creating production job for productionJobSpec %d' % prodPartialJobId)
    
    with databaseConnection.cursor() as databaseCursor:
        databaseCursor.execute("""
            INSERT INTO
                PRODUCTIONJOB (PRJOBID, PRODPARTIALJOBID, PRJOBENQUEUETIME, PRJOBSTATUS)
                VALUES (nextval(%(prJobIdSequence)s), %(prodPartialJobId)s, now(), 'QUEUED')
            RETURNING
                PRJOBID
        """, {'prJobIdSequence': DATABASE_PRODUCTIONJOB_ID_SEQUENCE, 'prodPartialJobId': prodPartialJobId})
        
        results = databaseCursor.fetchall()
        if len(results) == 1:
            return (results[0])[0]
        else:
            raise Exception("Inserting a new production job returned an unexpected number of values: %d", len(results))


def createStringSnsMessageAttribute(value):
    """
    Helper function that returns a dict containing the 'DataType' and 'StringValue' elements, to be used in <sns topic>.publish()
    """
    return { 'DataType': "String", 'StringValue': str(value)}


def deleteProductionJob(databaseConnection, prJobId):
    """
    Deletes the row with the specified prJobId from the PRODUCTIONJOB table of the database.
    Assumes that the production job has not yet been run.
    Will not delete rows in PRODUCTIONJOBLOGMESSAGES or PRODUCTIONJOBOUTPUTFILES that reference the specified job, so if either of those exist, the deletion will fail. 
    
    databaseConnection is assumed to be an active database connection, with a transaction in progress (or not yet started). This function will not call commit on that databaseConnection, nor close it.
    """
    print('Removing productionJob %d from the database.' % prJobId)
            
    with databaseConnection.cursor() as databaseCursor:
        databaseCursor.execute("""
            DELETE FROM
                PRODUCTIONJOB
            WHERE
                PRJOBID = %(prJobId)s
        """, {'prJobId': prJobId})


def deleteJobSpecInputs(databaseConnection, prodPartialJobId):
    """
    Deletes all rows in the JOBSPECINPUT table of the database related to the job spec with the specified prodPartialJobId.
    
    databaseConnection is assumed to be an active database connection, with a transaction in progress (or not yet started). This function will not call commit on that databaseConnection, nor close it.
    """
    print('Removing job spec input rows with prodPartialJobId = %d' % prodPartialJobId)
    
    with databaseConnection.cursor() as databaseCursor:
        databaseCursor.execute("""
            DELETE FROM
                JOBSPECINPUT
            WHERE
                PRODPARTIALJOBID = %(prodPartialJobId)s
        """, {'prodPartialJobId': prodPartialJobId})