'''
Author: Hieu Phung
Date created: 2019-01-07
Python Version: 3.6
'''

import json
import os
import sys
import psycopg2
from datetime import datetime, timedelta
from statistics import mean, stdev

dw_conn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"


def myMain():
    
    myPID = str(os.getpid())
    
    thisScriptName = sys.argv[0][:-3]
    logDir = "metricLogs/"
    
    if not os.path.exists(logDir):
        os.makedirs(logDir)

    logFileName = thisScriptName + "." + myPID + '.log'
    logFile = open(logDir + logFileName, 'w')
    sys.stdout = sys.stderr = logFile
    
    dt = datetime.now()
    
    dw_conn = psycopg2.connect(dw_conn_string)
    dw_cur = dw_conn.cursor()
    
    dw_cur.execute("SELECT cfgparametervalue::timestamp - interval '1' hour FROM configurationregistry " + 
        "WHERE cfgparametername = 'last_filelatency_update'")
    fromDt = dw_cur.fetchone()[0]
    
    dw_cur.execute("SELECT cfgparametervalue::timestamp FROM configurationregistry " + 
        "WHERE cfgparametername = 'last_popmetric_update'")
    toDt = dw_cur.fetchone()[0]
    
    print("Calculating for interval:", fromDt, "->", toDt, "\n")
    
    print("Getting COMPLETE prJobIds")
    
    dw_cur.execute("SELECT prJobId, prJobCompletionTime FROM job_log " +
	    "WHERE prJobStatus = 'COMPLETE' AND prJobCompletionTime between %s and %s " +
        "ORDER BY prJobCompletionTime", (fromDt, toDt))
    rows = dw_cur.fetchall()
    
    print("Got %s rows" % (len(rows),) )
    
    jobsCount = 0
    outFileCount = 0
    
    for row in rows:
        prJobId, prCmpltTime = row
        print(">> Getting recent timings for prJobId: %s (completed @%s)" % (prJobId, prCmpltTime))
        
        jobsCount += 1
        
        dw_cur.execute("""
            SELECT 
                max(lastinputorigcreatetime), 
                max(lastinputdetectedtime),
                max(lastinputsourcecreatetime),
                max(lastinputreceipttime),
                max(lastinputmessagecreatetime),
                max(ancestorcount)
            FROM
                ingest_log il, pgs_in_log pil
            WHERE
                il.fileId = pil.fileId
                and pil.prisneed != 'OPTIONAL'
                and pil.prJobId = %s
        """, (prJobId,) )
        
        row = dw_cur.fetchone()
        # print(row)
        
        lioct, lidt, lisct, lirt, limct, ac = row
        
        if None in row:
            print("WARN: One or more 'last*time' value is null")
        print("  lioct:", lioct, "lidt:", lidt, "lisct:", lisct, "lirt:", lirt, "limct:", limct, "ac:", ac)
        
        dw_cur.execute("""
            UPDATE ingest_log
            SET
                lastinputorigcreatetime = %s,
                lastinputdetectedtime = %s,
                lastinputsourcecreatetime = %s,
                lastinputreceipttime = %s,
                lastinputmessagecreatetime = %s,
                ancestorcount = %s + 1
            WHERE fileId IN (
                SELECT fileId from pgs_out_log WHERE prJobId = %s
            )
        """, (lioct, lidt, lisct, lirt, limct, ac, prJobId) )
        
        rowsUpdated = dw_cur.rowcount
        dw_conn.commit()
        
        if rowsUpdated == 0:
            print("WARN: no output files found for JobId: " + str(prJobId))
        else:
            print("  %s pgs_out files updated" % rowsUpdated)
            outFileCount += rowsUpdated
    
    print("Finished loop. Total Jobs crawled: %s. Total pgs_out files updated: %s" % (jobsCount, outFileCount) )
    
    dw_cur.execute("UPDATE configurationregistry SET cfgparametervalue = %s " +
        "WHERE cfgparametername = 'last_filelatency_update'", (toDt, ))
    dw_conn.commit()
    print("Updated configurationregistry")
    
    
    print("Duration: " + str(datetime.now()-dt))


    logFile.close()
    

if __name__ == "__main__":
    myMain()


