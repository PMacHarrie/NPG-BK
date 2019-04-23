'''
Author: Hieu Phung, Peter MacHarrie
Date created: 2019-02-18
Python Version: 3.6
'''

import os
import sys
import psycopg2
from datetime import datetime


dev1_conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
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
    myPID = str(os.getpid())
    
    dw_conn = psycopg2.connect(dw_conn_string)
    dw_cur = dw_conn.cursor()
    
    dev1_conn = psycopg2.connect(dev1_conn_string)
    dev1_cur = dev1_conn.cursor()
    
    dw_cur.execute("""
        SELECT cfgparametervalue, date_trunc('hour', now() - interval '0' hour)::timestamp
        FROM configurationregistry 
        WHERE cfgparametername = 'last_popmetric_update'
        ORDER BY cfgparametername""")
    row = dw_cur.fetchone()

    fromDt = row[0]
    toDt = row[1]

    print("Gathering data for interval:", fromDt, "->", toDt, "\n")
    
    
    # Do ingest_log
    
    dev1_cur.execute(
        """
        select
            (SELECT p.platformname 
                FROM productplatform_xref ppx, platform p 
                WHERE ppx.productid = pd.productId 
                AND ppx.platformid = p.platformid),
            pd.productId,
            productShortName,
            productType,
            productSubType,
            f.fileId,
            f.fileName,
            f.fileSize,
            to_char(fileInsertTime,'YYYY-MM-DD HH24:MI:SS.MS'),
            to_char(fileStartTime,'YYYY-MM-DD HH24:MI:SS.MS'),
            to_char(fileEndTime,'YYYY-MM-DD HH24:MI:SS.MS'),
            fileBeginOrbitNum,
            fileEndOrbitNum,
            pd.product_time_coverage,
            pd.productFileFormat,
            pd.productCoverageGapInterval_DS
        from
            fileMetadata f,
            productDescription pd
        where
            f.productId = pd.productId
            and fileInsertTime between %s and %s
        order by
            pd.productId,
            fileInsertTime
        """, (fromDt, toDt) )
        
    rows = dev1_cur.fetchall()
    
    print("Got", len(rows), "rows for ingest_log")
    
    ingestFileName = 'popMetricIngest.' + myPID + '.temp'
    
    with open(ingestFileName, 'w') as f:
        for row in rows:
            f.write('~'.join(map(str, row)) + "\n")
    
    print("  Wrote temp file:", ingestFileName)
    
    dw_cur.execute("TRUNCATE TABLE ingest_log_temp")
    
    # print("cat %s | psql -h %s -d %s -U %s -c \"copy ingest_log_temp from stdin with delimiter '~' null 'None'\"" % (ingestFileName, dwh, dwn, dwu))
    # os.system("export PGPASSWORD='%s'; cat %s | psql -h %s -d %s -U %s -c \"copy ingest_log_temp from stdin with delimiter '~' null 'None'\"" % (d, ingestFileName, dwh, dwn, dwu))
    
    with open(ingestFileName, 'r') as f:
        #next(f)  # Skip the header row.
        dw_cur.copy_from(f, 'ingest_log_temp', sep='~', null='None')
    dw_conn.commit()
    
    print("  Finished copy from file into ingest_log_temp")
    
    dw_cur.execute("DELETE FROM ingest_log il USING ingest_log_temp ilt WHERE il.filename = ilt.filename")
    
    print("  Finished ingest_log reconcile")
    
    dw_cur.execute("""INSERT INTO ingest_log (
        SELECT ilt.*,
            case 
                when filename ~ 'c\d{20}[^\d].' then to_timestamp(substring(filename, 'c(\d{20})'), 'YYYYMMDDHH24MISSUS')
                when filename ~ 'c\d{14}\.nc' then to_timestamp(substring(filename, 'c(\d{14})') || '00', 'YYYYDDDHH24MISSMS')
                when filename ~ 'c\d{14}\.bin' then to_timestamp(substring(filename, 'c(\d{14})'), 'YYYYMMDDHH24MISS')
                when filename ~ 'c\d{15}[^\d].' then to_timestamp(substring(filename, 'c(\d{15})') || '00', 'YYYYMMDDHH24MISSMS')
                else null
            end,
            if_filedetectedtime,
            if_filesourcecreationtime,
            if_filereceiptcompletiontime,
            if_filemessagecreatetime,
            case 
                when filename ~ 'c\d{20}[^\d].' then to_timestamp(substring(filename, 'c(\d{20})'), 'YYYYMMDDHH24MISSUS')
                when filename ~ 'c\d{14}\.nc' then to_timestamp(substring(filename, 'c(\d{14})') || '00', 'YYYYDDDHH24MISSMS')
                when filename ~ 'c\d{14}\.bin' then to_timestamp(substring(filename, 'c(\d{14})'), 'YYYYMMDDHH24MISS')
                when filename ~ 'c\d{15}[^\d].' then to_timestamp(substring(filename, 'c(\d{15})') || '00', 'YYYYMMDDHH24MISSMS')
                else null
            end,
            if_filedetectedtime,
            if_filesourcecreationtime,
            if_filereceiptcompletiontime,
            if_filemessagecreatetime,
            0,
            now()
        FROM ingest_log_temp ilt
            LEFT OUTER JOIN if_objectevent ifoe ON ilt.filename = ifoe.if_filename)""")
        
    dw_conn.commit()
    # print(dw_cur.rowcount)
    
    print(">>> Finished ingest_log load <<<")
    
    
    ## Do job_log
    
    dev1_cur.execute(
        """
        SELECT
            prJobId,
            j.prodPartialJobId,
            to_char(prJobEnqueueTime, 'YYYY-MM-DD HH24:MI:SS.MS'),
            to_char(prJobStartTime, 'YYYY-MM-DD HH24:MI:SS.MS'),
            to_char(prJobCompletionTime, 'YYYY-MM-DD HH24:MI:SS.MS'),
            prJobStatus,
            prAlgorithmReturnCode,
            prDataSelectionReturnCode,
            prJobCPU_Util,
            prJobMem_Util,
            prJobIO_Util,
            cloud_worker_node prJobWorkerNode,
            s.jobClass,
            s.jobPriority,
            to_char(pjsStartTime, 'YYYY-MM-DD HH24:MI:SS.MS'),
            to_char(pjsObsStartTime, 'YYYY-MM-DD HH24:MI:SS.MS'),
            to_char(pjsObsEndTime, 'YYYY-MM-DD HH24:MI:SS.MS'),
            pjsCompletionStatus,
            r.prId,
            prRuleName,
            r.algorithmId,
            algorithmName || '-' || algorithmVersion algorithm
        FROM
            productionJobSpec s 
                LEFT OUTER JOIN productionJob j ON s.prodPartialJobId = j.prodPartialJobId
                JOIN productionRule r ON s.prId = r.prId
                JOIN algorithm a ON r.algorithmId = a.algorithmId
        WHERE
            prJobCompletionTime between %s and %s
            or (pjsStartTime between %s and %s
                    and pjsCompletionStatus not in ('INCOMPLETE')
            )
        """, (fromDt, toDt, fromDt, toDt) )
        
    rows = dev1_cur.fetchall()
    
    print("Got", len(rows), "rows for job_log")
    
    jobsFileName = 'popMetricJobs.' + myPID + '.temp'
    
    with open(jobsFileName, 'w') as f:
        for row in rows:
            f.write('~'.join(map(str, row)) + "\n")
    
    print("  Wrote temp file:", jobsFileName)
    
    dw_cur.execute("TRUNCATE TABLE job_log_temp")
    
    with open(jobsFileName, 'r') as f:
        #next(f)  # Skip the header row.
        dw_cur.copy_from(f, 'job_log_temp', sep='~', null='None')
    dw_conn.commit()
    
    print("  Finished copy from file into job_log_temp")

    dw_cur.execute("DELETE FROM job_log jl USING job_log_temp jlt WHERE jl.prodPartialJobId = jlt.prodPartialJobId")
    
    print("  Finished job_log reconcile")
    
    dw_cur.execute("INSERT INTO job_log (SELECT jlt.*, now() FROM job_log_temp jlt)")
    dw_conn.commit()
    
    print(">>> Finished job_log load <<<")
    
    
    # Do pgs_in_log
    
    dev1_cur.execute(
        """
        SELECT
            prJobId,
            fileId,
            prisNeed
        FROM
            productionJob j,
            productionJobSpec s,
            jobSpecInput i,
            prInputProduct ip,
            prInputSpec pris
        WHERE
            j.prodPartialJobId = s.prodPartialJobId
            AND s.prodPartialJobId = i.prodPartialJobId
    	    AND prJobCompletionTime between %s and %s
    	    AND i.pripid = ip.pripid
    	    AND ip.prisId = pris.prisId
        ORDER BY
            prJobId,
            fileId
        """, (fromDt, toDt) )
    rows = dev1_cur.fetchall()
    
    print("Got", len(rows), "rows for pgs_in_log")
    
    pgsInFileName = 'popMetricPgsIn.' + myPID + '.temp'
    
    with open(pgsInFileName, 'w') as f:
        for row in rows:
            f.write('~'.join(map(str, row)) + "\n")
    
    print("  Wrote temp file:", pgsInFileName)
    
    dw_cur.execute("TRUNCATE TABLE pgs_in_log_temp")
    
    with open(pgsInFileName, 'r') as f:
        #next(f)  # Skip the header row.
        dw_cur.copy_from(f, 'pgs_in_log_temp', sep='~', null='None')
    dw_conn.commit()
    
    print("  Finished copy from file into pgs_in_log_temp")
    
    dw_cur.execute("DELETE FROM pgs_in_log pil USING pgs_in_log_temp pilt "
        + "WHERE pil.prJobId = pilt.prJobId and pil.fileId = pilt.fileId")
    
    print("  Finished pgs_in_log reconcile")
    
    dw_cur.execute("INSERT INTO pgs_in_log (SELECT pilt.*, now() FROM pgs_in_log_temp pilt)")
    dw_conn.commit()
    
    print(">>> Finished pgs_in_log load <<<")
    
    
    # Do pgs_out_log
    
    dev1_cur.execute(
        """
        SELECT
            j.prJobId,
            fileId
        FROM
            productionJobOutputFiles o
                LEFT OUTER JOIN filemetadata f ON o.productionJobOutputFileName = f.filename
                JOIN productionJob j ON o.prJobId = j.prJobId
        WHERE
            prJobCompletionTime between %s and %s
        ORDER BY
                j.prJobId,
                fileId
        """, (fromDt, toDt) )
    rows = dev1_cur.fetchall()
    
    print("Got", len(rows), "rows for pgs_out_log")
    
    pgsOutFileName = 'popMetricPgsOut.' + myPID + '.temp'
    
    with open(pgsOutFileName, 'w') as f:
        for row in rows:
            f.write('~'.join(map(str, row)) + "\n")
    
    print("  Wrote temp file:", pgsInFileName)
    
    dw_cur.execute("TRUNCATE TABLE pgs_out_log_temp")
    
    with open(pgsOutFileName, 'r') as f:
        #next(f)  # Skip the header row.
        dw_cur.copy_from(f, 'pgs_out_log_temp', sep='~', null='None')
    dw_conn.commit()
    
    print("  Finished copy from file into pgs_out_log_temp")
    
    dw_cur.execute("DELETE FROM pgs_out_log pol USING pgs_out_log_temp polt "
        + "WHERE pol.prJobId = polt.prJobId and pol.fileId = polt.fileId")
    
    print("  Finished pgs_out_log reconcile")
    
    dw_cur.execute("INSERT INTO pgs_out_log (SELECT polt.*, now() FROM pgs_out_log_temp polt)")
    dw_conn.commit()
    
    print(">>> Finished pgs_in_log load <<<")
    
    dw_cur.execute("UPDATE configurationregistry SET cfgparametervalue = %s " +
        "WHERE cfgparametername = 'last_popmetric_update'", (toDt, ))
    dw_conn.commit()
    print("Updated configurationregistry")
    
    print("Deleting expired DB rows")
    cfgSnippet = "(SELECT cfgparametervalue::int FROM configurationregistry where cfgparametername = 'dbStatisticsExpirationHr')"
    dw_cur.execute("DELETE FROM ingest_log WHERE row_lastupdatetime <= now() - interval '1' hour * " + cfgSnippet)
    dw_cur.execute("DELETE FROM job_log WHERE row_lastupdatetime <= now() - interval '1' hour * " + cfgSnippet)
    dw_cur.execute("DELETE FROM pgs_in_log WHERE rowInsertTime <= now() - interval '1' hour * " + cfgSnippet)
    dw_cur.execute("DELETE FROM pgs_out_log WHERE rowInsertTime <= now() - interval '1' hour * " + cfgSnippet)
    dw_conn.commit()
    
    dw_cur.close()
    dw_conn.close()
    
    # print("Removing temp files")
    # os.remove(ingestFileName)
    # os.remove(jobsFileName)
    # os.remove(pgsInFileName)
    # os.remove(pgsOutFileName)
    
    print("Duration:", (datetime.now()-dt).total_seconds())
    
    
    logFile.close()

if __name__ == "__main__":
    myMain()


