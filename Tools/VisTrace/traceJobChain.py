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
    
    if len(sys.argv) != 2:
        print("Usage: python3 traceJobChain.py <prJobId>")
        sys.exit()
    
    dw_conn = psycopg2.connect(dw_conn_string)
    dw_cur = dw_conn.cursor()
    
    prJobId = sys.argv[1]
    
    print("\n")
    
    getJobDetails(prJobId, dw_cur)
    
    dw_cur.close()
    dw_conn.close()



def getJobDetails(prJobId, dw_cur):
    
    inPart = ""
    jobPart = ""
    outPart = ""
    
    inFileIds = []
    
    dw_cur.execute("""SELECT
         jl.prJobId,
         prrulename,
         il.fileid inFileId,
         productshortname,
         if_filemessagecreatetime,
         lastinputmessagecreatetime,
         extract(epoch from (fileinserttime - lastinputmessagecreatetime)) as fileLatency,
         ancestorcount
       FROM ingest_log il, job_log jl, pgs_in_log pil
       WHERE il.fileid = pil.fileid
         and jl.prJobId = pil.prJobId
         and pil.prJobId = %s""", (prJobId,) )
    rows = dw_cur.fetchall()
    
    for row in rows:
        # print(row)
        pji, prn, ifi, psn, mct, limct, fl, ac = row
        inPart += "  In:  {0:<10} {1:40} {2:30} {3:30} {4:10} {5:4}\n".format(ifi, psn, str(mct), str(limct), fl, ac)
        inFileIds.append(ifi)
    
    dw_cur.execute("""SELECT
         jl.prJobId,
         prrulename,
         il.fileid outFileId, 
         productshortname,
         if_filemessagecreatetime,
         lastinputmessagecreatetime,
         extract(epoch from (fileinserttime - lastinputmessagecreatetime)) as fileLatency,
         ancestorcount 
       FROM ingest_log il, job_log jl, pgs_out_log pol 
       WHERE il.fileid = pol.fileid 
         and jl.prJobId = pol.prJobId 
         and pol.prJobId = %s""", (prJobId,) )
    rows = dw_cur.fetchall()
    
    for row in rows:
        # print(row)
        pji, prn, ofi, psn, mct, limct, fl, ac = row
        jobPart = "Job: {0:10} {1:50}".format(pji, prn)
        outPart += " Out:  {0:<10} {1:40} {2:30} {3:30} {4:10} {5:4}\n".format(ofi, psn, str(mct), str(limct), fl, ac)
    
    print(outPart[:-1])
    print("----------------------------------------------")
    print(jobPart)
    print("----------------------------------------------")
    print(inPart)
    
    print("\u2191\u2191\u2191\u2191\u2191\u2191\u2191\u2191\n")
    
    dw_cur.execute("SELECT distinct prJobId FROM pgs_out_log WHERE fileId in %s", (tuple(inFileIds),) )
    rows = dw_cur.fetchall()
    
    for row in rows:
        getJobDetails(row[0], dw_cur)
    

if __name__ == "__main__":
    myMain()

