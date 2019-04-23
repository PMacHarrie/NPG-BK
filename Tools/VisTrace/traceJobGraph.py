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
    
    prJobId = int(sys.argv[1])
    
    print("\n")
    
    # fileNodes = {}
    # jobNodes = {}
    nodes = {}
    edges = []
    visitedJobIds = []
    
    getJobDetails(prJobId, dw_cur, nodes, edges, visitedJobIds, 0)
    
    print(nodes.values())
    print(edges)
    
    dw_cur.close()
    dw_conn.close()



def getJobDetails(prJobId, dw_cur, nodes, edges, visitedJobIds, direction):
    
    visitedJobIds.append(prJobId)

    inFileIds = []
    outFileIds = []
    
    print("Getting outFiles for: " + str(prJobId))
    dw_cur.execute("""SELECT
         jl.prJobId,
         jl.prrulename,
         jl.prJobCompletionTime,
         jl.prJobCompletionTime - prjobstarttime as jobDur,
         jl.prjobworkernode,
         il.fileid, 
         il.productshortname,
         il.producttype || '-' || il.productsubtype as prodType,
         il.filename,
         il.filestarttime,
         il.fileendtime,
         il.fileinserttime,
         il.lastinputmessagecreatetime,
         fileinserttime - lastinputmessagecreatetime as fileLatency,
         ancestorcount 
       FROM ingest_log il, job_log jl, pgs_out_log pol 
       WHERE il.fileid = pol.fileid 
         and jl.prJobId = pol.prJobId 
         and pol.prJobId = %s
       ORDER BY il.lastinputmessagecreatetime""", (prJobId,) )
    rows = dw_cur.fetchall()
    
    for row in rows:
        # print(row)
        pji, prn, pjct, pd, pjwn, fi, psn, pt, fn, fst, fet, fit, limct, fl, ac = row
        
        jobNodeId = 'j' + str(pji)
        fileNodeId = 'f' + str(fi)
        outFileIds.append(fi)
        
        jobNode = {
            'id': jobNodeId,
            'label': prn,
            'title': "<b>%s</b><br/>JobId: %s<br/>CmplTime: %s<br/>Dur: %ss<br/>Node: %s" % (prn, pji, str(pjct), pd.total_seconds(), pjwn),
            'group': 'jobs'
        }
        
        fileNode = {
            'id': fileNodeId,
            'label': psn,
            'title': "<b>%s</b><br/>FileId: %s Type: %s<br/>Filename: %s<br/>InsertTime: %s<br/>LastInputMsgTime: %s<br/>Latency: %ss Ancestors: %s" 
                % (psn, fi, pt, fn, str(fit), str(limct), fl.total_seconds(), ac),
            'group': 'files'
        }
        
        edge = {
            'from': jobNodeId,
            'to': fileNodeId,
            # 'color': {
            #     'color': '#388E3C'
            # }
        }
        
        nodes[fileNodeId] = fileNode
        nodes[jobNodeId] = jobNode
        edges.append(edge)
        
        # print(jobNode)
        # print(fileNode)
        # print(edge)


    print("Getting inFiles for: " + str(prJobId))
    dw_cur.execute("""SELECT
         jl.prJobId,
         il.fileid, 
         il.productshortname,
         il.producttype || '-' || il.productsubtype as prodType,
         il.filename,
         il.filestarttime,
         il.fileendtime,
         il.fileinserttime,
         il.lastinputmessagecreatetime,
         COALESCE(fileinserttime - lastinputmessagecreatetime, interval '0' second) as fileLatency,
         ancestorcount 
       FROM ingest_log il, job_log jl, pgs_in_log pil
       WHERE il.fileid = pil.fileid
         and jl.prJobId = pil.prJobId
         and pil.prJobId = %s""", (prJobId,) )
    rows = dw_cur.fetchall()
    
    for row in rows:
        # print(row)
        pji, fi, psn, pt, fn, fst, fet, fit, limct, fl, ac = row
        
        jobNodeId = 'j' + str(pji)
        fileNodeId = 'f' + str(fi)
        inFileIds.append(fi)
        
        fileNode = {
            'id': fileNodeId,
            'label': psn,
            'title': "<b>FileId: %s</b> <br/>Type: %s<br/>Filename: %s<br/>InsertTime: %s<br/>LastInputMsgTime: %s<br/>Latency: %ss Ancestors: %s" 
                % (fi, pt, fn, str(fit), str(limct), fl.total_seconds(), ac),
            'group': 'files'
        }
        
        edge = {
            'from': fileNodeId,
            'to': jobNodeId,
            # 'color': {
            #     'color': '#F57C00'
            # }
        }
        nodes[fileNodeId] = fileNode
        edges.append(edge)
    
    # print("inFileIds", inFileIds)
    # print("outFileIds", outFileIds)
    
    if direction <= 0:
        print("Getting parent Jobs for: " + str(prJobId))
        query1 = "SELECT distinct prJobId FROM pgs_out_log WHERE fileId in (%s)" % ','.join(str(x) for x in inFileIds)
        dw_cur.execute(query1)
        rows = dw_cur.fetchall()
        
        for row in rows:
            if row[0] not in visitedJobIds and len(visitedJobIds) <= 100:
                getJobDetails(row[0], dw_cur, nodes, edges, visitedJobIds, -1)
        
    if direction >= 0:
        print("Getting child Jobs for: " + str(prJobId))
        if len(outFileIds) > 0:
            query2 = "SELECT distinct prJobId FROM pgs_in_log WHERE fileId in (%s)" % ','.join(str(x) for x in outFileIds)
            dw_cur.execute(query2)
            rows = dw_cur.fetchall()
            
            for row in rows:
                if row[0] not in visitedJobIds and len(visitedJobIds) <= 100:
                    getJobDetails(row[0], dw_cur, nodes, edges, visitedJobIds, 1)
        else:
            print("WARN: Job %s has no output files" % prJobId)
            
    
    



if __name__ == "__main__":
    myMain()

