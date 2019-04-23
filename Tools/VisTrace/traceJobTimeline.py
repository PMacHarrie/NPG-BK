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

dev1_conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"


def myMain():
    
    # if len(sys.argv) != 2:
    #     print("Usage: python3 traceJobChain.py <prJobId>")
    #     sys.exit()
    
    conn = psycopg2.connect(dev1_conn_string)
    cur = conn.cursor()
    
    # prJobId = int(sys.argv[1])
    
    print("\n")
    
    # fileNodes = {}
    # jobNodes = {}
    nodes = {}
    edges = []
    visitedJobIds = []
    
    getJobDetails(cur, nodes, edges)
    
    print(nodes.values())
    print(edges)
    
    cur.close()
    conn.close()



def getJobDetails(cur, nodes, edges):
    
    inFileIds = []
    outFileIds = []
    
    print("Getting latest files")
    cur.execute("""SELECT
         fileid,
         filename,
         fileinserttime,
         filestarttime,
         fileendtime
       FROM filemetadata 
       WHERE fileinserttime > now() - interval '10' minute
         AND productid = 4755
       """ )
    rows = cur.fetchall()
    
    for row in rows:
        # print(row)
        fi, fn, fit, fst, fet = row
        
        fileNodeId = 'f' + str(fi)
        
        fileNode = {
            'id': fileNodeId,
            'content': str(fi),
            'title': "Filename: %s<br/>InsertTime: %s" % (fn, str(fit)),
            'start': str(fst),
            'end': str(fet)
        }
        
        nodes[fileNodeId] = fileNode
        
        # print(jobNode)
        # print(fileNode)
        # print(edge)
        


if __name__ == "__main__":
    myMain()

