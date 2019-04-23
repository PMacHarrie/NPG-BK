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
dev1_conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"

def myMain():
    
    if len(sys.argv) != 2:
        print("Usage: python3 traceJobChain.py <prId>")
        sys.exit()
    
    dev1_conn = psycopg2.connect(dev1_conn_string)
    cur = dev1_conn.cursor()
    
    prId = int(sys.argv[1])
    
    print("\n")
    
    # fileNodes = {}
    # jobNodes = {}
    nodes = {}
    edges = {}
    visitedRuleIds = []
    
    getJobDetails(prId, cur, nodes, edges, visitedRuleIds)
    
    print(nodes.values())
    print(edges.values())
    
    cur.close()
    dev1_conn.close()



def getJobDetails(prId, cur, nodes, edges, visitedRuleIds):
    
    visitedRuleIds.append(prId)

    inProdIds = []
    outProdIds = []
    
    print("Getting outFiles for: " + str(prId))
    cur.execute("""SELECT
         pr.prId,
         pr.prRuleName,
         pr.prRuleType,
         pd.productId,
         pd.productShortName,
         pd.productType || '-' || pd.productSubType,
         COALESCE( 
                (SELECT p.platformname 
                  FROM productplatform_xref ppx, platform p 
                  WHERE ppx.productid = pd.productId 
                    AND ppx.platformid = p.platformid
                ), 'N/A'
              ) as platform
       FROM productionrule pr, proutputspec pros, productdescription pd 
       WHERE pr.prid = pros.prid
         and pros.productid = pd.productid
         and pr.prid = %s""", (prId,) )
    rows = cur.fetchall()
    
    for row in rows:
        print(row)
        prid, prn, prt, pi, psn, pt, pp = row
        
        ruleNodeId = 'r' + str(prid)
        prodNodeId = 'p' + str(pi)
        outProdIds.append(pi)
        
        if 'wmoHeader' not in prn and 'bundled_nups' not in prn and 'BUFR ' not in prn and 'dss.pl_' not in prn:
            ruleNode = {
                'id': ruleNodeId,
                'label': prn,
                'title': "<b>%s</b><br/>Rule Id: %s<br/>Rule Type: %s" % (prn, str(prid), prt),
                'group': 'rules'
            }
            
            nodes[ruleNodeId] = ruleNode
        
        # prodNode = {
        #     'id': prodNodeId,
        #     'label': psn,
        #     'title': "<b>%s</b><br/>Product Id: %s<br/>Product Type: %s<br/>Platform: %s" % (psn, str(pi), pt, pp),
        #     'group': 'prods'
        # }
        
        # edge = {
        #     'from': ruleNodeId,
        #     'to': prodNodeId,
        #     # 'color': {
        #     #     'color': '#388E3C'
        #     # }
        # }
        
        
        # nodes[prodNodeId] = prodNode
        # edges.append(edge)
        
        # print(jobNode)
        # print(fileNode)
        # print(edge)


    print("Getting inFiles for: " + str(prId))
    cur.execute("""SELECT
         pr.prId,
         pd.productId,
         pd.productShortName,
         pd.productType || '-' || pd.productSubType,
         COALESCE( 
                (SELECT p.platformname 
                  FROM productplatform_xref ppx, platform p 
                  WHERE ppx.productid = pd.productId 
                    AND ppx.platformid = p.platformid
                ), 'N/A'
              ) as platform
       FROM productionrule pr, prinputspec pris, prinputproduct prip, productdescription pd 
       WHERE pr.prid = pris.prid
         and pris.prisid = prip.prisid
         and prip.productid = pd.productid
         and pr.prId = %s""", (prId,) )
    rows = cur.fetchall()
    
    for row in rows:
        print(row)
        prid, pi, psn, pt, pp = row
        
        ruleNodeId = 'r' + str(prid)
        prodNodeId = 'p' + str(pi)
        inProdIds.append(pi)
        
        # prodNode = {
        #     'id': prodNodeId,
        #     'label': psn,
        #     'title': "<b>%s</b><br/>Product Id: %s<br/>Product Type: %s<br/>Platform: %s" % (psn, str(pi), pt, pp),
        #     'group': 'prods'
        # }
        
        # edge = {
        #     'from': prodNodeId,
        #     'to': ruleNodeId,
        #     # 'color': {
        #     #     'color': '#F57C00'
        #     # }
        # }
        # # nodes[prodNodeId] = prodNode
        # edges.append(edge)
        
        
    print("Getting parent/child Jobs for: " + str(prId))
    # print("inFileIds", inFileIds)
    # print("outFileIds", outFileIds)
    
    query1 = "SELECT distinct pr.prId FROM productionrule pr, proutputspec pros " + \
        " WHERE pr.prid = pros.prid and pros.productid in (%s)" % ','.join(str(x) for x in inProdIds)
    print(query1)
    cur.execute(query1)
    rows = cur.fetchall()
    
    for row in rows:
        if 'r' + str(prId) in nodes:
            edge = {
                'from': 'r' + str(row[0]),
                'to': 'r' + str(prId),
            }
            edges[str(prId) + str(row[0])] = edge
        if row[0] not in visitedRuleIds: #and len(visitedRuleIds) <= 100:
            getJobDetails(row[0], cur, nodes, edges, visitedRuleIds)
        
    query2 = "SELECT distinct pr.prId FROM productionrule pr,  prinputspec pris, prinputproduct prip " + \
        " WHERE pr.prid = pris.prid and pris.prisid = prip.prisid and prip.productid in (%s)" % ','.join(str(x) for x in outProdIds)

    print(query2)
    cur.execute(query2)
    rows = cur.fetchall()
    
    for row in rows:
        if 'r' + str(prId) in nodes:
            edge = {
                'from': 'r' + str(prId),
                'to': 'r' + str(row[0]),
            }
        edges[str(row[0]) + str(prId)] = edge
        if row[0] not in visitedRuleIds: #and len(visitedRuleIds) <= 100:
            getJobDetails(row[0], cur, nodes, edges, visitedRuleIds)



if __name__ == "__main__":
    myMain()

