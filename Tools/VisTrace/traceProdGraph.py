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
    
    dev1_conn = psycopg2.connect(dev1_conn_string)
    cur = dev1_conn.cursor()
    
    print("\n")
    
    nodes = {}
    edges = {}

    getJobDetails(cur, nodes, edges)
    
    print(nodes.values())
    print(edges.values())
    
    cur.close()
    dev1_conn.close()



def getJobDetails(cur, nodes, edges):

    productIds = []
    print("Getting nodes")
    cur.execute("""SELECT
         pd.productId,
         pd.productShortName,
         pd.productType || '-' || pd.productSubType,
         COALESCE( 
            (SELECT p.platformname 
              FROM productplatform_xref ppx, platform p 
              WHERE ppx.productid = pd.productId 
                AND ppx.platformid = p.platformid
            ), 'N/A'
          ) as prodplatform
       FROM productdescription pd
       """)
    rows = cur.fetchall()
    
    for row in rows:
        print(row)
        pi, psn, pt, pp = row
        
        if pp == "SNPP":
            productIds.append(pi)
            
            prodNode = {
                'id': pi,
                'label': psn,
                # 'title': "<b>%s</b><br/>Product Id: %s<br/>Product Type: %s<br/>Platform: %s" % (psn, str(pi), pt, pp),
                'group': pp
            }
            
            nodes[pi] = prodNode

    
    
    print("Getting edges")
    cur.execute("""SELECT
         pr.prId,
         pr.prRuleName,
         prip.productId as inProd,
         pros.productId as outProd
       FROM productionrule pr, prinputspec pris, prinputproduct prip, proutputspec pros
       WHERE pr.prid = pris.prid
         and pris.prisid = prip.prisid
         and pr.prid = pros.prid
         and (prip.productId in %s OR pros.productId in%s)
         """ , (tuple(productIds), tuple(productIds)) )
    rows = cur.fetchall()
    
    for row in rows:
        print(row)
        prid, prn, ip, op = row
        
        edge = {
            'from': ip,
            'to': op,
            # 'title': "(" + str(prid) + ") " + prn
            # 'color': {
            #     'color': '#388E3C'
            # }
        }
        edges[str(ip) + "_" + str(op)] = edge

if __name__ == "__main__":
    myMain()

