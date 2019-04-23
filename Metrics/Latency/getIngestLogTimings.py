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

timings = {}
timingsBig = {}
statusCounts = {}

def myMain():
    
    reportDate = '2019-03-15'
    
    conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    
    print("running SQL query 1")
    query = "SELECT filename, ilstatus, ilmetrics_json, ilfailurereason FROM ingestlog WHERE " + \
        "rowinserttime >= to_timestamp('" + reportDate + "', 'YYYY-MM-DD') + interval '16' hour AND rowinserttime < to_timestamp('" + reportDate + "', 'YYYY-MM-DD') + interval '24' hour"
    # query = "SELECT filename, ilstatus, ilmetrics_json FROM ingestlog WHERE rowinserttime > now() - interval '12' hour"
    cur.execute(query)
    rows = cur.fetchall()
    print("got data from SQL 1")
    processSqlResults(rows)
    rows = None
    
    print("running SQL query 2")
    query = "SELECT filename, ilstatus, ilmetrics_json, ilfailurereason FROM ingestlog WHERE " + \
        "rowinserttime >= to_timestamp('" + reportDate + "', 'YYYY-MM-DD') + interval '8' hour AND rowinserttime < to_timestamp('" + reportDate + "', 'YYYY-MM-DD') + interval '16' hour"
    # query = "SELECT filename, ilstatus, ilmetrics_json FROM ingestlog WHERE rowinserttime > now() - interval '12' hour"
    cur.execute(query)
    rows = cur.fetchall()
    print("got data from SQL 2")
    processSqlResults(rows)
    rows = None
    
    print("running SQL query 3")
    query = "SELECT filename, ilstatus, ilmetrics_json, ilfailurereason FROM ingestlog WHERE " + \
        "rowinserttime >= to_timestamp('" + reportDate + "', 'YYYY-MM-DD') AND rowinserttime < to_timestamp('" + reportDate + "', 'YYYY-MM-DD') + interval '8' hour"
    # query = "SELECT filename, ilstatus, ilmetrics_json FROM ingestlog WHERE rowinserttime > now() - interval '12' hour"
    cur.execute(query)
    rows = cur.fetchall()
    print("got data from SQL 3")
    processSqlResults(rows)
    rows = None
    
    cur.close()
    conn.close()

    print('=======================')
    print("Summary for " + reportDate)
    print('')
    for status, count in statusCounts.items():
        print("{0:10}  {1:120}".format(count, status))
    print('')
    printSummary(timings)
    print('')
    print('===== BIG INGEST =====')
    printSummary(timingsBig)


def processSqlResults(rows):
    for row in rows:
        filename, status, times, failurereason = row
        # print(filename, status)
        statusKey = status + " >> " + failurereason
        if statusKey in statusCounts:
            statusCounts[statusKey] += 1
        else:
            statusCounts[statusKey] = 1
        
        longestStep = {'msg': None, 'dur': 0}
        for rec in times:
            if rec.get('msg') == 'start':
                continue
            
            # if float(rec.get('dur')) > longestStep.get('dur') and rec.get('msg') != "Total":
            #     longestStep = rec
                
            if status.startswith('BIG'):
                addToTimings(timingsBig, rec)
            else:
                addToTimings(timings, rec)


def addToTimings(dict, timingRec):
    msg = timingRec.get('msg')[:48]
    # print(timingRec.get('dur'), 1 + float(timingRec.get('dur')) )
    if msg in dict:
        dict[msg].append(float(timingRec.get('dur')))
        # print(1 + timingRec.get('dur') )
    else:
        dict[msg] = [float(timingRec.get('dur'))]
        

def printSummary(dict):
    print("{0:50} {1:>8} {2:>11} {3:>11} {4:>11} {5:>11}".format("Operation", "Count", "Min (s)", "Max (s)", "Mean (s)", "StDev (s)") )
    print("{0:50} {1:>8} {2:>11} {3:>11} {4:>11} {5:>11}".format("---------", "-----", "-------", "-------", "--------", "---------") )
    for name, vals in dict.items():
        stDev = 0
        if len(vals) > 1:
            stDev = stdev(vals)
        print("{0:50};{1:8d};{2:11.6f};{3:11.6f};{4:11.6f};{5:11.6f}".format(name, len(vals), min(vals), max(vals), mean(vals), stDev) )

if __name__ == "__main__":
    myMain()

