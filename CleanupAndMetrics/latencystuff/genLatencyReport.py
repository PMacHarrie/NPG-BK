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
timeColNames = ['lastInputMessageCreateTime', 'lastInputOrigCreateTime', 
                'lastInputDetectedTime', 'lastInputSourceCreateTime', 'lastInputReceiptTime']

def myMain():
    
    numDays = 7
    datesForQuery = []
    dateRangeStr = ''

    if len(sys.argv) != 4:
        print("""
           Usage: python3 genLatencyReport.py [<from> <to> as YYYY-MM-DD_HH24:MI:SS] timeStats OR 
                  python3 genLatencyReport.py days <num of days ago> timeStats
           where timestats = comma separated list (no spaces) of any values in [all,lastInputMessageCreateTime,  
                lastInputOrigCreateTime,lastInputDetectedTime,lastInputSourceCreateTime,lastInputReceiptTime]""")
        sys.exit()
        
    if sys.argv[1] == 'days':
        numDays = sys.argv[2]
        if not numDays.isdigit() or int(numDays) < 1:
            print("Invalid argument for days: " + numDays)
            sys.exit()
        todaysDate = datetime.today().strftime('%Y-%m-%d')
        dateRangeStr = todaysDate + "_to_" + numDays + "daysAgo"
        for x in range(int(numDays), -1, -1):
            datesForQuery.append("to_timestamp('%s', 'YYYY-MM-DD') - interval '%s' day" % (todaysDate, x) )
    else:
        fromDtArg = sys.argv[1] 
        toDtArg   = sys.argv[2]
        dateRangeStr = fromDtArg.replace('-','').replace(':','') + "_to_" + toDtArg.replace('-','').replace(':','')
        datesForQuery.append("to_timestamp('%s', 'YYYY-MM-DD_HH24:MI:SS')" % fromDtArg)
        datesForQuery.append("to_timestamp('%s', 'YYYY-MM-DD_HH24:MI:SS')" % toDtArg)

    print(datesForQuery)
    
    if 'all' in sys.argv[3]:
        timeStats = timeColNames;
    else:
        timeStats = [t.strip() for t in sys.argv[3].split(',') if t in timeColNames]
    print(timeStats)
    
    dw_conn = psycopg2.connect(dw_conn_string)
    
    for timeStat in timeStats:
        queryAndCsv(datesForQuery, dateRangeStr, timeStat, dw_conn)
    
    dw_conn.close()
    print("Done")


def queryAndCsv(datesForQuery, dateRangeStr, timeCol, dw_conn):
    
    print("Processing timeColumn: " + timeCol)
    outFile = open('latencySummary_%s_%s.csv' % (dateRangeStr, timeCol), 'w')
    outFile.write("platformname,product,Date,cnt,min,max,avg,stddev,le_6min,r6_7min,r7_8min,r8_9min," +
        "r9_10min,r10_11min,r11_12min,r12_13min,r13_14min,r14_15min,r15_30min,r30_90min,r90_116min," +
        "r116_130min,r130_137min,r137_147min,gte_147min\n")
        
    dw_cur = dw_conn.cursor()
    
    for x in range(len(datesForQuery)-1):
        fromDt = datesForQuery[x]
        toDt = datesForQuery[x+1]
        print("  Getting stats for %s -> %s" % (fromDt, toDt) )
        
        dw_cur.execute("""
            select
              platformname,
              product,
              to_char(date_of_proc, 'YYYY-MM-DD') latDate,
              count(*) cnt,
              min (productlatsec) min,
              max (productlatsec) max,
              round(avg(productlatsec)::numeric, 3) avg,
              trunc(stddev(productlatsec)::numeric, 3) sdev,
              sum( case when productlatsec <  60.0 * 6.0                                    then 1 end) le_6min,
              sum( case when productlatsec >= 60.0 * 6.0   and productlatsec < 60.0 * 7.0   then 1 end) r6_7min,
              sum( case when productlatsec >= 60.0 * 7.0   and productlatsec < 60.0 * 8.0   then 1 end) r7_8min,
              sum( case when productlatsec >= 60.0 * 8.0   and productlatsec < 60.0 * 9.0   then 1 end) r8_9min,
              sum( case when productlatsec >= 60.0 * 9.0   and productlatsec < 60.0 * 10.0  then 1 end) r9_10min,
              sum( case when productlatsec >= 60.0 * 10.0  and productlatsec < 60.0 * 11.0  then 1 end) r10_11min,
              sum( case when productlatsec >= 60.0 * 11.0  and productlatsec < 60.0 * 12.0  then 1 end) r11_12min,
              sum( case when productlatsec >= 60.0 * 12.0  and productlatsec < 60.0 * 13.0  then 1 end) r12_13min,
              sum( case when productlatsec >= 60.0 * 13.0  and productlatsec < 60.0 * 14.0  then 1 end) r13_14min,
              sum( case when productlatsec >= 60.0 * 14.0  and productlatsec < 60.0 * 15.0  then 1 end) r14_15min,
              sum( case when productlatsec >= 60.0 * 15.0  and productlatsec < 60.0 * 30.0  then 1 end) r15_30min,
              sum( case when productlatsec >= 60.0 * 30.0  and productlatsec < 60.0 * 90.0  then 1 end) r30_90min,
              sum( case when productlatsec >= 60.0 * 90.0  and productlatsec < 60.0 * 116.0 then 1 end) r90_116min,
              sum( case when productlatsec >= 60.0 * 116.0 and productlatsec < 60.0 * 130.0 then 1 end) r116_130min,
              sum( case when productlatsec >= 60.0 * 130.0 and productlatsec < 60.0 * 137.0 then 1 end) r130_137min,
              sum( case when productlatsec >= 60.0 * 137.0 and productlatsec < 60.0 * 147.0 then 1 end) r137_147min,
              sum( case when productlatsec >= 60.0 * 147.0                                  then 1 end) gte_147min
            from (
              select
                platformname,
                productShortName || '-' || productType || '-' || productSubType as product,
                fileStartTime date_of_proc,
                extract(epoch from (fileInsertTime - %s)) as productlatsec
              from ingest_log
              where
                fileStartTime >= %s AND
                fileStartTime < %s
            ) t1 
            group by
              platformname,
              product,
              to_char(date_of_proc, 'YYYY-MM-DD')
        """ % (timeCol, fromDt, toDt) )
    
        rows = dw_cur.fetchall()

        print("  Writing out to file...")
        for row in rows:
            outFile.write(','.join('' if col is None else str(col) for col in row) + '\n')
    
    dw_cur.close()
    
    outFile.close()
    

if __name__ == "__main__":
    
    myMain()


