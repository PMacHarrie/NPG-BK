import boto3
import json
import os
import sys
import math
import requests
import psycopg2
from datetime import datetime, timedelta

# sqs = boto3.client('sqs', region_name='us-east-1')
# sns = boto3.client('sns', region_name='us-east-1')
s3Resource = boto3.resource('s3')
s3 = boto3.client('s3')

conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
conn = psycopg2.connect(conn_string)


intervalHours = 23

def myMain():

    print("starting")
    
    outFile = open('statusSummary.html', 'w')
    
    outFile.write("""
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="900">
        <title>NDE Status Report Summary</title>
        <style>
            .conDiv {
                border: 1px solid #1B2631;
            }
            .red {
                background-color: #EC7063;
            }
            .orange {
                background-color: #F5B041;
            }
            .yellow {
                background-color: #F9E79F;
            }
            table {
                border-collapse: collapse;
            }
            td, th {
                border-collapse: collapse;
                border: 1px solid #1B2631;
                border-top: 0px;
                padding: 5px 6px;
                word-wrap: break-word;
            }
            th {
                font-weight: normal;
            }
            thead {
            	background: #E5E7E9;
            	display: block;
                overflow-y: scroll;
                overflow-x: hidden;
            }
            tbody {
            	max-height: 300px;
                display: block;
            	overflow: scroll;
            	overflow-x: hidden;
            }
            tbody tr, thead tr {
            	display: table;
            	width: 100%%;
            	table-layout: fixed;
            }
            tbody tr:hover {
                background: #EBF5FB;
            }
            table.incoming th:nth-child(n+2), table.incoming td:nth-child(n+2){
                width: 200px;
            }
            .timeCols th:first-child, .timeCols td:first-child{
                width: 350px;
            }
            table.gap th:nth-child(n+2), table.gap td:nth-child(n+2){
                width: 200px;
            }
        </style>
      </head>
      <body style="padding: 20px;">
      <h2 style="float: left; margin: 0px;">Status Report</h2>
      <h4 style="float: right; margin: 0px;">Report time: %s UTC</h4>
      <div style="clear: both;"></div>
    """ % (datetime.utcnow(), ) )
  
    doIncomingDir(outFile)
    
    obsHr = [] 
    for hr in range (intervalHours+1):
        obsHr.append((datetime.now() - timedelta(hours = hr)).strftime("%I%p") )
    
    print(obsHr)
    
    doProductCounts(obsHr, outFile)
    doJobCounts(obsHr, outFile)
    doGapCheck(outFile)
    
    outFile.write("""
      </body>
    </html>
    """)
    
    outFile.close()
    
    print("uploading to s3")
    s3.upload_file("statusSummary.html", "ndepg-cm", "nde/statusSummary.html")
    
    # os.system('cp statusSummary.html /home/ubuntu/nde-cloud/static/')
    print("done")
    
    

def doIncomingDir(outFile):
    print("doIncomingDir")
    deltaMins = 102
    
    iiLs = s3Resource.Bucket('ndepg').objects.filter(Prefix='i/')
    countTotal = 0
    countOld = 0
    
    # currDt = datetime.utcnow().replace(tzinfo=pytz.utc)
    currDt = datetime.utcnow()
    
    tableData = ""
    for iiObj in iiLs:
        countTotal += 1
        # print(isinstance(iiObj.last_modified, datetime))
        iiObjDt = iiObj.last_modified.replace(tzinfo=None)
        ageDelta = currDt - iiObjDt
        if ageDelta > timedelta(minutes=12):
            countOld += 1
            filename = iiObj.key.rsplit('/', 1)[-1]
            ageHr = math.ceil(ageDelta.total_seconds()/3600)
            tableData += '<tr><td>{0}</td><td>{1}</td><td>{2}</td></tr>\n'.format(filename, iiObjDt, ageHr)
    
    outFile.write('<h3>S3 Incoming Input | Total: ' + str(countTotal) + ' | Old (in table below): ' + str(countOld) + '</h3>\n')
    outFile.write('<div class="conDiv"><table class="incoming"><thead><tr><th>Filename</th><th>Last Modified</th><th>Age (hr)</th></tr></thead>\n')
    outFile.write('<tbody>' + tableData + '</tbody>')
    outFile.write('</table></div>\n')
    
    
def doProductCounts(obsHr, outFile):
    
    query = """
        select * from crosstab (
          $$
            (
                select
                    (pl.platformName || '_' || p.productShortName) Platform_Product,
                    to_char(fileEndTime, 'HHAM') hr,
                    count(*) Cnt
                from 
                    fileMetadata f, productDescription p, productplatform_xref ppx, platform pl
                where 
                    fileEndTime  >= now() - interval '{0}' hour
                    and f.productId = p.productId 
                    and p.productId = ppx.productId 
                    and pl.PLATFORMID = ppx.PLATFORMID
                group by 
                    (pl.platformName || '_' || p.productShortName), 
                    to_char(fileEndTime, 'HHAM')
            )
            union
            (
                select
                    ('ANC__' || p.productShortName) Platform_Product,
                    to_char(fileEndTime, 'HHAM') hr,
                    count(*) Cnt
                from 
                    fileMetadata f, productDescription p 
                where 
                    fileEndTime  >= now() - interval '{1}' hour
                    and f.productId = p.productId 
                    and p.productType = 'ANC' 
                    and f.productId in (select productId from PrInputProduct)
                group by 
                    ('ANC__' || p.productShortName), 
                    to_char(fileEndTime, 'HHAM')
            )
            order by 1
          $$,
          $$ select unnest(string_to_array('{2}', ',')) $$
        ) AS ct ("Platform_Product" text, {3}) 
    """.format(intervalHours, intervalHours, ','.join(obsHr), '_' + ' text, _'.join(obsHr) + ' text')

    print(query)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()

    outFile.write('<h3>Product Ingested Counts</h3>\n')
    outFile.write('''<div class="conDiv">
                        <table class="timeCols"><thead><tr>
                            <th>Platform_Product</th>''')
    for hr in obsHr:
        outFile.write('<th>' + hr + '</th>')
    outFile.write('</tr></thead>\n<tbody>')
    
    for row in rows:
        outFile.write('<tr>')
        for val in row:
            if val is None:
                outFile.write('<td></td>')
            else:
                outFile.write('<td>' + str(val) + '</td>')
        outFile.write('</tr>\n')        
    outFile.write('</tbody></table></div>\n\n\n')
        
        
def doJobCounts(obsHr, outFile):
    
    query = """
        select * from crosstab (
            $$
            select prRuleName, pjsObsHr,
                cast (
                    case
                            when I is null and F is null and E is null then ''||T
                            when I is null and F is null and E is not null then ''||T||' (E='||E||')'
                            when I is null and F is not null and E is null then ''||T||' (F='||F||')'
                            when I is null and F is not null and E is not null then ''||T||' (F='||F||',E='||E||')'
                            when I is not null and F is null and E is null then ''||T||' (I='||I||')'
                            when I is not null and F is null and E is not null then ''||T||' (I='||I||',E='||E||')'
                            when I is not null and F is not null and E is null then ''||T||' (I='||I||',F='||F||')'
                            when I is not null and F is not null and E is not null then ''||T||' (I='||I||',F='||F||', E='||E||')'
                    end  as varchar(30)
                ) Summary
            from (
                select pjsObsHr, t.prRuleName prRuleName,
                    sum(case when JobStatus = 'I' then cnt end) I,
                    sum(case when JobStatus = 'C' then cnt end) C,
                    sum(case when JobStatus = 'F' then cnt end) F,
                    sum(case when JobStatus = 'E' then cnt end) E,
                    sum(cnt) T
                from (
                    select 
                        to_char(pjsObsEndTime, 'HHAM') pjsObsHr, 
                        prRuleName,
                        case 
                            when pjsCompletionStatus not like 'COMPLETE%' and (prJobStatus not in ('COMPLETE', 'FAILED')) then 'I'
                            when pjsCompletionStatus = 'COMPLETE-NOINPUT' then 'C'
                            when prJobStatus not in ('COMPLETE', 'FAILED') then 'I'
                            when prJobStatus = 'COMPLETE' then 'C'
                            when prJobStatus = 'FAILED' then 'F'
                        end JobStatus,
                        count(*) Cnt
                    from PRODUCTIONJOBSPEC s
                        LEFT OUTER JOIN ProductionJob j 
                            ON s.prodPartialJobId = j.PRODPARTIALJOBID
                        INNER JOIN productionRule pr 
                            ON s.prId = pr.prId
                    where 
                        PJSOBSENDTIME > now() - interval '{0}' hour
                    group by 
                        to_char(pjsObsEndTime, 'HHAM'), prRuleName,
                        case when pjsCompletionStatus not like 'COMPLETE%' and (prJobStatus not in ('COMPLETE', 'FAILED')) then 'I'
                        when pjsCompletionStatus = 'COMPLETE-NOINPUT' then 'C'
                        when prJobStatus not in ('COMPLETE', 'FAILED') then 'I'
                        when prJobStatus = 'COMPLETE' then 'C'
                        when prJobStatus = 'FAILED' then 'F' end
                ) t
                group by 
                    t.prRuleName, pjsObsHr
                order by 
                    t.prRuleName, pjsObsHr
            ) t1
            $$,
            $$ select unnest(string_to_array('{1}', ',')) $$
        ) AS ct ("Platform_Product" text, {2}) 
        """.format(intervalHours, ','.join(obsHr), '_' + ' text, _'.join(obsHr) + ' text')
    
    print(query)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    
    outFile.write('<h3>Production Job Counts</h3>\n')
    outFile.write('''<div class="conDiv">
                        <table class="timeCols"><thead><tr>
                            <th>Rule Name</th>''')
    for hr in obsHr:
        outFile.write('<th>' + hr + '</th>')
    outFile.write('</tr></thead>\n<tbody>\n')
    
    for row in rows:
        outFile.write('<tr>')
        for val in row:
            if val is None:
                outFile.write('<td></td>')
            else:
                if 'F=' in val:
                    temp = '<td class="red">'
                elif 'E=' in val:
                    temp = '<td class="yellow">'
                elif 'I=' in val:
                    temp = '<td class="orange">'
                else:
                    temp = '<td>'
                outFile.write(temp + str(val) + '</td>')
        outFile.write('</tr>\n')        
    outFile.write('</tbody></table></div>\n\n\n')
    
    
def doGapCheck(outFile):
    
    print("doGapCheck")
    query = """
        select
            productShortName,
            nextStartTime - t1.fileEndTime GapDuration,
            t1.fileId,
            t1.fileEndTime,
            t1.nextStartTime,
            min(f.fileEndTime) RangeStart,
            max(f.fileEndTime) RangeEnd
        from
            (
                select
                    productId,
                    fileId,
                    fileEndTime,
                    lead(fileStartTime) over (partition by productId order by fileStartTime) nextStartTime
                from
                    fileMetadata t2
                where
                    fileDeletedFlag in ('N') and fileEndTime between now() - interval '306' minute and now() - interval '102' minute
                order by
                    1,4
            ) t1,
            productDescription p,
            fileMetadata f
        where
            t1.nextStartTime - t1.fileEndTime > interval '10' second and
            t1.productId = p.productId
            and p.productId = f.productId
            and f.fileEndTime between now() - interval '306' minute and now() - interval '102' minute
        group by
            productShortName,
            nextStartTime - t1.fileEndTime,
            t1.fileId,
            t1.fileEndTime,
            t1.nextStartTime
        order by 2, 1, 4;
        """
    
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    
    outFile.write('<h3>Gap Check - Previous 2 Orbits</h3>\n')
    outFile.write('''<div class="conDiv">
                        <table class="incoming"><thead><tr>
                            <th>Product</th>
                            <th>Gap Dur (HH:MM:SS.s)</th>
                            <th>Gap Start Time</th>
                            <th>Gap End Time</th>
                            <th>Start Range</th>
                            <th>End Range</th></tr></thead>
                            <tbody>\n''')
    for row in rows:
        outFile.write('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n' % 
            (row[0], row[1], row[3], row[4], row[5], row[6]) )
        
    outFile.write('</tbody></table></div>\n\n')

if __name__ == "__main__":
    myMain()
