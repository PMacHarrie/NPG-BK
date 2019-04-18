import boto3
import datetime
import psycopg2

# Author Peter MacHarrie

print "Cleanup database. "

conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
conn = psycopg2.connect(conn_string)

conn.set_session(autocommit=True)

cursor = conn.cursor()

#sql="delete from filequalitysummary where fileId in (select fileid from fileMetadata where fileinserttime <= now() - interval '15' day)"
#print(sql)
#cursor.execute(sql)
#print("fqs deleted rows=", cursor.rowcount)
#conn.commit()



# Create Temp table

sql="drop  table if exists  cleanupTemp2"
print(sql)
cursor.execute(sql)
print("drop table cleanupTemp2=", cursor.rowcount)
conn.commit()


sql="select prodPartialJobId into cleanupTemp2 from productionJobSpec where pjsTimeoutTime <= now() - interval '9' day"
print(sql)
cursor.execute(sql)
print("Created Temp Table")
conn.commit()

cursor.execute(
	"delete from jobSpecParameters where prodPartialJobId in ( select prodPartialJobId from cleanupTemp2 )" )
print("jsp deleted rows=", cursor.rowcount)
conn.commit()

cursor.execute(
	"delete from jobSpecInput where prodPartialJobId in (select prodPartialJobId from cleanupTemp2 )" )
print("jsi deleted rows=", cursor.rowcount)
conn.commit()

cursor.execute(
	"delete from productionJobLogMessages where prJobId in (select prJobId from productionJob j where prodPartialJobId in (select prodPartialJobId from cleanupTemp2))" )
print("jlm deleted rows=", cursor.rowcount)
conn.commit()

cursor.execute(
	"delete from productionjoboutputfiles where prJobId in (select prJobId from productionJob j where prodPartialJobId in (select prodPartialJobId from cleanupTemp2))" )
print("jof deleted rows=", cursor.rowcount)
conn.commit()

cursor.execute(
	"delete from productionjob where prodPartialJobId in (select prodPartialJobId from cleanupTemp2 )" )
print("pj deleted rows=", cursor.rowcount)
conn.commit()

cursor.execute( "delete from productionjobspec where prodPartialJobId  in (select prodPartialJobId from cleanuptemp2)" )
print("pjs deleted rows=", cursor.rowcount)
conn.commit()





# Done with jobs clear temp table and delete from fileMetadata

cursor.execute("truncate table cleanuptemp2");
conn.commit()

cursor.execute("""
insert into cleanuptemp2
select fileId from fileMetadata f
where
fileInsertTime <= now() - interval '8' day
and fileInsertTime <= now() - interval '1' hour * (select productretentionperiodhr from productDescription where productId = f.productId)
and exists (select 1 from jobSpecInput where fileId = f.fileId limit 1) = False
""")
conn.commit()

cursor.execute("delete from fileMetadata where fileId in (select prodPartialJobId from cleanuptemp2)")
print("fm deleted rows=", cursor.rowcount)
conn.commit()

cursor.execute("truncate table cleanuptemp2");
conn.commit()




# Assume files are cleaned up after their retention period

cursor.execute("insert into cleanuptemp2 select fileId from fileMetadata f where fileDeletedFlag='N' and fileInsertTime <= now() - interval '1' hour * (select productretentionperiodhr from productDescription p where p.productId = f.productId)")
cursor.execute("update fileMetadata set fileDeletedFlag = 'Y' where fileid in (select prodPartialJobId from cleanuptemp2)")

conn.commit()

print "updated fileDeletedFlag='N'"



# More aggressive Cleanup of jobSpecParameters (If job completed more than 1 hour ago, delete jobSpecParameters)

cursor.execute("truncate table cleanuptemp2");
conn.commit()


sql="insert into cleanupTemp2 select prodPartialJobId from productionJob where prJobStatus in ('BLED', 'FAILED', 'FAILED-CPIN', 'PURGED', 'COMPLETE') and prJobCompletionTime  <= now() - interval '1' day"
print(sql)
cursor.execute(sql)
print("insert productionJob rows into temp=", cursor.rowcount)
conn.commit()

cursor.execute(
	"delete from jobSpecParameters where prodPartialJobId in ( select prodPartialJobId from cleanupTemp2 )" )
print("jsp deleted rows=", cursor.rowcount)
conn.commit()





# Cleanup IngestLog

cursor.execute(
	"delete from ingestlog where rowInsertTime <= now() - interval '7' day" )
print("il deleted rows=", cursor.rowcount)
conn.commit()

cursor.execute(
	"delete from npg_metric where measurementTime <= now() - interval '7' day" )
print("npg metric deleted rows=", cursor.rowcount)
conn.commit()


cursor.execute("drop table cleanuptemp2")
print("drop cleanuptemp2=", cursor.rowcount)
conn.commit()



# Vacuum

print "Vacuuming"

cursor.execute("vacuum analyze jobSpecParameters")
conn.commit()

cursor.execute("vacuum analyze jobSpecInput")
conn.commit()

cursor.execute("vacuum analyze productionJob")
conn.commit()

cursor.execute("vacuum analyze productionJobSpec")
conn.commit()

cursor.execute("vacuum analyze productionJobOutputFiles")
conn.commit()

cursor.execute("vacuum analyze productionJobLogMessages")
conn.commit()

cursor.execute("vacuum analyze fileMetadata")
conn.commit()

cursor.execute("vacuum ingestlog")
conn.commit()

cursor.execute("vacuum analyze productionrule")
conn.commit()

cursor.execute("vacuum analyze jobtemp")
conn.commit()

cursor.execute("vacuum analyze potjobtemp")
conn.commit()

cursor.execute("vacuum analyze potjobtemp2")
conn.commit()







print "Cleaning up nde-dw."

conn_string = "host='nde-dw.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dw' user='nde_dw' password='nde'"
conn2 = psycopg2.connect(conn_string)

conn2.set_session(autocommit=True)

cursor2 = conn2.cursor()

cursor2.execute("delete from if_interfaceevent where if_interfacerequestpolljobenqueue <= now() - interval '20' day")
conn2.commit()

cursor2.execute("vacuum analyze if_interfaceevent")
conn2.commit()

cursor2.execute("delete from if_objectevent where if_filereceiptcompletiontime <= now() - interval '20' day")
conn2.commit()

cursor2.execute("vacuum analyze if_objectevent")
conn2.commit()

print "Done."
