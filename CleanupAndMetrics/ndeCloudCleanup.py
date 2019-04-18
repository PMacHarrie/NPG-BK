import boto3
from datetime import datetime
from dateutil import tz
import psycopg2
import os
import sys

# Redirect sysout/syserr to log file
myPID = str(os.getpid())

logFileName = 'cleanup.' + myPID + '.log'
logf = open (logFileName, 'w')
sys.stdout = sys.stderr = logf

conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# Some products share home dirctories, get max of retention period 

cursor.execute("select productHomeDirectory, max(productretentionperiodhr) + 24 from productDescription group by 1 order by 1")

conn.commit()
res=cursor.fetchall()

#print (res)
productDict = {}
for prod in res:
	#print('a', prod[0],':',prod[1])
	productDict[prod[0]]=prod[1]
	#print('b', productDict[prod[0]])


s3_dict = {}


s3 = boto3.resource('s3')

response = s3.Bucket('ndepg').objects.all()

print ("Cleaning up (i/) incoming_input > 5 days old")
i=0

to_zone = tz.tzlocal()

utc  = datetime.now()

utc=utc.replace(tzinfo=tz.tzutc())
currentDT = utc.astimezone(to_zone)

print (currentDT.tzinfo)

for object in response:
	i += 1
	#print(object.key)
	#print("object=", object)
	#print("content_type", object.last_modified)
	#print("metadata", inner.metadata)
	#print("content_type", inner.content_type)
	#print("content_length", inner.content_length)

	folder = object.key.rpartition('/')[0]
        #print "folder=", folder,  productDict[folder], object.key
	if not(folder in s3_dict):
		s3_dict[folder] = {
			'cnt' :  0,
			'size' :  object.size,
			'oldest object' :  object.last_modified
		}
	else:
		s3_dict[folder]['size'] += object.size
		s3_dict[folder]['cnt'] += 1
		if s3_dict[folder]['oldest object'] > object.last_modified:
			s3_dict[folder]['oldest object'] = object.last_modified


	if object.key.endswith('/'):
		continue

	y=currentDT.date()
	z=object.last_modified
	z.replace(tzinfo=None)	

	age = currentDT - z
	#print ("age=",age)
	#print ("age=",age.total_seconds())
	ageHoursF = age.total_seconds() / 3600.0
	#print ("age=", ageHoursF)
	objectAgeHr = int(age.total_seconds() / 3600.0)
	#print ("age=", objectAgeHr)

	if object.key.startswith("i/"):
		#print(object.key)
		ii_TargetAgeHr = 120
		if objectAgeHr > ii_TargetAgeHr:
			print("(i/) incoming_input: deleting...", object.key)
			print("Target=",y, "Object=",z)
			object.delete()
	elif "forensics_data" in object.key:
		#print(object.key)
		fd_TargetAgeHr = 120
		if objectAgeHr > fd_TargetAgeHr:
			print("forensics_date: deleting...", object.key)
			print("Target=",y, "Object=",z)
			object.delete()
	else:
            #print(object.key)
	    #print("product: deleting...", object.key)
            objs = object.key.split('/')
            homeDir = objs[0] 
            print "homeDir=", homeDir
            #print "objs=", objs
            if homeDir in productDict:
                targetAgeHr = productDict[homeDir]
                print "target=", targetAgeHr, "objAge=", objectAgeHr
                if objectAgeHr > targetAgeHr:
                    print("Target=",targetAgeHr, "Object=",objectAgeHr)
                    print("last_modified", object.last_modified)
                    print("product: deleting...", object.key)
                    object.delete()
#            else:
                # Unknown product delete 
#                print "unknown product is being deleted from ", homeDir
#                print ("product: deleting...", object.key)
#                print ("last_modified", object.last_modified)
#                object.delete()
            else:
		print ("Skipped object",object.key)


print "Peak S3 Inventory"

print "folder,Count,Size,Oldest object\n"
for x in s3_dict:
        print x + ',' + str(s3_dict[x]['cnt']) + ',' + str(s3_dict[x]['size']) + ',' + s3_dict[x]['oldest object'].strftime("%Y/%m/%d %H:%M:%S")

#print "Cleanup database. "

#sql="select fileId into cleanupTemp from fileMetadata where fileInsertTime <= now() - interval '30' day";
#print(sql)
#cursor.execute(sql)
#conn.commit()
#print("Created cleanup temp=", cursor.rowcount)

#sql="delete from filequalitysummary where fileId in (select fileid from cleanupTemp)"
#print(sql)
#cursor.execute(sql)
#print("fqs deleted rows=", cursor.rowcount)
#conn.commit()
#cursor.execute("delete from jobspecinput  where fileId in (select fileid from cleanupTemp)")
#print("jsi deleted rows=", cursor.rowcount)
#conn.commit()
#cursor.execute("delete from fileMetadata where fileId in (select fileid from cleanupTemp)")
#print("fm deleted rows=", cursor.rowcount)
#conn.commit()
#sql="drop table cleanupTemp";
#print(sql)
#cursor.execute(sql)
#conn.commit()
#print("dropped cleanup temp=", cursor.rowcount)

logf.close()


