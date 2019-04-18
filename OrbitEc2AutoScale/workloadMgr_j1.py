#
# workloadMgr.py
# NDE Proving Ground workload manager.
# Original Author: Peter MacHarrie
# Date: January 10, 2019
#
#
# Attempted to manage with Autoscale at instance level, but not possible, autoscale will do choseinstances, wrt/ i.e. decrementing desired +/- 1. No control at instance level.
#

import boto3
from botocore.exceptions import ClientError
import json
import psycopg2
import datetime
import time
from dateutil.tz import tzlocal
import os
import sys
from dateutil import tz



# Configuration

pollCycleS=20
pgIsOn=0
resourceDurationMinutes=60

resources = {
	"autoscaleGroups" : {
		"OAS_Reg_J1_8" : {
			"desired" : 0,
			"instances" : {},
			"load_target" : 5,
			"MaxSize" : None
		},
		"OAS_Small_J1_12" : {
			"desired" : 0,
			"instances" : {},
			"load_target" : 2,
			"MaxSize" : None
		},
	}
}

eventTypes = {
	'J1' : {
	}
}

# Environment

mypid_s=str(os.getpid())

asg_client = boto3.client('autoscaling')
ec2_client = boto3.client('ec2')

conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
dbconn = psycopg2.connect(conn_string)




def get_ObsPlan(platformName, site):
	#print "pfn=", platformName
	sql = """
		select 
			orbit, 
			to_char(aos_time, 'MM-DD-YY HH24:MI:SS'), 
			to_char(aos_time + interval '59' minute, 'MM-DD-YY HH24:MI:SS'), 
			extract(epoch from aos_time - now()) 
		from 
			observatorydataplan 
		where 
			platformname = %s
			and site = %s
			and now() between aos_time and aos_time + interval '59' minute 
		order by aos_time
		limit 1
	"""

	cursor = dbconn.cursor()

	cursor.execute(sql, (platformName,site))
	rows = cursor.fetchall()

	obsPlan = {
		"withinWindow" : False,
		"prior"    : None,
		"next"     : None
	}

	#print datetime.datetime.now(), mypid_s, "DEBUG", "rows=", rows

	for row in rows:
		obsPlan["withinWindow"] = True
	        #print "orbit=", row[0], " downLinkTime", row[1], "currentTime=", datetime.datetime.now()
		eventTypes[platformName] = { "windowBegin" : row[1], "windowEnd" : row[2] }

	sql = "select min(to_char(aos_time, 'MM-DD-YY HH24:MI:SS')) from observatorydataplan where platformname = %s and site = %s and aos_time >= now()"
	cursor.execute(sql, (platformName,site))
	row = cursor.fetchone()
	#rint "Next window starts at ", row[0]

	obsPlan["next"] = row[0]

	sql = "select max(to_char(aos_time, 'MM-DD-YY HH24:MI:SS')) from observatorydataplan where platformname = %s and site = %s and aos_time < now()"
	cursor.execute(sql, (platformName,site))
	row = cursor.fetchone()
	#print "Previous window started at ", row[0]
	obsPlan["prior"] = row[0]
	dbconn.commit()

	return obsPlan




def get_asgStatus(asgName):

	startTime = datetime.datetime.now()
	myStatus={
		"instances" : {},
		"DesiredCapacity" : None,
		"MaxSize" : None
	}

	response = asg_client.describe_auto_scaling_groups(
		AutoScalingGroupNames=[
			asgName
		],
		MaxRecords=1
	)
	
	if "AutoScalingGroups" in response:
		if len(response["AutoScalingGroups"]) > 0:
			myStatus["DesiredCapacity"] = response["AutoScalingGroups"][0]["DesiredCapacity"]
			myStatus["MaxSize"] = response["AutoScalingGroups"][0]["MaxSize"]
			if "Instances" in response["AutoScalingGroups"][0]:
				if len(response["AutoScalingGroups"][0]["Instances"]) > 0:
					for instance in response['AutoScalingGroups'][0]['Instances']:
						myStatus["instances"][instance['InstanceId']] = { 
							"HealthStatus" : instance["HealthStatus"],
							"LifecycleState" : instance["LifecycleState"],
						}

	return myStatus



def get_ec2_info(instanceId):

# Turned out not to be useful, since Autoscale manages the instances.

	print "DEBUG", "gettting instance info", instanceId

	launchTime=False

	instanceInfo = ec2_client.describe_instances(
		InstanceIds=[
			instanceId
		]
	)

	if "Reservations" in instanceInfo:
		#print "debug1 ", instanceInfo
		if "Instances" in instanceInfo["Reservations"][0]:
			#print "debug2 "
			launchTime = instanceInfo["Reservations"][0]["Instances"][0]["LaunchTime"]

	return launchTime


# Basic Structure
# Am I configured to process data? 
# Yes.
#   Am I within a window where I need to be processing data?
#   Yes.  
#     Do I already have resources in place?
#     Yes: Pass
#     No:  Start my resources
#   No.
#     If resources are present, scale-in.
# No.
#  Exit. (Currently cost is overriding factor, prototype status.)


# Initialize State

removeL = []
for asg in  resources['autoscaleGroups']:
	#print "asg=", asg
	response = asg_client.describe_auto_scaling_groups(
		AutoScalingGroupNames=[
			asg
		]
	)
#	print "response=", response
	if len(response['AutoScalingGroups']) == 0:
		print datetime.datetime.now(), mypid_s, "ERROR", "auto scale group does not exist:", asg
		removeL.append(asg)
	else:
		resources['autoscaleGroups'][asg]['desired']=response['AutoScalingGroups'][0]['DesiredCapacity']
		resources['autoscaleGroups'][asg]['MaxSize']=response['AutoScalingGroups'][0]['MaxSize']
		for instance in response['AutoScalingGroups'][0]['Instances']:
		#	print "
			resources['autoscaleGroups'][asg]['instances'][instance['InstanceId']] = {
				"LifecycleState" : instance['LifecycleState'],
				"HealthStatus" : instance['HealthStatus']
			}
#			x=get_ec2_info(instance['InstanceId'])
#			print "x=",x
#			if x == False:
#				pass
#			else:
#				resources['autoscaleGroups'][asg]['instances'][instance['InstanceId']]["launchTime"] = x


# Remove asg config not found in AWS.

for i in removeL:
	del resources['autoscaleGroups'][i]

print datetime.datetime.now(), mypid_s, "INFO", "current resource state", resources


# Verify Orbit Schedule is current


sql = """
	select min(aos_time), count(*) from observatorydataplan where platformname = 'J1' and site = 'Svalbard' and aos_time >= now()
	"""

cursor = dbconn.cursor()

cursor.execute(sql)
row = cursor.fetchone()
dbconn.commit()

#print row

if row[1] == 0:
	print  datetime.datetime.now(), mypid_s, "ERROR", "observatory data plan not found, exiting."
	exit()


print  datetime.datetime.now(), mypid_s, "INFO", "workloadMgr state initialized."



# Main Loop

loopy=1
i=0

asgStatus={}

while loopy:
	for asgName in resources["autoscaleGroups"]:
		print "DEBUG asgName=", asgName
		asgStatus=get_asgStatus(asgName)
		print datetime.datetime.now(), mypid_s, "INFO", "autoscale  group:", asgName, asgStatus
		print datetime.datetime.now(), mypid_s, "INFO", "resources status:", resources
		obsPlan = get_ObsPlan('J1', 'Svalbard')
		print datetime.datetime.now(), mypid_s, "INFO", "event status:    ", obsPlan, eventTypes
		if obsPlan["withinWindow"]:
#			print  datetime.datetime.now(), mypid_s, "INFO", asgName, "withinWindow"
			if resources['autoscaleGroups'][asgName]['desired'] >= resources['autoscaleGroups'][asgName]['load_target']:
				# The DesiredCapacity could be higher if changed via AWS Console. Currently assume its intentional, reset to target when outside window.
				print  datetime.datetime.now(), mypid_s, "INFO", asgName, "load_target=",resources['autoscaleGroups'][asg]['load_target'], "current desired=", asgStatus['DesiredCapacity']
			else:
				print  datetime.datetime.now(), mypid_s, "INFO", asgName, "< load_target, scaling out."
				try:
					response = asg_client.set_desired_capacity(
						AutoScalingGroupName=asgName,
						DesiredCapacity=resources["autoscaleGroups"][asgName]["load_target"],
						HonorCooldown=True
					)
				except ClientError as e:
					print  datetime.datetime.now(), mypid_s, "ERROR", asgName, "Failed to set desired capacity, retry next iteration.", e
					continue
				#print "response=", response
				resources['autoscaleGroups'][asgName]['desired'] = resources['autoscaleGroups'][asgName]['load_target']

				# There is a significant time delay between setting DesiredInstance and creation of InstanceData. # Doesn't make it into resources["instances"]
				# Also don't want to wait, since waiting prevents other autoscale groups from scaling in a timely manner.
				#asgStatus=get_asgStatus(asgName)
#				print datetime.datetime.now(), mypid_s, "INFO", "started instances=", asgStatus
#				resources['autoscaleGroups'][asgName]['instances']= asgStatus["instances"]
		else:
#			print  datetime.datetime.now(), mypid_s, "INFO", "not withinWindow"
			if resources['autoscaleGroups'][asgName]['desired'] >= resources['autoscaleGroups'][asgName]['load_target']: 
				response = asg_client.set_desired_capacity(
					AutoScalingGroupName=asgName,
					DesiredCapacity=0,
					HonorCooldown=True
				)
				print datetime.datetime.now(), mypid_s, "INFO", asgName, "scaling in.", "response=", response
				resources['autoscaleGroups'][asgName]['desired'] = 0
				resources['autoscaleGroups'][asgName]['instances'] = {}
			
		sys.stdout.flush()
	# end for asg

	time.sleep(pollCycleS)
#	if i == 100:
#		loopy=0
	i+=1
# end while
