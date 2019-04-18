import boto3
import socket
import json
import subprocess


ec2 = boto3.client("ec2", region_name='us-east-1')

# Get Resource Name

hostIP = socket.gethostname()
hostIP = hostIP.replace('ip-', '')
hostIP = hostIP.replace('.ec2.internal', '')
hostIP = hostIP.replace('-', '.')
#print "hostIP=", hostIP
response = ec2.describe_instances(
        Filters=[
                 {
                        "Name" : "private-ip-address",
                        "Values" : [hostIP]
                 }
        ]
)
#print response['Reservations'][0]['Instances'][0]['InstanceId'] + "_" + response['Reservations'][0]['Instances'][0]['PrivateIpAddress'] + "_" + response['Reservations'][0]['Instances'][0]['InstanceType']


# Get Metric (CPU and Disk Utilization)

instanceInfo = {
	"heartbeat" : 1,
	"name" : response['Reservations'][0]['Instances'][0]['InstanceId'],
	"ip" : response['Reservations'][0]['Instances'][0]['PrivateIpAddress'],
	"type" : response['Reservations'][0]['Instances'][0]['InstanceType'],
	"cpu%" : 0.0,
	"disk" : {}
}

diskUtil = subprocess.check_output(["sar", "-dp", "5", "1"])
#print "diskUtil=", diskUtil


for line in diskUtil.split('\n'):
	#print line
	if "Average:" in line:
		if "%util" in line:
			pass
		else:
			#print line
			vals = line.split()
			#print vals
			instanceInfo['disk'][vals[1]]= { 
				'prettyName' : "", 
				'util%' : vals[9],
				"used%" : None
			 }

#print instanceInfo


diskInfo = subprocess.check_output(["df", "-m"])
#print "diskInfo=", diskInfo

for line in diskInfo.split('\n'):
	#print line
	if "Filesystem" in line or len(line) == 0:
		pass
	else:
		vals = line.split()
		#print vals
		devName = vals[0].replace("/dev/", "")
		if devName in instanceInfo["disk"]:
			instanceInfo["disk"][devName]["prettyName"]=vals[5]
			instanceInfo["disk"][devName]["used%"]=vals[4]
			#print "x", vals
			

cpuUtil = subprocess.check_output(["sar", "-u", "5", "1"])

#print "cpuUtil=", cpuUtil

# Example Output
# line= 03:29:11 PM     CPU     %user     %nice   %system   %iowait    %steal     %idle <<<
#line= 03:29:16 PM     all      0.00      0.00      0.00      0.00      0.00    100.00 <<<
#line= Average:        all      0.00      0.00      0.00      0.00      0.00    100.00 <<<

# Get 100.0 - Average %idle

for line in cpuUtil.split('\n'):
	#print "line=", line, "<<<"
	if "Average:" in line:
		#print "line=", line, "<<<"
		vals = line.split()
		#print vals
		instanceInfo["cpu%"]= 100.0 - float(vals[7])
		#print "util=", cpuUtil_Val

print json.dumps(instanceInfo)
