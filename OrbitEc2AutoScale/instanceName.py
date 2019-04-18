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
print response['Reservations'][0]['Instances'][0]['InstanceId']
# + "_" + response['Reservations'][0]['Instances'][0]['PrivateIpAddress'] + "_" + response['Reservations'][0]['Instances'][0]['InstanceType']

