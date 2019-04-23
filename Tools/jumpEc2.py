'''
Author: Hieu Phung
Date created: 2019-02-15
Python Version: 3.6
'''

import sys
import boto3
import subprocess

parentPid = sys.argv[1]
# print(parentPid)

ec2 = boto3.client('ec2', region_name='us-east-1')

response = ec2.describe_instances(
    Filters=[
        {
            'Name': 'instance-state-name',
            'Values': ['running']
        },
    ],
    # InstanceIds=[
    #     'string',
    # ],
    # DryRun=True|False,
    # MaxResults = 50
    # NextToken='string'
)
#print(response)
ipDict = {}
print("# of instances running:", len(response['Reservations']))
for resv in response['Reservations']:
    instance = resv['Instances'][0]

    networkIf = instance['NetworkInterfaces'][0]
    ip = networkIf['PrivateIpAddresses'][0]['PrivateIpAddress']
    # print(ip)

    tags = instance['Tags']
    for tag in tags:
        if tag['Key'] == 'Name':
            instName = tag['Value']
            # print(instName)
            if instName != 'Jump':
                if instName in ipDict:
                   instName = instName + " ..." + ip.split('.')[-1]
                ipDict[instName] = ip

instNames = ipDict.keys()
# print(instNames)
instNames.sort()

sshDict = {0: 'quit'}

print("{0:3}  {1:60}  {2:>10}".format(" ##", "Instance", "Private IP") )
print("{0:3}  {1:60}  {2:>10}".format(" --", "--------", "----------") )

for idx, instName in enumerate(instNames):
    sshDict[idx + 1] = "ssh -i ~/testuser.pem " + ipDict[instName]
    if 'ubuntu' in instName.lower() or 'JupyterHub' in instName:
        sshDict[idx + 1] = "ssh -i ~/testuser.pem ubuntu@" + ipDict[instName]

    print("{0:>3}  {1:60}  {2:>10}".format(idx + 1, instName, ipDict[instName]) )


print('')
selNum = raw_input("Enter selection (number above) or '0' to quit: ")

while(not selNum.isdigit() or int(selNum) not in sshDict):
    selNum = raw_input("Invalid choice! Enter selection (number above) or '0' to quit: ")

selNum = int(selNum)
if selNum != 0:
    cmd = sshDict[selNum]
    # print(cmd)
    # os.system(cmd)
    with open('jumpEc2.' + parentPid, 'w') as f:
        f.write(cmd)
else:
    with open('jumpEc2.' + parentPid, 'w') as f:
        f.write('')


