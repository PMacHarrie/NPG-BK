import boto3

dynamo = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamo.Table('Interface_Ingest_Request')

x = table.scan()
for i in x['Items']:

	print(i)
	table.delete_item(Key={'objectKey' : i['objectKey'],})
