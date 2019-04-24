import json
import elasticsearch

es = {
        "conn" : elasticsearch.Elasticsearch(
            ["https://vpc-nde-dev2-jnk45bdfrsochphfpjinhf6dxu.us-east-1.es.amazonaws.com:443"],
            verify_certs=False
            ),
        "arn" : "arn:aws:es:us-east-1:784330347242:domain/nde-dev2"
}

#res = es['conn'].search(index="file", body={"query" : {"match_all" : {} } }, size="9999")
#print res

EDM_DataDictionary = {}

productShortName = "x"
platformName = "y"

def doDatasets(d, productShortName, platformName):
	#print "dataset=", d
	for ds in d:
		#print "product", productShortName, "platform", platformName, "DataSet Name=", ds
		#print d[ds]['datatype']
		#print EDM_DataDictionary[productShortName][platformName][ds]
		if ds in EDM_DataDictionary[productShortName][platformName]:
			pass
		else:
			myShape = "[" + ','.join([str(dim) for dim in d[ds]['shape']]) + "]"
			EDM_DataDictionary[productShortName][platformName][ds]= {
				'datatype' : d[ds]['datatype'],
				'shape'    : myShape
			}
	return None


def doVariables(d, productShortName, platformName):
	#print "variable=", d
	for var in d:
		#print "Variable Name =", var 
		#print "product", productShortName, "platform", platformName, "Variable Name=", var
		#print EDM_DataDictionary[productShortName][platformName]
		if var in EDM_DataDictionary[productShortName][platformName]:
			pass
		else:
			myShape = "[" + ','.join([str(dim) for dim in d[var]['shape']]) + "]"
			EDM_DataDictionary[productShortName][platformName][var] = { 'datatype' : d[var]['datatype'], 'shape' : myShape }
	return None


def traverse(d, productShortName, platformName):
	for k,v in d.iteritems():
		if isinstance(v, dict):
	#		print "k=", k
			if k == 'datasets':
				doDatasets(v, productShortName, platformName)
			elif k == 'variables':
				doVariables(v, productShortName, platformName)
			else:
				traverse(v, productShortName, platformName)
#		else:
			#print "kv=", k, ":",  v
			#print "v=", v, "v type =", type(v)

#			print "{0} : {1}".format(k,v)

def crawl(res):
	for i in res['hits']['hits']:

		productShortName = i['_source']['edmCore']['productShortName']

#		print json.dumps(i['_source']['edmCore']['platformNames'], indent=2)

		if 'platformNames' in i['_source']['edmCore']:
			if isinstance(i['_source']['edmCore']['platformNames'], list):
				if not i['_source']['edmCore']['platformNames']:
					platformName = 'Not specified'
				else:
#					print "True"
					platformName = i['_source']['edmCore']['platformNames'][0]
			else:
#				print "False"
				platformName = i['_source']['edmCore']['platformNames']
		else:
			platformName = 'Not specified'

#		print json.dumps(i['_source']['edmCore']['productShortName'], indent=2)
#		print productShortName
#		print platformName
		#print type( i['_source']['edmCore']['platformNames'])

		EDM_DataDictionary[productShortName]={}
		EDM_DataDictionary[productShortName][platformName]={ 'datatype': None, 'shape' : None}

		if 'objectMetadata' in i['_source']:
			objectMetadata = i['_source']['objectMetadata']
			traverse(objectMetadata, productShortName, platformName)
#	if 'variables'  in i['_source']['objectMetadata']:
#		for x in i['_source']['objectMetadata']['variables']:
#			print "x=", x, i['_source']['objectMetadata']['variables'][x]
#	if 'datasets'  in i['_source']['objectMetadata']:
#		for x in i['_source']['objectMetadata']['datasets']:
#			print "x=", x

i=1
res = es['conn'].search(
		index="file",
		scroll='1m',
		body={"query" : {"match_all" : {} } }, 
		size=2000
	)

scroll_size = res['hits']['total']
crawl(res)
sid = res['_scroll_id']
print "hits=", len(res['hits']['hits'])
myhits = len(res['hits']['hits'])
#myhits = 0
while (myhits > 0):
	res = es['conn'].scroll(scroll_id = sid, scroll='2m')
	sid = res['_scroll_id']
	scroll_size = res['hits']['total']

	#print "sid=", sid, scroll_size
	#print "res=", res

#	for x in res:
#		print "x=", x
	#print "hits=", res['hits']
	i += 1
	crawl(res)
#	if i == 10:
#		break
	myhits = len(res['hits']['hits'])
#	print "hits=", len(res['hits']['hits'])


#print "EDM Data Dictionary=", json.dumps(EDM_DataDictionary, indent=2, sort_keys=True)

for x in EDM_DataDictionary:
	#print "x=", x, EDM_DataDictionary[x]
	for y in EDM_DataDictionary[x]:
	#	print "y=", x, y
		for z in EDM_DataDictionary[x][y]:
			print x + "," + y + "," + z + "," + json.dumps(EDM_DataDictionary[x][y][z])



