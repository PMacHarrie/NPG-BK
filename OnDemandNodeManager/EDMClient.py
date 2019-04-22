import requests
import json
import boto3
#import httplib




################## PRODUCT API CALLS ##################
productGatewayUrl = "https://rtr23qq36i.execute-api.us-east-1.amazonaws.com/Develop/product"
#get specific product
#return product json
def product_get(id):
	requestUrl = productGatewayUrl + "/" + str(id)
	r = requests.get(requestUrl)
	return r.content
#get list of products
#return json list of products
def products_get():
	requestUrl = productGatewayUrl
	r = requests.get(requestUrl)
	return r.content

#create product
#ASK PETER?? Pass in product string
#return product id
def product_create(prodJson):
	payload = "body"
	requestUrl = productGatewayUrl
	r = requests.put(requestUrl, data = prodJson)
	prodId = r.content['body']['productid']
	return prodId

#update product
#return T/F
def product_update(id, prodJson):
	requestUrl = productGatewayUrl + "/" + str(id)
	r = requests.patch(requestUrl, data = prodJson)
	if r.status_code == 200:
		return True
	else:
		return False

#delete product
#return T/F
def product_delete(id):
	requestUrl = productGatewayUrl + "/" + str(id)
	r = requests.delete(requestUrl)
	if r.status_code == 200:
		return True
	else:
		return False

#search product
#return T/F
def product_search(regex):
	params = {
		"matchFilePatern": regex
	}
	requestUrl = productGatewayUrl + "/search"
	r = requests.get(requestUrl, params)
	return json.loads(r.text)

################## FILE API CALLS ##################
fileGatewayUrl = "https://mlf6ufvhl6.execute-api.us-east-1.amazonaws.com/dev_w_cors/file"

#Ingest File (Internal to VPC)
#Possible inputs:
#1) External Reference (GOES-R):
#{
#"body": {
#    "filename": "OR_ABI-L1b-RadM1-M3C06_G16_s20182151743330_e20182151743393_c20182151743435.nc",
#    "filestoragereference": {
#      "bucket": "noaa-goes16",
#      "key": "ABI-L1b-RadM/2018/215/17/OR_ABI-L1b-RadM1-M3C06_G16_s20182151743330_e20182151743393_c20182151743435.nc"
#    }
#  }
#}
#TEST: grab from BIG DATA QUEUE
#
# This file is already in a bucket we can access, so the files in the database will just reference the noaa-goes16 bucket
#
#2) Local File :
#{
#  "body": {
#    "filename": "LOCALFILE",
#    "filestoragereference": {
#        "bucket": "ndepg",
#        "key": "incoming_input/LOCALFILENAMEONLY"
#  	}
#  }
#}
#
# filename should refence a file on the local filesystem
####
def file_ingest(ingestJson):

	fileName = ingestJson["filename"]
	bucket = ingestJson["filestoragereference"]["bucket"]
	key = ingestJson["filestoragereference"]["key"]
	searchResult = product_search(fileName)

	if "productid" not in searchResult:
		return False

	#if file is local, upload to incoming input
	if bucket == "ndepg":
		s3 = boto3.resource('s3')
		s3.meta.client.upload_file(fileName, "ndepg", key)
		fileNameOnly = fileName.split("/")[0]
		ingestJson["body"]["filename"] = fileNameOnly

	#put file to 
	requestUrl = fileGatewayUrl

	r = requests.put(requestUrl, data = json.dumps(ingestJson))
	return r

#Get File
def file_get(id):

	requestUrl = fileGatewayUrl + "/" + str(id)

	#print "requestUrl=",  requestUrl

	try:
		r = requests.get(requestUrl)
	except requests.ConnectionError, e:
		print "EDM:file_get exception caught"
		

	outputFileName = "output"
	redirectURL = None

	#print "r=", r
	if r.history:
		#print "redirected from "
		for resp in r.history:
			redirectURL = r.url

	#print  ">>>>>" + redirectURL + "<<<<<<"
# Example:
# https://ndepg.s3.amazonaws.com/products/VIIRS-IMG-GEO-TC/GITCO_npp_d20180925_t1508152_e1509394_b35813_c20180925154502223336_niic_int.h5?AWSAccessKeyId=ASIA3NHN3KLVAXCPUWSD&Signature=6TK6dTxxeobOMcEYbcTAi6gf3NM%3D&x-amz-security-token=FQoGZXIvYXdzEGQaDLw6vAgwv7Jc5XH%2BzCKHAt5%2BHWdzkksFZdjcngLEMrbBLBzsJV485eiTVyUtlcqREo1RvawHYIZZBRze%2FYjCygrh%2FhfJVr%2FXvIkfKsDe7TyG2UixbbKxzXchKIMQOLhytet%2BMB56hYkbrKluu%2Fe87Z8PY5dxUbNxRIbBeFqKG9APPeislrYNAUkF%2BRVG%2Bw%2FjQj6977Z5OlfMVemLeeobmoZJD6KodcgEWhA5fyQJeA7b%2FxYgjmE674MLR7EacRLgPRw2vgEX1pRotVO84o8lIAQcMk6OIy8xrlyrk%2B7ZhRuwQSw35qL%2Fs9aAeP%2F0cZLnCdl0ow11mTMPYv%2F%2BOxQ03CSvlCZ2DgDhmfGZlYPPI52moo%2BJWHbYKOKlr90F&Expires=1537996662
#
# FileName is between the last '/' and the first '?' characters

	urlPrefix = redirectURL.split('?')[0]
	outputFileName = urlPrefix[urlPrefix.rindex('/')+1:]

	#print outputFileName

	with open(outputFileName, 'wb') as f:
		f.write(r.content)

	if r.status_code == 200:
		return outputFileName
	else:
		return False



#Get Internal Array from a file

def file_array_get(id, arrayName):
	requestUrl = fileGatewayUrl + "/" + str(id) + "/array/" + arrayName

	r = requests.get(requestUrl)

	outputFileName = "output"
	redirectURL = None
	if r.history:
		print "redirected from "
		for resp in r.history:
			redirectURL = r.url

	#print  ">>>>>" + redirectURL + "<<<<<<"

	urlPrefix = redirectURL.split('?')[0]
	outputFileName = urlPrefix[urlPrefix.rindex('/')+1:]

	with open(outputFileName, 'wb') as f:
		f.write(r.content)

	if r.status_code == 200:
		return True
	else:
		return False

# get files metadata
def file_metadata_get(id, metaJson):
	metaJson = json.dumps(metaJson)
	requestUrl = fileGatewayUrl + "/" + str(id) + "/" + metaJson
	r = requests.get(requestUrl)

	#TODO FILENAME???
	with open('file_' + str(id) + '_METADATA.json', 'wb') as f:
		f.write(r.content)

	if r.status_code == 200:
		return True
	else:
		return False

#Search Files
def file_search(searchJSON):
	requestUrl = fileGatewayUrl + "/" + searchJSON
	r = requests.get(requestUrl)
	return r.content

