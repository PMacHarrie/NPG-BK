from time import sleep
import json
import os
import sys
#import psycopg2
import datetime
from netCDF4 import Dataset
import netCDF4 as n4
import numpy as np
import pandas as pd
import csv

fileName = sys.argv[1]
print "file=", fileName

nc = Dataset(fileName, mode='r')


#x = n4.chartostring(nc['stationName'][0])
#print x, "<<<"
#print np.char.split(x), "<<<"
#print np.char.join(" ", np.char.split(x)), "<<<"

#exit()

stationId = n4.chartostring(nc['stationId'][:])
#stationName = p.join(" ", np.split(n4.chartostring(nc['stationName'][:])))
stationName = np.char.join(" ", np.char.split(n4.chartostring(nc['stationName'][:])))
stationType = n4.chartostring(nc['stationType'][:])
latitude = nc['latitude'][:]
longitude = nc['longitude'][:]
latitude = nc['latitude'][:]
elevation = nc['elevation'][:]
tv=nc['observationTime']
observationTime = n4.num2date(tv[:], tv.units)
temperature = nc['temperature'][:]
dewpoint = nc['dewpoint'][:]
relHumidity = nc['relHumidity'][:]
stationPressure = nc['stationPressure'][:]
seaLevelPressure = nc['seaLevelPressure'][:]
altimeter = nc['altimeter'][:]
windDir = nc['windDir'][:]
windSpeed = nc['windSpeed'][:]
windGust = nc['windGust'][:]
rawPrecip = nc['rawPrecip'][:]
precipAccum = nc['precipAccum'][:]
precipRate = nc['precipRate'][:]
waterLevel = nc['waterLevel'][:]
skyCvr = n4.chartostring(nc['skyCvr'][:,0])
skyCovLayerBase = nc['skyCovLayerBase'][:,0]
restrictedDataLevel = nc['restrictedDataLevel'][:]
precip1min = nc['precip1min'][:]

df = pd.DataFrame( {
	'stationId' : stationId,
	'stationName' : stationName,
	'stationType' : stationType,
	'latitude' : latitude,
	'longitude' : longitude,
	'elevation' : elevation,
	'observationTime' : observationTime,
	'temperature' : temperature,
	'dewpoint' : dewpoint,
	'relHumidity' : relHumidity,
	'stationPressure' : stationPressure,
	'seaLevelPressure' : seaLevelPressure,
	'altimeter' : altimeter,
	'windDir' : windDir,
	'windSpeed' : windSpeed,
	'windGust' : windGust,
	'rawPrecip' : rawPrecip,
	'precipAccum' : precipAccum,
	'waterLevel' : waterLevel,
	'skyCvr' : skyCvr,
	'skyCovLayerBase' : skyCovLayerBase,
	'restrictedDataLevel' : restrictedDataLevel,
	'precip1min' : precip1min
})

df.to_csv("temp.csv", index=False, na_rep="NULL", quoting=csv.QUOTE_NONNUMERIC, columns=['stationId', 'stationName', 'stationType', 'latitude', 'longitude', 'elevation', 'observationTime', 'temperature', 'dewpoint', 'relHumidity', 'stationPressure', 'seaLevelPressure', 'altimeter', 'windDir', 'windSpeed', 'windGust', 'rawPrecip', 'precipAccum', 'waterLevel', 'skyCvr', 'skyCovLayerBase', 'restrictedDataLevel', 'precip1min'])

exit()

# for x in nc.variables:
#	print 'VarialbeName=', x
#	print 'Variable=', nc[x]
#	print 'VariableContent=', nc[x][:]

# print nc
# print nc['geospatial_lat_lon_extent']

def x():
	 ll = {
	    'lon':  str(nc['geospatial_lat_lon_extent'].getncattr('geospatial_westbound_longitude')),
	    'lat':  str(nc['geospatial_lat_lon_extent'].getncattr('geospatial_southbound_latitude'))
	 }
	 ul = {
	    'lon':  str(nc['geospatial_lat_lon_extent'].getncattr('geospatial_westbound_longitude')),
	    'lat':  str(nc['geospatial_lat_lon_extent'].getncattr('geospatial_northbound_latitude'))
	 }
	 ur = {
	    'lon':  str(nc['geospatial_lat_lon_extent'].getncattr('geospatial_eastbound_longitude')),
	    'lat':  str(nc['geospatial_lat_lon_extent'].getncattr('geospatial_northbound_latitude'))
	 }
	 lr = {
	    'lon':  str(nc['geospatial_lat_lon_extent'].getncattr('geospatial_eastbound_longitude')),
	    'lat':  str(nc['geospatial_lat_lon_extent'].getncattr('geospatial_southbound_latitude'))
	 }
	 fileSpatialArea = 'MULTIPOLYGON(((' + ll['lon'] + ' ' + ll['lat'] + ', ' \
		+ ul['lon'] + ' ' + ul['lat'] + ', ' \
		+ ur['lon'] + ' ' + ur['lat'] + ', ' \
		+ lr['lon'] + ' ' + lr['lat'] + ', ' \
		+ ll['lon'] + ' ' + ll['lat'] \
	        + ')))'

	 print 'fileName=', fileName
	 #print 'bucket=', g16_s3_bucket
	 #print 'folder=', folderName
	 #print 'startTime=', fileStartTime
	 #print 'endTime=', fileEndTime
	 #print "filesize=", fileSize
	 #print "fileSpatial=", fileSpatialArea

	 sql="select productHomeDirectory, productShortName, productId from productDescription where %(fileName)s like productFilenamePattern"
	 cursor.execute(sql, {'fileName': fileName})
	 (productHomeDir, productShortName, productId)=cursor.fetchone()
	 conn.commit()
	 #print 'productHomeDir=', rows[0]

# cursor.execute("insert into goes16 values (nextval('goes16_fileid'), %s, to_timestamp(%s, 'YYYYDDDHH24MISSMS'), to_timestamp(%s, 'YYYYDDDHH24MISSMS'), now(), null, %s, %s)", (objectName,fileStartTime, fileEndTime, messageId, os.getpid()))
	 iSql="""
	 insert into fileMetadata 
	(fileid, productId, fileInsertTime, fileSize, fileName, fileStartTime, fileEndTime, fileDeletedFlag, fileMetadataXML, fileSpatialArea, fileProductSupplementMetadata) values 
	(nextval('s_filemetadata'), %s, now(), %s, %s, to_timestamp(%s, 'YYYYDDDHH24MISSMS'), to_timestamp(%s, 'YYYYDDDHH24MISSMS'), 'N', %s, st_geogfromtext(%s), %s)
	RETURNING fileId
	"""
	 #print iSql
	 fileId=-1
	 try:
		cursor.execute(iSql, (productId, fileSize, fileName, fileStartTime, fileEndTime, g16_s3_bucket + '/' + g16_s3_key, fileSpatialArea, temp[1]))
	 except psycopg2.IntegrityError:
		print "Duplicate Key. No insert."
		conn.rollback()
	 else:
		fileId = cursor.fetchone()[0]
		conn.commit()
	 
	 conn.commit()
	 os.remove(fileName)

	 #fileSpatialArea = 'x'

	 # Hard-coded: Channel 1 radiance data is the trigger for all of the CSPP production rules.
	 triggerFlag = "false"
	 if productShortName == 'ABI-L1b-RadF-C01' or productShortName == 'ABI-L1b-RadC-C01' or productShortName == 'ABI-L1b-RadM1-C01' or productShortName == 'ABI-L1b-RadM2-C01':
	 	triggerFlag = "true"

	 response= sns.publish(
		TopicArn='arn:aws:sns:us-east-1:784330347242:ValidatedProduct',
		Message=fileName,
		Subject='GEOS-16 Incoming File',
		MessageAttributes={
			'triggerFlag'                   : { 'DataType' : 'String', 'StringValue' : triggerFlag },
			'fileId'                        : { 'DataType' : 'String', 'StringValue' : str(fileId) },
			'productId'                     : { 'DataType' : 'String', 'StringValue' : str(productId) },
			'productShortname'              : { 'DataType' : 'String', 'StringValue' : productShortName},
			'fileName'                      : { 'DataType' : 'String', 'StringValue' : fileName},
			'fileStartTime'                 : { 'DataType' : 'String', 'StringValue' : fileStartTime},
			'fileEndTime'                   : { 'DataType' : 'String', 'StringValue' : fileEndTime},
			'fileSpatialArea'               : { 'DataType' : 'String', 'StringValue' : fileSpatialArea},
			'fileDayNightFlag'              : { 'DataType' : 'String', 'StringValue' : 'NULL' },
			'fileProductSupplementMetadata' : { 'DataType' : 'String', 'StringValue' : g16_s3_bucket + '/' + g16_s3_key },
			'productHomeDirectory'          : { 'DataType' : 'String', 'StringValue' : productHomeDir }
		}
	 )
 #print('Deleted:message')


conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
conn = psycopg2.connect(conn_string)
while True:
	howMany = sqs.get_queue_attributes (
		QueueUrl = url,
		AttributeNames=[
			'ApproximateNumberOfMessages'
		]	
	)

	numMessages = howMany['Attributes']['ApproximateNumberOfMessages']
	print('Number of message=', numMessages)

	for i in range(int(numMessages)-1):
		doit(conn)
	print("sleeping...")
	sleep(15)

