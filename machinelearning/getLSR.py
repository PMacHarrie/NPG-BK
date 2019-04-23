import json
import httplib
import psycopg2
from datetime import datetime, date, time, timedelta

x=datetime.now()

print "dt=", x
print "dt=", datetime.strftime(x, '%Y%m%d%H%M')

d = timedelta(1, 0, 0)

print d

y =  x - d

print "dt=", datetime.strftime(y, '%Y%m%d%H%M')

sts = datetime.strftime(y, '%Y%m%d%H%M')
ets = datetime.strftime(x, '%Y%m%d%H%M')

#print "sts, ets=", sts, ets


hconn = httplib.HTTPSConnection("nwschat.weather.gov")
#hconn.request("GET", "/geojson/lsr.php?inc_ap=no&sts=201810040000&ets=201810042359&wfos=")

requestS = "/geojson/lsr.php?inc_ap=no&sts=" + sts + "&ets=" + ets + "&wfos="
print "request=", requestS

hconn.request("GET", requestS)

#https://nwschat.weather.gov/vtec/#2018-O-NEW-KLZK-SV-W-0221
#https://nwschat.weather.gov/vtec/json-text.php
#uri = ("https://nwschat.weather.gov/vtec/json-text.php?" #           "year=%s&wfo=%s&phenomena=%s&eventid=%s&significance=%s" #           ) % (year, wfo, phenomena, eventid, significance)

h1 = hconn.getresponse()

print "status=", h1.status, "reason=", h1.reason
print "h1=", h1

data1 = h1.read()

#print "data1=", data1

lsrJ = json.loads(data1)

#for x in lsrJ['features']:
#	print x
#	print "********"

#
# {
#	"type":"FeatureCollection",
#	"crs":{
#		"type":"EPSG",
#		"properties":{"code":4326,"coordinate_order":[1,0]}},
#		"features":[
#			{
#				"type":"Feature",
#				"id":0,
#				"properties":{
#					"magnitude":"4.39",
#					"wfo":"JAN",
#					"valid":"2018-07-07 04:10",
#					"type":"R",
#					"county":"Forrest",
#					"typetext":"HEAVY RAIN",
#					"st":"MS",
#					"remark":"rainfall totals at htbm6 since 7pm. the duration of the heavy rain event was 4 hours 10 minutes.",
#					"city":"1 WSW Hattiesburg",
#					"source":"co-op observer",
#					"lat":"31.31",
#					"lon":"-89.33",
#					"prodlinks":"N\/A"},
#				"geometry":{"type":"Point","coordinates":["-89.33","31.31"]}
#			},
#			{
#				"type":"Feature",

#ndedw=# \d localstormreport 
#             Table "public.localstormreport"
#     Column     |            Type             | Modifiers 
#----------------+-----------------------------+-----------
# eventtype      | character varying(255)      | 
# eventlatitude  | double precision            | 
# eventlongitude | double precision            | 
# eventtime      | timestamp without time zone | 
# eventmagnitude | character varying(255)      | 
# eventjson      | character varying(8192)     | 
#



"psql --host=ndedw.cnvddtnhoxko.us-east-1.redshift.amazonaws.com --port=5439 --username=ndedw"

#psql --host=ndedw.cnvddtnhoxko.us-east-1.redshift.amazonaws.com --port=5439 --username=ndedw
conn_string = "host='ndedw.cnvddtnhoxko.us-east-1.redshift.amazonaws.com'  user='ndedw' port='5439' password='Ndedw~1!'"
#conn_string ="host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"

conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

sql="insert into localstormreport values(%s, %s, %s, %s, %s, %s)"

for x in lsrJ['features']:
	print 'x=', x
	eType = x['properties']['typetext']
	eLat = x['properties']['lat']
	eLon = x['properties']['lon']
	eTime = x['properties']['valid']
	eMag = x['properties']['magnitude']
	cursor.execute(sql, (eType, eLat, eLon, eTime, eMag, json.dumps(x)))
	conn.commit()	
	


