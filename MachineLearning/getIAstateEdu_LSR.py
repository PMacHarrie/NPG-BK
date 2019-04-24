import json
import httplib
import psycopg2
from datetime import datetime, date, time, timedelta
import elasticsearch 

es = {
        "conn" : elasticsearch.Elasticsearch(
            ["https://vpc-nde-dev2-jnk45bdfrsochphfpjinhf6dxu.us-east-1.es.amazonaws.com:443"],
            verify_certs=False
            ),
        "arn" : "arn:aws:es:us-east-1:784330347242:domain/nde-dev2"
}

x=datetime.now()

print "dt=", x
print "dt=", datetime.strftime(x, '%Y%m%d%H%M')

# Look back 6 hours (will test for duplicates later)

d = timedelta(0, 24*3600, 0)

print d

y =  x - d

print "dt=", datetime.strftime(y, '%Y%m%d%H%M')

sts = datetime.strftime(y, '%Y%m%d%H%M')
ets = datetime.strftime(x, '%Y%m%d%H%M')

print "sts, ets=", sts, ets

#https://mesonet.agron.iastate.edu/geojson/lsr.php?sts=201811120000\&ets=201811121400\&wfos=

hconn = httplib.HTTPSConnection("mesonet.agron.iastate.edu")
#hconn = httplib.HTTPSConnection("nwschat.weather.gov")


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

#print json.dumps(lsrJ, indent=3)

#for x in lsrJ['features']:
#	print x
#	print "********"



# iastate edu format
#{
#	u'geometry': {
#		u'type': u'Point', u'coordinates': [-92.75, 38.01]
#	}, 
#	u'type': u'Feature', 
#	u'id': 2105, 
#	u'properties': {
#		u'city': u'CAMDENTON', 
#		u'remark': u'ABOUT 2.5 INCHES HERE NORTH OF CAMDENTON.', 
##		u'wfo': u'SGF', 
#		u'valid': u'2018-11-12T23:39:00Z', 
#		u'lon': -92.75, 
#		u'st': u'MO', 
#		u'county': u'CAMDEN', 
#		u'source': u'AMATEUR RADIO', 
#		u'magnitude': u'2.5', 
#		u'lat': 38.01, 
#		u'prodlinks': u'N/A', 
#		u'type': u'S', 
#		u'typetext': u'SNOW'
#	}
#}


# nwschat format
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


#"psql --host=ndedw.cnvddtnhoxko.us-east-1.redshift.amazonaws.com --port=5439 --username=ndedw"

#psql --host=ndedw.cnvddtnhoxko.us-east-1.redshift.amazonaws.com --port=5439 --username=ndedw
#conn_string = "host='ndedw.cnvddtnhoxko.us-east-1.redshift.amazonaws.com'  user='ndedw' port='5439' password='Ndedw~1!'"
conn_string ="host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"

conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

#    {
#      "geometry": {
#        "type": "Point",
#        "coordinates": [
#          "-108.53",
#          "39.09"
#        ]
#      },
#      "type": "Feature",
#      "id": 157,
#      "properties": {
#        "city": "Grand Junction",
#        "remark": "",
#        "wfo": "GJT",
#        "valid": "2018-10-05 18:30",
#        "lon": "-108.53",
#        "st": "CO",
#        "county": "Mesa",
#        "source": "public",
#        "magnitude": "0.25",
#        "lat": "39.09",
#        "prodlinks": "N/A",
#        "type": "H",
#        "typetext": "HAIL"
#      }
#    }
#
#create table localstormreport (
#eventype varchar(255),
#eventSpatail spatial,
#eventtime timeStamp,
#event magnitude varchar(255),
#eventjson json
#);

sql="insert into localstormreport values(nextval('s_lsrid'), %s, %s, %s, st_geographyfromtext('SRID=4326;POINT(%s %s)'), %s)"

for x in lsrJ['features']:
	myJSONdoc = {}
	print "x=", x
	myDocId = x['properties']['valid']+x['properties']['type']+str(x['properties']['lon'])+str(x['properties']['lat'])+x['properties']['wfo']+x['properties']['type']
	myJSONdoc=x['properties']
	myJSONdoc['location']= { "lat" : x['properties']['lat'], "lon": x['properties']['lon'] }
	print json.dumps(myJSONdoc, indent=2)
#        res = es['conn'].index(index="lsr", doc_type="_doc", id=myDocId, body=myJSONdoc, request_timeout=20)

	eType = x['properties']['typetext']
	eLat = x['properties']['lat']
	eLon = x['properties']['lon']
	eTime = x['properties']['valid']
	eMag = x['properties']['magnitude']

	# Is this a duplicate report (run every hour looking back four hours)

	dupSql = ("select count(*) from localstormreport where eventType = %s and eventTime = %s and eventMagnitude = %s and ST_EQUALS(eventLocationPoint::geometry, st_geographyfromtext('SRID=4326;POINT(%s %s)')::geometry ) = True ")
#	dupSql = ("select (eventLocationPoint = st_geographyfromtext('SRID=4326;POINT(%s %s)')) = True from localstormreport where eventType = %s and eventTime = %s and eventMagnitude = %s")
	cursor.execute(dupSql, (eType, eTime, eMag, float(eLon), float(eLat)) )
#	cursor.execute(dupSql, (float(eLon), float(eLat), eType, eTime, eMag ))
	row=cursor.fetchone()
	print ("row", row[0])

	if row[0] >= 1:
		print "Duplicate storm report, .. skipping."
	else:
		cursor.execute(sql, (eType, eTime, eMag, float(eLon), float(eLat), json.dumps(x['properties'])))

	conn.commit()	
	
#print json.dumps(lsrJ, indent=2)

