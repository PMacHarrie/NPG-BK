import psycopg2
import EDMClient as edm
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap, cm

import numpy as np
from netCDF4 import Dataset

from pyproj import Proj, transform
import datetime

#
# Get Input Products
# Range of LSR Inputs
# Range of Match
# Outputs
# Summarize Matches
# Plot
#
#
# Parameters
#
# EDM.product.arrays
# 
# Vector.lsr.parameter search
#
# Match
#  Temporal
#  Spatial
#
# Spatial/Temporal Range of LSRs
# Set of arrays
#

# First up, CONUS images for an hour before end range of LSRs.

# Algorithm Parameters
# lsr.wfo PHI
# lsr.timeStart (Optional)
# lsr.timeEnd   (Optional)
# lsr.eventIdList (Optional)
# inputProducts: MESOSCALE - Bands 9
#
#

pcf_lines = tuple(open("./matchLSR_perf.py.PCF", 'r'))

#print "pcf_lines", pcf_lines


pcf = {
	"working_directory" : None,
	"nde_mode" : None,
	"job_coverage_start" : None,
	"job_coverage_end" : None,
	"mesoProductLevel" : "CMI",
	"mesoProductBand" : None,
	"lsrEndTime" : None,
	"lsrStartTime" : None,
	"wfo" : None,
	"satelliteName" : "G16",
	"fileNamePrefix" : "match_LSR",
	"fileFormat" : "jpg",
	"color" : "plt.cm.binary"
}

for line in pcf_lines:
	x = line.rstrip('\n')
	if x[0] == "#":
		pass
	else:
		key,var = x.split("=")
#		print key,var
		pcf[key]=var


#print pcf

# Read Database

conn_string = "host='nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com' dbname='nde_dev1' user='nde_dev1' password='nde'"
conn = psycopg2.connect(conn_string)
cursor=conn.cursor()

lsrSQL = """
	select 
		lsrId,
		st_X(eventLocationPoint::geometry), 
		st_Y(eventLocationPoint::geometry), 
		eventtype,
		eventtime,
		eventJson->>'city',
		eventJson from localstormreport where eventJson->>'wfo' = %s and eventTime between %s and %s
"""




# Figure out which products have been passed in


mesoScale1Product = "ABI_L2_CMIPM1_C" + pcf["mesoProductBand"]
mesoScale2Product = "ABI_L2_CMIPM2_C" + pcf["mesoProductBand"]

print mesoScale1Product, mesoScale2Product
cursor.execute(lsrSQL, (pcf['wfo'],pcf['lsrStartTime'], pcf['lsrEndTime']))
rows=cursor.fetchall()

events = {}
for row in rows:
	print row[0], row[1], row[2], row[3], row[4], row[5]
	events[row[0]] = { "lon": row[1], "lat": row[2], "type" : row[3], "time" : row[4], "city" : row[5] }
#	print "time=", events[row[0]]["time"]
#	print "time type=", type(events[row[0]]["time"])

lsrSQL = """
select
        wfo, fileid, fileName, mp, fileEndTime, productid
from
(
select
        wfo,
        min(eventTime) eventStartTime,
        max(eventTime) eventEndTime,
        count(*),
        Box2d( st_collect(eventLocationPoint::geometry) ) mP
from
(
        select
                eventType, eventTime, eventLocationPoint, eventJson->>'wfo' wfo, eventJson->>'city', eventJson->>'st'
        from localstormreport
        where
                eventJson->>'wfo' = %s
                and eventTime between %s and %s
) t
group by
        wfo
)       t2,
        fileMetadata f
where
        f.fileEndTime between t2.eventStartTime - interval '7' minute and t2.eventEndTime + interval '2' minute
        and st_intersects(fileSpatialArea, st_geogFromText(st_asewkt(mp))) = True
	and productId in (select productId from productDescription where productShortName = %s or productShortName = %s)
order by
	productid, fileName
	
"""

#print "lsrSQL=", lsrSQL

cursor.execute(lsrSQL, (pcf['wfo'],pcf['lsrStartTime'],pcf['lsrEndTime'], mesoScale1Product, mesoScale2Product))
rows=cursor.fetchall()
myPngs=[]
i=0
print datetime.datetime.now()

# If mesoscale views are overlapping, pick one.

useThisFirstProductId = None

for row in rows:
	i += 1
	print row

	if useThisFirstProductId == None:
		useThisFirstProductId = row[5]

	if row[5] == useThisFirstProductId :
		pass
	else:
		continue

#	BOX(-77.05 34.68,-76.73 34.73)
	eventBoundary = row[3]

	eventBoundary=eventBoundary.replace("BOX(", "")
	eventBoundary=eventBoundary.replace(")", "")
	eventBoundary=eventBoundary.replace(" ", ",")

	eventCoords = eventBoundary.split(",")

	print eventCoords

	event_Lower_Lon = float(eventCoords[0])
	event_Lower_Lat = float(eventCoords[1])
	event_Upper_Lon = float(eventCoords[2])
	event_Upper_Lat = float(eventCoords[3])

# Open the file

	fileName=edm.file_get(row[1])
	nc = Dataset("./" + fileName)
	#print "nc=", nc

# Convert the GOES coordinates to lon/lat

	sat_h = nc.variables['goes_imager_projection'].perspective_point_height
	sat_lon = nc.variables['goes_imager_projection'].longitude_of_projection_origin
	sat_sweep = nc.variables['goes_imager_projection'].sweep_angle_axis

	p1 = Proj(proj='geos', h=sat_h, lon_0=sat_lon, sweep=sat_sweep)	
#	p2 = Proj(init='epsg:4326', proj='latlong')

	X = nc.variables['x'][:] * sat_h
	Y = nc.variables['y'][:] * sat_h

#	X1, Y1 = np.meshgrid(X, Y)


	# Convert X,Y -> lon,lat
	
	lon1, lat1 = p1(X, Y, inverse=True)

	# Match the CMI footprint

	X2, Y2 = np.meshgrid(lon1, lat1)


#	print lon1
#	print lat1
#	X2, Y2 = transform(p1, p2, X1, Y1)
#	lon2, lat2 = p2(X1, Y1, inverse=True)


#	print X2

	print X2.size, X.size

	CMI = nc.variables['CMI'][:]

	print "Base map min/max=", X2.min(), Y2.min(), X2.max(), Y2.max()

# This creates grids and pixelation. (But, so does the full image specified directly below!)
	mapLowerLon = X2.min() if X2.min() > event_Lower_Lon - 4.0 else event_Lower_Lon - 4.0
	mapLowerLat = Y2.min() if Y2.min() > event_Lower_Lat - 4.0 else event_Lower_Lat - 4.0
	mapUpperLon = X2.max() if X2.max() < event_Upper_Lon + 4.0 else event_Upper_Lon + 4.0
	mapUpperLat = Y2.max() if Y2.max() < event_Upper_Lat + 4.0 else event_Upper_Lat + 4.0

#	mapLowerLon = X2.min()
#	mapLowerLat = Y2.min()
#	mapUpperLon = X2.max()
#	mapUpperLat = Y2.max()

	event_Lower_Lat = eventCoords[1]

	print "Before basemap", datetime.datetime.now()

	print "map coords", mapLowerLon, mapLowerLat, mapUpperLon, mapUpperLat

	fig=plt.figure(1, figsize=(12,12))
	ax = fig.add_axes([0.1,0.1,0.8,0.8])
	m = Basemap(
		projection='merc', 
		resolution='i', 
		area_thresh=5000,
		llcrnrlon=mapLowerLon,
		llcrnrlat=mapLowerLat,
		urcrnrlon=mapUpperLon,
		urcrnrlat=mapUpperLat,
		lat_ts= mapLowerLat + ( (mapUpperLat - mapLowerLat) / 2 )
	)

	print "After basemap", datetime.datetime.now()

	#image_lons, image_lats = m(lon2, lat2)

#	fig=plt.figure(3)
#	fig.text( "wtf?")
#	plt.title("GOES 16 Mesoscale Observed Local Storm Reports")
#	print "After figure", datetime.datetime.now()

	m.drawcoastlines()
	m.drawstates()

#	Creating a title before drawing lat/lon will prevent grid lines and labels from being displayed.
	parallels = np.arange(mapLowerLat, mapUpperLat, 1.0)

	print "parallels=", parallels

	m.drawparallels(parallels, labels=[1,0,0,0],fontsize=10)
	meridians = np.arange(mapLowerLon, mapUpperLon, 1.0)
	print "meridians=", meridians
	m.drawmeridians(meridians, labels=[0,0,0,1],fontsize=10)
	
	# Plot image

	if pcf["color"] == "plt.cm.binary":
		mcolormesh = m.pcolormesh(X2, Y2, CMI, latlon=True, cmap=plt.cm.binary, alpha=0.6)
	elif pcf["color"] == "cm.GMT_haxby":
		mcolormesh = m.pcolormesh(X2, Y2, CMI, latlon=True, cmap=cm.GMT_haxby, alpha=0.6)
	else:
		print "invalid color"

#	mcolormesh = m.pcolormesh(X2, Y2, CMI, latlon=True, cmap=pcf["color"], alpha=0.6)
#	mcolormesh = m.pcolormesh(X2, Y2, CMI, latlon=True, cmap=cm.GMT_haxby, alpha=0.6)
#	mcolormesh = m.pcolormesh(X2, Y2, CMI, latlon=True, cmap=plt.cm.binary, alpha=0.6)

	mcbar = m.colorbar(mcolormesh, location='right', pad="5%")
	mcbar.set_label('CMI')

	print "After pcolormesh", datetime.datetime.now()


	# Prepare event plotting

	fileEndTime = row[4]

#	print "fileEndTime = ", fileEndTime
	print "fileEndTime type = ", type(fileEndTime)

#	If the eventTime <= fileTime - 3 minutes, plot event marker orange,
#       If eventTime between fileTime and fileTime + 10 minutes, plot red
#       If eventTime >= fileTime + 10 minutes, plot black


	e_lons=[]
	e_lats=[]
	eventType=[]
	eventColor=[]

	for event in events:
		#print event
		#x1, y1 = p2(events[event]['lon'], events[event]['lat'])
		eventTime = events[event]["time"]
#		print "file=", fileEndTime, "event=",eventTime
		c=fileEndTime - eventTime
#		#print "c=", c
#		#print "c type=", type(c)
		timeDiff = c.days * 86400 + c.seconds
		#print "timeDiff=", timeDiff
#		#print "timeDiff type=", type(timeDiff)

		if timeDiff >= -120 and timeDiff < 0:
			e_lons.append(events[event]['lon'])
			e_lats.append(events[event]['lat'])
			eventType.append("")
			eventColor.append('g')
		elif timeDiff >= 0 and timeDiff <= 120:
			e_lons.append(events[event]['lon'])
			e_lats.append(events[event]['lat'])
			eventType.append(events[event]['type'])
			eventColor.append('r')
		elif timeDiff >=120:
			e_lons.append(events[event]['lon'])
			e_lats.append(events[event]['lat'])
			eventType.append("")
			eventColor.append('b')
		

	# This converts lon/lat -> map's X,Y (Not needed if use latlon=True)

	m_lons, m_lats = m(e_lons, e_lats)


#	print m_lons[1], m_lats[1] , eventType[1]
	#m.plot(m_lons, m_lats, 'bo', markersize=18)


	print "e_*=", e_lons, e_lats , eventType
#	print "e_lons[0]=", e_lons[0]


	# Bogus
	# yet another Basemap bug! (YABB) scatter/plot must have a minimum of 3 points!
	# BBOOOOOGGGGGUUUUUUSSSSSS!!

	if len(e_lons) == 1:
		e_lons.append(e_lons[0])
		e_lats.append(e_lats[0])
		e_lons.append(e_lons[0])
		e_lats.append(e_lats[0])
		eventColor.append('r')
		eventColor.append('r')
		eventType.append(eventType[0])
		eventType.append(eventType[0])
	elif len(e_lons) == 2:
		e_lats.append(e_lats[0])
		e_lons.append(e_lons[0])
		eventColor.append('r')
		eventType.append(eventType[0])

	print "Before scatter pcolormesh", datetime.datetime.now()

	mText = None

	if len(e_lons) > 0:
		print e_lons[0], e_lats[0] , eventType[0], eventColor[0]
#		print m_lons[0], m_lats[0] , eventType[0], eventColor[0]
		print "e_*=", e_lons, e_lats , eventType
		scat = m.scatter(e_lons, e_lats, marker='o', color=eventColor, latlon=True)
#		scat.legend()
		#m.scatter(e_lons, e_lats, marker='o', color='r', latlon=True)

#		m.plot(e_lons, e_lats, marker='o', color=eventColor, latlon=True)

	# Label Storm Reports, text doesn't take lon,lat. Convert to map's X,Y
		m_lons, m_lats = m(e_lons, e_lats)
		for i in range(len(m_lons)):
		#	print "i=", i
			if eventType[i] == "":
				pass
			else:
				mText = plt.text( m_lons[i], m_lats[i], eventType[i] )

	plt.title("G16 + LSR:" + fileName)

	print "Before savefig", datetime.datetime.now()
	fileParts=fileName.split("_")
	outFileName = pcf["fileNamePrefix"] + "_" + pcf["satelliteName"] + "_" + fileParts[3] + "_" + fileParts[4] + "_" + fileParts[5] + "." + pcf['fileFormat']
	print outFileName
	plt.savefig(outFileName)
	print "After savefig", datetime.datetime.now()

	# Clear out the text labels
	plt.close()

#	m.imshow(np.flipud(CMI))

	myPngs.append(outFileName)
	
#	if i >= 1:
#		break
	# Write to PSF
	with open('./matchLSR_perf.py.PSF', 'a') as psfile:
		psfile.writelines(outFileName + '\n')

print myPngs
