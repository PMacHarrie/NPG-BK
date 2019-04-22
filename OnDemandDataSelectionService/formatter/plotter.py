#!/usr/bin/env python2.7
import os
#import cartopy.crs as ccrs
import numpy as np
#from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from netCDF4 import Dataset
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap, cm
from scipy import stats

print "This is plotter...."

PSF = open ('dss.pl.PSF', 'r')

myFileName = PSF.readline().rstrip()

print "myFile=", myFileName + "<<<<"

PSF.close()

nc = Dataset(myFileName, 'r')

print "nc=", nc

x = ""
lon = ""
lat = ""

for name in nc.variables:
	print "name=", name
	if "Radiance" in name:
		nc.variables[name].set_auto_maskandscale(False)
		y=nc.variables[name][:]
	#	print "Stats ******"
	#	print stats.describe(y)
	#	print "Stats ******"
		y1=np.ma.masked_array(y, y >= 65228)
		print y1.min(), y1.mean(), y1.max()

		ao = nc.variables[name].getncattr('add_offset')
		sf = nc.variables[name].getncattr('scale_factor')
		maxV = nc.variables[name].getncattr('valid_max')

		print "Scaled *******"
#		print 1, "{0:2.9f}".format(y1.max() / sf + ao)
#		print 2, "{0:2.9f}".format(y1.max() / sf - ao)
		print 3, "{0:2.18f}".format(y1.max() * sf - ao)
		print 4, "{0:2.18f}".format(y1.max() * sf + ao)
#		print 5, "{0:2.9f}".format((y1.max() / sf) + ao)
#		print 6, "{0:2.9f}".format((y1.max() / sf) - ao)
		print 7, "{0:2.18f}".format((y1.max() * sf) + ao)
		print 8, "{0:2.18f}".format((y1.max() * sf) - ao)
#		print 9, "{0:2.9f}".format(y1.max() / (sf + ao))
#		print 10, "{0:2.9f}".format(y1.max() / (sf - ao))
#		print 11, "{0:2.9f}".format(y1.max() * (sf + ao))
#		print 12, "{0:2.9f}".format(y1.max() * (sf - ao))
		print "Scaled *******"
		x=nc.variables[name][:]
		print "ao=", float(ao), type(ao)
		print "sf=", float(sf), type(sf)
		print "x=", type(x), x.shape, x.size, x
		x=(x * sf) +  ao
		#x=(x / sf) +  ao
		#x[x>maxV]=np.nan
		#print "Stats ******"
		#print stats.describe(x)
		#print "Stats ******"
		#y1=np.ma.masked_array(x, x >= 4.0)
		print "x=", x.min(), x.mean(), x.max()
		x[x>4.0]=np.nan
		#print stats.describe(x)
		#print y1
		print "x=", np.nanmin(x), np.nanmean(x), np.nanmax(x)

	if "Longitude" in name:
		nc.variables[name].set_auto_maskandscale(False)
		lon=nc.variables[name][:]
	if "Latitude" in name:
		nc.variables[name].set_auto_maskandscale(False)
		lat=nc.variables[name][:]

np.set_printoptions(threshold=np.nan, nanstr='nan', precision=6)

# Criss-cross nans

print "lat[2311,4120]=", lat[2311,4120]
print "lon[2311,4120]=", lon[2311,4120]
print "x[2311,4120]=", x[2311,4120]

lon[lon <= -179.990] = np.nan
lon[lon >= 179.999] = np.nan
lat[lat <= -90.0] = np.nan

#lon[np.isnan(lat)] = np.nan
#lon[np.isnan(x)] = np.nan

#lat[np.isnan(lon)]  = np.nan
#x[np.isnan(lon)]    = np.nan

minLon = np.nanmin(lon)
maxLon = np.nanmax(lon)
minLat = np.nanmin(lat)
maxLat = np.nanmax(lat)

print '1on min/max', minLon, maxLon
print '1at min/max', minLat, maxLat

i=0
#while i < 1541:
#	j=0
#	while j < 8241:
#		print i, j, x[i][j], lon[i][j], lat[i][j]
#		print i, j, x[i][j], lon[i][j], lat[i][j]
#		j+=1
#	#print "i=", i, "j=", j
#	i+=1

# Find middle location

print "lat.shape[0]=", lat.shape[0], lat.shape[0]/2
print "lat.shape[1]=", lat.shape[1], lat.shape[1]/2
print "lat[2311,4120]=", lat[2311,4120]
print "lon[2311,4120]=", lon[2311,4120]
#exit()

lat_m = lat[lat.shape[0]/2,lat.shape[1]/2]
print "debug"
lon_m = lon[lon.shape[0]/2,lon.shape[1]/2]

minLat = np.nanmin(lat)
maxLat = np.nanmax(lat)

minLon = np.nanmin(lon)
maxLon = np.nanmax(lon)

print "debug"
print "min/max lat/lon", minLat, minLon, maxLat, maxLon
print "Mid Points", lat_m, lon_m
print "x=", np.nanmin(x), np.nanmean(x), np.nanmax(x)

if np.isnan(minLat):
	print "No valid input, night granule?"
	exit()

figure = plt.figure(3, figsize=(60,50))

print "Done figure"

m_projection='merc'

print "lat_m=", lat_m

if minLat >= 50.0 and maxLat >= 77:
	m_projection='npstere'
	print "m_projection=", m_projection
	boundinglat = minLat - 1.0
	#boundinglat = -90.0
	thisMap = Basemap ( 
		projection=m_projection,
		boundinglat=boundinglat,
		lon_0=-89.9
	)
elif minLat <= -77.0 and maxLat <= -50:
	m_projection='spstere'
	print "m_projection=", m_projection
	boundinglat = maxLat + 1.0
	thisMap = Basemap ( 
		projection=m_projection,
		boundinglat=boundinglat,
		lon_0=lon_m
	)
else:
	print "m_projection=", m_projection
	thisMap = Basemap ( 
		projection=m_projection,
		llcrnrlon=minLon,
		llcrnrlat=minLat,
		urcrnrlon=maxLon ,
		urcrnrlat=maxLat ,
		lat_ts=lat_m,
		resolution='l'
		)


print "Done Basemap"

print thisMap.urcrnrlat

#x = np.ma.masked_array(x, x <= 0.)

(i, j) = x.shape

#print "i, j", i, j

masked_lon=np.ma.masked_invalid(lon)
masked_lat=np.ma.masked_invalid(lat)

print np.min(masked_lon), np.mean(masked_lon), np.max(masked_lon)
print np.min(masked_lat), np.mean(masked_lat), np.max(masked_lat)
 
x=np.ma.masked_invalid(x)

print np.min(x), np.mean(x), np.max(x)

thisMap.drawcoastlines()
thisMap.drawcountries()
thisMap.drawstates()

##xx,yy = thisMap(masked_lon, masked_lat)
#print "done xx,yy"
#thisMap.pcolormesh(masked_lon, masked_lat, x[:-1], latlon=True, cmap=cm.GMT_haxby, alpha=0.6)
thisMap.pcolormesh(masked_lon, masked_lat, x[:-1], latlon=True, cmap=plt.cm.binary, alpha=0.6)
#thisMap.pcolormesh(masked_lon, masked_lat, x[:-1], latlon=True, cmap='rainbow', alpha=0.6)
#thisMap.pcolormesh(lon,lat,X, latlon=True, cmap=cm.GMT_haxby, alpha=0.6)
#thisMap.pcolormesh(lon,lat,x, latlon=True, cmap='binary', alpha=0.6)

parallels = np.arange(minLat, maxLat, 5.0)
thisMap.drawparallels(parallels, labels=[1,0,0,0],fontsize=14)

meridians = np.arange(minLon, maxLon, 5.0)
thisMap.drawmeridians(meridians, labels=[0,0,0,1],fontsize=14)

#thisMap.drawcoastlines()
#thisMap.drawcountries()
#thisMap.drawstates()

myFileName = myFileName.replace('nc', 'png')
plt.savefig(myFileName, transparent=True, transparency=.6)
 
print myFileName

PSF = open ('dss.pl.PSF', 'w')
PSF.write(myFileName + '\n')
PSF.write("#END-of-PSF")
PSF.close()

