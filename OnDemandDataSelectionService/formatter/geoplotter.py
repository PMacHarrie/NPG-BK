#!/usr/bin/env python2.7
import os
import numpy as np
from netCDF4 import Dataset
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap, cm
import h5py

from pyproj import Proj, transform

from datetime import datetime, timedelta

h5 = h5py.File('goes16_conus_latlon_east.h5', 'r')

print "This is geoPlotter.py"

PSF = open ('dss.pl.PSF', 'r')

myFileName = PSF.readline().rstrip()

print "myFile=", myFileName + "<<<<"

PSF.close()

nc = Dataset(myFileName, 'r')

#print "nc=", nc

#np.set_printoptions(threshold=np.nan, nanstr='nan')

#print nc.variables['t']

add_seconds = nc.variables['t'][:]

#print "add_seconds=", add_seconds, "type=", type(add_seconds)

DATE = datetime(2000, 1, 1, 12) + timedelta(seconds=add_seconds)

print "DATE=", DATE

for b in [2, 3, 1]:
	print "b=", b
	varName = 'band_wavelength_c%02d' % b
#	print "varName=", varName
#	print "%s is %s %s" % (nc.variables['band_wavelength_C%02d' % b].long_name,
#	                       nc.variables['band_wavelength_C%02d' % b][0],
#	                       nc.variables['band_wavelength_C%02d' % b].units)


R = nc.variables['CMI_C02'][:].data
G = nc.variables['CMI_C03'][:].data
B = nc.variables['CMI_C01'][:].data

#print "R type=", type(R)
#print "shape", R.shape, R

#print "x=", nc.variables['x']


#print "x data=", nc.variables['x'][:]



# Turn fill values to nan
R[R==-1] = np.nan
G[G==-1] = np.nan
B[B==-1] = np.nan



# Apply range limits, 0 < RGBs < 1
R = np.maximum(R, 0)
R = np.minimum(R, 1)
G = np.maximum(G, 0)
G = np.minimum(G, 1)
B = np.maximum(B, 0)
B = np.minimum(B, 1)

# Apply gamma correction
gamma=0.4
R = np.power(R, gamma)
G = np.power(G, gamma)
B = np.power(B, gamma)

#print R


# Calculate the "True" Green
G_true = 0.48358168 * R + 0.45706946 * B + 0.06038137 * G
G_true = np.maximum(G_true, 0)
G_true = np.minimum(G_true, 1)

# The final RGB array :)
RGB = np.dstack([R, G_true, B])

#print RGB

#plt.figure(figsize=[12,12])
#plt.imshow(RGB)
#plt.savefig('test.png')

#plt.title('GOES-16 From OUTER SPACE!', fontweight='bold', fontsize=12)
#plt.title('%s' % DATE.strftime('%Y-%M-%dT%H%M%S UTC'))


print nc.variables['goes_imager_projection']

sat_h = nc.variables['goes_imager_projection'].perspective_point_height
sat_lon = nc.variables['goes_imager_projection'].longitude_of_projection_origin
sat_lat = nc.variables['goes_imager_projection'].latitude_of_projection_origin
sat_sweep = nc.variables['goes_imager_projection'].sweep_angle_axis

x=np.float64(nc.variables['x'][:])
y=np.float64(nc.variables['y'][:])


#print type(sat_h), 'sat h type'

X = x * sat_h
Y = y * sat_h

#print X, type(X[0])

#thisMap = Basemap(projection='geos', lon_0=sat_lon,
#	resolution='i', area_thresh=5000,
#	llcrnrx=X.min(), llcrnry=Y.min(),
#	urcrnrx=X.max(), urcrnry=Y.max())

#plt.figure(figsize=[15,12])

#thisMap.imshow(np.flipud(RGB)) # Images are upsize down, so flip
#thisMap.drawcoastlines()
#thisMap.drawcountries()

#plt.title('GOES-16 From OUTER SPACE!', fontweight='bold', fontsize=12)
#plt.title('%s' % DATE.strftime('%Y-%M-%dT%H%M%S UTC'))
#plt.savefig('test.png')

print "sat_h=",sat_h
print "sat_lon=", sat_lon
print "sat_sweep=", sat_sweep

#util.py:    + ' +h=%f' % metadata['perspective_point_height'] \
#util.py:    + ' +a=%f' % metadata['semi_major_axis'] \
#util.py:    + ' +b=%f' % metadata['semi_minor_axis'] \
#util.py:    + ' +lon_0=%f' % metadata['longitude_of_projection_origin'] \
#util.py:    + ' +lat_0=%f' % metadata['latitude_of_projection_origin'] \
#util.py:    + ' +sweep=' + metadata['sweep_angle_axis'] \
#util.py:    + ' +f=%f' % metadata['inverse_flattening'].astype('float')**(-1)

#int32 goes_imager_projection()
#    long_name: GOES-R ABI fixed grid projection
#    grid_mapping_name: geostationary
#    perspective_point_height: 35786023.0
#    semi_major_axis: 6378137.0
#    semi_minor_axis: 6356752.31414
#    inverse_flattening: 298.2572221
#    latitude_of_projection_origin: 0.0
#    longitude_of_projection_origin: -75.0
#    sweep_angle_axis: x

sat_lon = nc.variables['goes_imager_projection'].longitude_of_projection_origin
pph = nc.variables['goes_imager_projection'].perspective_point_height
invflat = nc.variables['goes_imager_projection'].inverse_flattening.astype('float')**(-1)
semiMajorAxis = nc.variables['goes_imager_projection'].semi_major_axis
semiMinorAxis = nc.variables['goes_imager_projection'].semi_minor_axis

#p = Proj(proj='geos', h=sat_h, lon_0=sat_lon, lat_0=sat_lat, sweep=sat_sweep, f=invflat, a=semiMajorAxis, b=semiMinorAxis)
p = Proj(proj='geos', h=sat_h, lon_0=sat_lon, lat_0=sat_lat, sweep=sat_sweep)

print "p=", p

# Convert map points to lat/lon 
XX, YY = np.meshgrid(X, Y)
print "type XX=", type(XX), type(XX[0][0])
exit()

lons, lats = p(XX, YY, inverse=True)

#outP = Proj(proj='lcc', lat_0=25, lat_1=25, lat_2=25, lon_0=-95)
#LCC_FIT_HR, proj4, +proj=lcc +a=6371200.0 +b=6371200.0 +lat_0=25 +lat_1=25 +lat_2=25 +lon_0=-95 +units=m +no_defs, None, None, 400, -400, None, None, ,
#outP = Proj("+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +a=6378137 +b=6378137 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs")

(i, j) = lons.shape

print ("i, j=", i, j)

for j1 in range(j):
	for i1 in range(i):
		print j1, i1, "X|Y", '{:08.4f}'.format(X[i1]), '{:08.4f}'.format(Y[j1]), "Lon|Lat", '{:08.4f}'.format(lons[i1, j1]), '{:08.4f}'.format(lats[i1,j1]), "hdf5 Lon|Lat->", h5['longitude'][i1,j1], h5['latitude'][i1,j1]
		if np.isnan(h5['longitude'][i1, j1]):
			continue
		else:
			lon,lat=p(X[j1], Y[i1], inverse=True)
			print X[i1], Y[j1], "lon/lat=", lon, lat

exit()


lats[np.isnan(R)]=57
lons[np.isnan(R)]=-152
#lats[lats>90.0] = np.nan
#lons[lons>180.0] = np.nan
#lats[lats<-90.0] = np.nan
#lons[lons<-180.0] = np.nan

#lats=h5['latitude'][:]
#lons=h5['longitude'][:]




thisMap = Basemap ( resolution='i', projection='lcc', area_thresh=5000, \
	width=1800*3000, height=1060*3000, \
	lat_1=38.5, lat_2=38.5, \
	lat_0=38.5, lon_0=-97.5)


print "min/max lons/lats=", np.nanmin(lons), np.nanmax(lons), np.nanmin(lats), np.nanmax(lats)
#thisMap = Basemap ( 
#	projection='merc',
#	area_thresh=5000,
#	width=1800*3000, height=1060*3000,
#	llcrnrlon=np.nanmin(lons)-0.5, llcrnrlat=np.nanmin(lats)-0.5,
#	urcrnrlon=np.nanmax(lons)+0.5, urcrnrlat=np.nanmax(lats)+0.5,
#	resolution='i',
#	lat_ts = 45.0
#	)

print "shape XX=", XX.shape
print "shape lons=", lons.shape

print "Done Basemap"

rgb = RGB[:,:-1,:] #Remove one column for pcolormesh

print "rgb=", rgb[0-10:]

colorTuple = rgb.reshape((rgb.shape[0] * rgb.shape[1]), 3) # flatten array

colorTuple = np.insert(colorTuple, 3, 1.0, axis=1) # add alpha channel

plt.figure(figsize=[15, 12])

print "shape colorTuple=", colorTuple.shape, colorTuple[0-10:]

print np.nanmin(R), np.max(R), np.nanmin(colorTuple), np.nanmin(colorTuple)

#newmap=thisMap.pcolormesh(lons, lats, R, cmap=cm.GMT_haxby, linewidth=0, latlon=True)
newmap=thisMap.pcolormesh(lons, lats, R, color=colorTuple, linewidth=0, latlon=True)

print "done newmap=", newmap

newmap.set_array(None) # ???

print "min/max lons/lats=", np.nanmin(lons), np.nanmax(lons), np.nanmin(lats), np.nanmax(lats)


parallels = np.arange(np.nanmin(lats), 65.0 , 10.0)
print "parallels=", parallels

thisMap.drawparallels(parallels, labels=[1,0,0,0],fontsize=10)

meridians = np.arange(np.nanmin(lons), -50.0, 10.0)
print "meridians", meridians

thisMap.drawmeridians(meridians, labels=[0,0,0,1],fontsize=10)

thisMap.drawcoastlines()
thisMap.drawcountries()

#plt.title('GOES-16 From OUTER SPACE!', fontweight='bold', fontsize=12)
#plt.title('%s' % DATE.strftime('%Y-%M-%dT%H%M%S UTC'))
#plt.show()

plt.savefig('test.png')

print "thisMap.proj=", thisMap.projection
exit()


#x = np.ma.masked_array(x, x <= 0.)

#xx,yy = thisMap(lon, lat)

#print "shape xx", xx.shape

#print "x=", x

#thisMap.pcolormesh(lon,lat,x, latlon=True, cmap=cm.GMT_haxby, alpha=0.6)
#thisMap.pcolormesh(lon,lat,x, latlon=True, cmap='binary', alpha=0.6)

#parallels = np.arange(minLat, maxLat, 5.0)
#thisMap.drawparallels(parallels, labels=[1,0,0,0],fontsize=10)

#meridians = np.arange(minLon, maxLon, 5.0)
#thisMap.drawmeridians(meridians, labels=[0,0,0,1],fontsize=10)

#thisMap.drawcoastlines()
#thisMap.drawcountries()

#myFileName = myFileName.replace('nc', 'png')
#plt.savefig(myFileName, transparent=True, transparency=.6)
#plt.show()

 
#print myFileName

#PSF = open ('dss.pl.PSF', 'w')
#PSF.write(myFileName + '\n')
#PSF.write("#END-of-PSF")
#PSF.close()
