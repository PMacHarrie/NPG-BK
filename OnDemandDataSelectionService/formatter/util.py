#!/usr/bin/env python2.7
'''
20141022, created

@author: dcp

Utilities for remapping and creating images from dss tailored netCDF4 2D data 
 sets. The following utlities are included:
    setuplogger: creates the logging object for all functions
    scaleData: rescales data to defined data type fitting data to valid values
    readPCF: reads parameter values from dss.pl.PCF file
    readPSF: reads the NC4 file name from the dss.pl.PSF
    writePSF: write the final output file name to the PSF 
        (remapped NC4 file, geotiff, or other image format)
    parseProjStr: parses the PCF parameter "grid" string to create grid 
        information dictionary
    makeProjDict: creates projection dictionary for pyresample
    getGEOProj4: retrieves GEO projection information
    readNC4: Opens and reads in the contents of the dss output NC4 file using 
        h5py (HDF5 API)
    
'''
from remap_config import *
import os, sys
import logging
import string
import re
import h5py
import numpy as np
import gzip
import zipfile
import zlib

log = logging.getLogger("formatting")

def setuplogger(workDir):
    """Set up the formatting logger to append to the dss.pl.log file.
        Input: full path to the working directory
        Output: logging object provided to all functions
    """
    FORMAT = '%(asctime)s %(levelname)s: %(filename)s, %(funcName)s: %(message)s'
    logging.basicConfig(filename=workDir + '/dss.pl.log',level=logging.INFO,format=FORMAT)
    log = logging.getLogger("formatting")
    #debug = logging.StreamHandler(sys.stdout)
    #debug.setLevel(logging.DEBUG)
    #log.addHandler(debug)
    error = logging.StreamHandler(sys.stderr)
    error.setLevel(logging.ERROR)
    log.addHandler(error)

def gzipFile(inFile,level=9):
    """GZIP compress a file and save as file name with gz extension
        Parameter:
            inFile: file to be gzipped
            level: compression level (for zlib; can be 1-9), default is 9
    """
    log.info("GZIP compressing file: " + inFile)
    outfile = inFile + '.gz'
    f_in = open(inFile,'rb')
    f_out = gzip.open(outfile,'wb',level)
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

def zipFile(inFile):
    """ZIP compress (zlib) a file and save name with zip extension
        Parameter:
            inFile: file to be gzipped
    """
    log.info("ZIP archiving and zlib compressing file: " + inFile)
    outfile = inFile + '.zip'
    f_out = zipfile.ZipFile(outfile,mode='w')
    f_out.write(inFile,compress_type=zipfile.ZIP_DEFLATED)
    f_out.close()  

def scaleData(dataSet, fillValue, dataType):
    """Linear scale imagery data to defined data types. Uses the 
        minimum/maximum valid data value to scale. Sets fill value to the 
        dataType top of range value as default. Assumes only scaling to 8 or
        16 bit data types.
        Input:
            dataSet: numpy data array
            fillValue: fill value for input data (assumes a single fill value)
            dataType: output numpy data type desired (assumes either 8 or 16 
                bits for imagery)
        Output:
            Returns a rescaled data array as the defined data type with default 
                fill values
    """
    
    log.info("Scaling data to " + dataType)
    if dataSet.dtype.name == dataType:
        log.warning("Data is already cast as specified data type. Not scaling and continuing.")
        return dataSet, fillValue
    
    fill_mask = dataSet == fillValue
    min_in = np.nanmin(dataSet[~fill_mask]).astype('float')
    max_in = np.nanmax(dataSet[~fill_mask]).astype('float')
    min_out = dataRange[dataType][0]
    max_out = dataRange[dataType][1] - 1
    
    m = ( max_out - min_out )  / ( max_in - min_in )
    b = min_out - m * min_in
    
    rescaled = np.multiply(dataSet, m)
    np.add(rescaled, b, rescaled)
    np.clip(rescaled, min_out, max_out, out=rescaled)
    fillValue=dataRange[dataType][1]
    rescaled[fill_mask] = fillValue 
    
    return rescaled.astype(dataType), fillValue

def readPCF(workPath,parameter):
    """Open and read PCF file for defined parameters.
        Input:
            workPath: full path to the working directory
            parameter: PCF parameter to be read (name = value)
        Output:
            Returns parameter value 
    """
    value = None
    with open(workPath + '/dss.pl.PCF','r') as pcfFile:
        for line in pcfFile:
            match = re.search('^' + parameter,line)
            if match:
                index = string.find(line,'=')
                value = line[index+1:].rstrip('\n')
    log.info("Reading dss.pl.PCF parameter... " + parameter + ": {}".format(value))
    return value

def readPSF(workPath):
    """Read PSF file for output dss (tailored) NC4 file name. Assumes only one 
        file per job.
        Input:
            workPath: full path to the working directory
        Output:
            Returns the file name in the PSF (assumes it's just file name not 
                full path)
    """
    log.info("Finding output NC4 file name from dss.pl.PSF")     
    fileName = None
    print "wp=", workPath
    with open(workPath + '/dss.pl.PSF', 'r') as psfFile:
        fileName = psfFile.readline().rstrip('\n')
        log.info("FOUND: {}".format(fileName))
        return fileName

def writePSF(workDir,fileName,overwrite='yes',close=False):
    """Writes output formatted file name to the dss.pl.PSF
        Input:
            workDir: full path to the working directory
            fileName: output file name to be persisted in the database 
                (i.e. ingested)
            overwrite: yes (default) will overwrite existing PSF, no will 
                append new files to existing PSF
            close: false (default), if true, will append file and put closing
                text in
        Output:
            Writes output file name to PSF
    """
    log.info("Writing: " + fileName + " to dss.pl.PSF")
    if re.match("yes",overwrite):
        with open(workDir + '/dss.pl.PSF', 'w') as psfFile:
            psfFile.write(fileName + '\n')
    else:
        with open(workDir + '/dss.pl.PSF', 'a') as psfFile:
            psfFile.write(fileName + '\n')
    if close:
        with open(workDir + '/dss.pl.PSF', 'a') as psfFile:
            psfFile.write('#END-of-PSF')
    
def parseProjString(line):
    """Parse the grid string. May be defined in the production rule (key is 
        "grid=") or pre-defined in grids.conf.nde.
        Input:
            line: string of projection information as follows
                grid name: anything string you want (e.g. merc_fit), 
                kind of grid: right now only "proj4",
                proj4_str: PROJ.4 string definition (http://trac.osgeo.org/proj/wiki/GenParms), 
                grid width: width of grid in number of pixels [None*],  
                grid height: height of grid in number of pixels [None*], 
                pixel_size_x: size of one pixel in X direction in grid units 
                    (usually meters) [None**], 
                pixel_size_y: size of one pixel in Y direction in grid units 
                    (usually meters and MUST be negative) [None**],
                LL_lat: lower left corner latitude of grid in degrees [NONE***], 
                LL_lon: lower left corner longitude of grid in degrees [NONE***],
                UR_lat: upper right corner latitude of grid in degrees [NONE***],
                UR_lon: upper right corner longitude of grid in degrees [NONE***]
        Output:
            Returns all projection information in a grid dictionary as follows
                "grid_name": name of grid
                "static": True (grid is completely defined) or False (partial 
                    grid definition)
                "proj4_str": proj4 string for pyproj
                "pixel_size_x": size in meters of grid pixels in the east-west 
                    direction
                "pixel_size_y": size in meters of grid pixels in the north-south 
                    direciton (note that convention is to make this negative)
                "grid_width": size of grid in pixels along horizontal
                "grid_height": size of grid in pixels along vertical
                "ll_corner": lower left bound for grid in latitude/longitude
                "ur_corner": upper right bound for grid in latitude/longitude
    """
    log.info("Parsing the PCF defined grid:" + line)
    
    info = {}
    
    parts = string.split(line,',')
    if len(parts) < 9:
        log.error('Grid definition is not correct. Check production rule grid. Exiting')
        sys.exit(os.EX_SOFTWARE)
    
    grid_name = parts[0].lstrip()
    proj4_string = parts[2].lstrip()
    
    static = True #Grid is completely defined
    if parts[3].strip() == "None" or parts[3] == ' ':
        static = False
        grid_width = None
    else:
        grid_width = int(parts[3])
    
    if parts[4].strip() == "None" or parts[4] == ' ':
        static = False
        grid_height = None
    else:
        grid_height = int(parts[4])
    
    if parts[5].strip() == "None" or parts[5] == ' ':
        static = False
        pixel_size_x = None
    else:
        pixel_size_x = float(parts[5])
    
    if parts[6].strip() == "None" or parts[6] == ' ':
        static = False
        pixel_size_y = None
    else:
        pixel_size_y = float(parts[6])
    
    if parts[7].strip() == "None" or parts[7] ==' ':
        static = False
        ll_lon = None
    else:
        ll_lon = float(parts[7])
        
    if parts[8].strip() == "None" or parts[8] ==' ':
        static = False
        ll_lat = None
    else:
        ll_lat = float(parts[8])
        
    if parts[9].strip() == "None" or parts[9] ==' ':
        static = False
        ur_lon = None
    else:
        ur_lon = float(parts[9])
        
    if parts[10].strip() == "None" or parts[10] ==' ':
        static = False
        ur_lat = None
    else:
        ur_lat = float(parts[10])
    
    if (pixel_size_x is None and pixel_size_y is not None) or \
            (pixel_size_x is not None and pixel_size_y is None):
        log.error("Both or neither pixel sizes must be specified for '%s'" % grid_name)
        sys.exit(os.EX_SOFTWARE)
    if (grid_width is None and grid_height is not None) or \
            (grid_width is not None and grid_height is None):
        log.error("Both or neither grid sizes must be specified for '%s'" % grid_name)
        sys.exit(os.EX_SOFTWARE)
    if (ll_lat is None and ll_lon is not None) or \
            (ll_lat is not None and ll_lon is None):
        log.error("Both or neither upper left lat and lon must be specified for '%s'" % grid_name)
        sys.exit(os.EX_SOFTWARE)
    if (ur_lat is None and ur_lon is not None) or \
            (ur_lat is not None and ur_lon is None):
        log.error("Both or neither lower right lat and lon must be specified for '%s'" % grid_name)
        sys.exit(os.EX_SOFTWARE)
    if (ll_lat is None and ur_lat is not None) or \
            (ur_lat is not None and ll_lat is None):
        log.error("Both or neither corner points (upper left and lower right) must be specified for '%s'" % grid_name)
        sys.exit(os.EX_SOFTWARE)
    if grid_width is None and pixel_size_x is None:
        log.error("Either grid size or pixel size must be specified for '%s'" % grid_name)
        sys.exit(os.EX_SOFTWARE)
    
    info["grid_name"] = grid_name
    info["grid_kind"] = parts[1].lstrip()
    info["static"]    = static
    info["proj4_str"] = proj4_string
    info["pixel_size_x"] = pixel_size_x
    info["pixel_size_y"] = pixel_size_y
    info["grid_width"] = grid_width
    info["grid_height"] = grid_height
    info["ll_corner"] = [ll_lon, ll_lat] 
    info["ur_corner"] = [ur_lon, ur_lat]

    log.info("Grid name: " + grid_name)
    log.info("PROJ4 String: " + proj4_string)
    log.info("Grid size (width, height): {}, {}".format(grid_width, grid_height))
    log.info("Pixel size (X, Y): {}, {}".format(pixel_size_x, pixel_size_y))
    log.info("Bounding box (lower left and upper right lon, lat): {}, {}; {}, {}".format(ll_lon, ll_lat, ur_lon, ur_lat))
    
    return info

def makeProjDict(proj4):
    """Convert proj4 string into dictionary for pyresample.
        Input: proj4 string
        Output: projection dictionary in expected pyresample format
    """
    proj_dict = {}
    proj4_parts = string.split(proj4.replace('+',''),' ')
    for items in proj4_parts:
        if '=' in items:
            proj_dict[string.split(items,'=')[0]] = string.split(items,'=')[1]
    
    return proj_dict

def getGEOProj4(metadata):
    """Gets the GEO projection string from the metadata
    """
    proj4 = '+proj=geos +units=m' \
    + ' +h=%f' % metadata['perspective_point_height'] \
    + ' +a=%f' % metadata['semi_major_axis'] \
    + ' +b=%f' % metadata['semi_minor_axis'] \
    + ' +lon_0=%f' % metadata['longitude_of_projection_origin'] \
    + ' +lat_0=%f' % metadata['latitude_of_projection_origin'] \
    + ' +sweep=' + metadata['sweep_angle_axis'] \
    + ' +f=%f' % metadata['inverse_flattening'].astype('float')**(-1)
    
    #if metadata['sweep_angle_axis'] == 'x':
    #    proj4 += ' +sweep=y' # GOES scanning geometry
    #else:
    #    proj4 += ' +sweep=x' #e.g. Meteosat scanning geometry
    
    return proj4

def readNC4(filename, layerName=None): 
    """Opens and reads data and attributes (metadata) the dss output NC4 file.
        Input:
            filename: full path and input file name
            layerName: name of NC4 variable to read. Define in PCF. [OPTIONAL]
        Output:
            vars,meta:
            Returns a variable dictionary with data (including lat/lon if 
                applicable) and data attributes. Also, returns the global 
                metadata attributes in a seperate metadata dictionary.
            
    """
    log.info("Reading the netCDF4 file from the PSF: " + filename)
    lat = None
    lon = None
    nc4f = h5py.File(filename,'r')
    
    llFillValue = None #Initialize fill value for lat/lon arrays
    
    #Get global metadata
    metadata = {}
    gatts = nc4f.attrs.iteritems()
    for items in gatts:
        metadata[items[0].encode('ascii','ignore')] = items[1]
    metadata['orbit'] = 'polar' #initialize orbit
    
    #Get array data and array attributes. Polar data will have a 2D shape while 
    # GOES has a time dimension added (3D)
    data = {}
    keys = nc4f.iteritems()
    for items in keys:
        #Find bounding lats/lons if this is a GEO file (i.e. no lat/lon arrays)
        #Polar (swath) data will have lat/lon arrays
        if re.match('geospatial_lat_lon_extent',items[0],re.I):
            metadata['north_bound'] = nc4f[items[0]].attrs.get('geospatial_northbound_latitude')
            metadata['south_bound'] = nc4f[items[0]].attrs.get('geospatial_southbound_latitude')
            metadata['east_bound'] = nc4f[items[0]].attrs.get('geospatial_eastbound_longitude')
            metadata['west_bound'] = nc4f[items[0]].attrs.get('geospatial_westbound_longitude')
            metadata['nadir_lon'] = nc4f[items[0]].attrs.get('geospatial_lon_nadir')
            if metadata['nadir_lon'] == -75:
                metadata['orbit'] = 'GEOS-East'
            else:
                metadata['orbit']='GEOS-West'
        if re.match('y$',items[0]):
            metadata['ul_y'] = nc4f[items[0]].attrs.get('add_offset')
            metadata['pixel_size_y'] = nc4f[items[0]].attrs.get('scale_factor')
        if re.match('x$',items[0]):
            metadata['ul_x'] = nc4f[items[0]].attrs.get('add_offset')
            metadata['pixel_size_x'] = nc4f[items[0]].attrs.get('scale_factor')
        
        #Check if there is a definition of the data's projection (true for GOES 
        # data) and place in metadata dictionary
        if re.search('imager_projection', items[0], re.I):
            arrayAtts = nc4f[items[0]].attrs.iteritems()
            for atItems in arrayAtts:
                metadata[atItems[0].encode('ascii','ignore')] = atItems[1]
        
        #Pull out 2D and 3D arrays from file. If layer name has been provided, 
        # will delete all other layers.    
        if len(nc4f[items[0]].shape) == 2 or len(nc4f[items[0]].shape) == 3:
            arrayName = string.replace(nc4f[items[0]].name,'/','').encode('ascii','ignore')
            data[arrayName] = {}
            arrayAtts = nc4f[items[0]].attrs.iteritems()
            data[arrayName]['_FillValue'] = None
            for atItems in arrayAtts:
                attributeName = atItems[0].encode('ascii','ignore')
                data[arrayName][attributeName] = atItems[1]
                if (re.match('lat', arrayName, re.I) or re.match('lon', arrayName, re.I)) and (attributeName == 'missing_value' or attributeName == '_FillValue'):
                    #assumes fill values are all negative for lat/lon float
                    llFillValue = np.amax(atItems[1]).astype('int') 
                    data[arrayName]['_FillValue'] = llFillValue
                elif attributeName == 'missing_value' or attributeName == '_FillValue':
                    if np.all(atItems[1]) > 0:
                        dataFillValue = np.amin(atItems[1]).astype('int')
                    elif np.all(atItems[1]) < 0:
                        dataFillValue = np.amax(atItems[1]).astype('int') 
                    data[arrayName]['_FillValue'] = dataFillValue
            dtype = nc4f[items[0]].dtype.name
            data[arrayName]["dtype"] = dtype
            data[arrayName]["shape"] = nc4f[items[0]].shape
            with nc4f[items[0]].astype(dtype):
                data[arrayName]["data"] = nc4f[items[0]][:]
            #Find lat/lon bounds and fill data for polar swath
            if re.match('lat', arrayName, re.I):
                lat = data[arrayName]["data"]
                log.info("Reading latitude array")
            elif re.match('lon', arrayName, re.I):
                lon = data[arrayName]["data"]
                log.info("Reading longitude array")
            elif layerName and not re.match(layerName, arrayName):
                del data[arrayName]
            else:
                metadata['arrayName'] = arrayName
                log.info("Reading in array: " + arrayName)
    #Get latitude/longitude bounding box for swath granules
    if metadata['orbit'] == 'polar' and lat is not None:
        goodData_mask = (lat > llFillValue) & (lon > llFillValue)
        metadata["north_bound"] = np.amax(lat[goodData_mask])
        metadata["south_bound"] = np.amin(lat[goodData_mask])
        metadata["east_bound"] = np.amax(lon[goodData_mask])
        metadata["west_bound"] = np.amin(lon[goodData_mask])
        #Check if straddling 180
        if metadata["west_bound"] <= -179.0 and metadata["east_bound"] >= 179.0:
            metadata["west_bound"] = np.amax(lon[goodData_mask & (lon < 0.0)])
            metadata["east_bound"] = np.amin(lon[goodData_mask & (lon > 0.0)])
            metadata["straddles_180"] = True
        else:
            metadata["straddles_180"] = False
    
    if not data:
        log.error('No data found (layer name may be incorrect)')
        sys.exit(os.EX_SOFTWARE)
               
    return data, metadata
