#!/usr/bin/env python2.7
"""
@author: dcp

Program remaps NC4 CF files according to a user provided grid definition. The projection 
is defined in terms of a PROJ.4 string. There are pre-defined grids, in 
grids.conf.nde, that can be used by specifying the grid name in the production 
rule (e.g. AWIPS_211e) or custom grids can be defined in the production rule. 
Remapped data is stored in netCDF4 Climate Forecast conventions (default) or in 
another defined image format. Conversion to image formats is handled post 
re-mapping using the functions from convertImage.py. 

The grid may be completely defined (as in the case of AWIPS grids) or it may be 
partially defined so that the grid is fitted to the data.

The capability to remove the bow-tie effect from NPP/JPSS VIIRS SDR imagery is
included and can be invoked by setting the "viirs_mend" parameter in the 
production rule.

remapSatelliteData.py <full_path_to_working_directory>
Must have PYTHONPATH defined for directory with python code.

PCF (key/value pairs) Information:
    formatter=remapSatelliteData.py [REQUIRED]
    ops_dir = Full path to dss/formatter directory. [REQUIRED]
    resampleRadius = radius in meters to look for nearest neighbor from
                grid pixel in swath/GOES. VIIRS ~ 2000, CrIS ~ 25000,
                ATMS ~ 70000 (at least double the spatial resolution).
    grid = One line of comma delimited grid information (below) OR grid name
     from grids.conf.nde (e.g. grid=AWIPS_211e) [REQUIRED]
            grid name: User defined grid name (e.g. MERC_FIT) [REQUIRED], 
            kind of grid: right now only "proj4" [REQUIRED],
            proj4_str: PROJ.4 string defining the projection 
                (http://trac.osgeo.org/proj/wiki/GenParms) [REQUIRED], 
            grid width: width of grid in number of pixels [None],  
            grid height: height of grid in number of pixels [None], 
            pixel_size_x: size of one pixel in X direction in grid units 
                (usually meters) [None], 
            pixel_size_y: size of one pixel in Y direction in grid units 
                (usually meters and MUST be negative) [None],
            LL_lat: lower left corner latitude of grid in degrees [None], 
            LL_lon: lower left corner longitude of grid in degrees [None],
            UR_lat: upper right corner latitude of grid in degrees [None],
            UR_lon: upper right corner longitude of grid in degrees [None]
          
          NOTE: At a minimum grid dimensions (width and height) or pixel_sizes 
          must be specified. All values must be specified for a completely 
          defined grid. String can have "None" or " " where [None] is 
          identified.

          EXAMPLES:
              grid=AWIPS_211e
              
              OR
              
              grid=AWIPS_211e, proj4, +proj=lcc +a=6371200 +b=6371200 +lat_0=25 
              +lat_1=25 +lat_2=25 +lon_0=-95 +units=m +no_defs, 5120, 5120, 
              1015.9, -1015.9, -113.133, 16.368, -49.384, 57.289
              
              OR
              
              grid=LCC_FIT, proj4, +proj=lcc +a=6371200.0 +b=6371200.0 +lat_0=25 
              +lat_1=25 +lat_2=25 +lon_0=-95 +units=m +no_defs, None, None, 
              1000, -1000, None, None, , 

    file_format = nc4 (default), jpeg, tiff, gif, bmp, png, and/or 
        geotiff. If listing multiple file formats, separate each with a comma
        [OPTIONAL]
    layer_name = Array name (as it is in the file) that is to be 
        remapped - will remap any 2D or 3D array. [OPTIONAL]
    invert = <yes or no (default)>. Invert color scale 
        (e.g. for IR cloud imagery). [OPTIONAL]
    dataType = 'uint8', 'uint16', or 'float'. Data type for output data 
        (rescaled) as numpy data type [OPTIONAL] 
    viirs_mend = 'MOD' or 'IMG'. For mending VIIRS bow tie effect in 
        SDRs. [OPTIONAL]

Output:
    Program remaps 2D data arrays into a PCF defined projected grid using 
    nearest neighbor resampling. The default ouptut format is netCDF4 Climate 
    Forecast conventions for projected data with internal compression. The PCF 
    may define other imagery formats (see above). All logging is appended to 
    the dss.pl.log. The final output file name is written to the PSF and 
    control is returned to dss.pl.

MODIFICATION
    revised: 20141110 dcp, created

"""
from util import *
from convertImage import *
from remap_config import *
import viirsmend as vm
import os, sys, re, string, logging
import math
import numpy as np
from pyproj import Proj
from pyresample import geometry, image
from netCDF4 import Dataset
from time import gmtime, strftime


"""For development (plotting images)
from pyresample import plot
import matplotlib.pyplot as plt
"""

#Set up logging to append to dss.pl.log file
log = logging.getLogger("formatting")

def resample(proj_def, data_def, data, fillValue, resampleRad):
    """Resample the data into the projected area using nearest neighbor.

        Input:
            proj_def: pyresample projection (grid) definition
            data_def: pyresample satellite data geometry definition 
                (swath for polar or geostationary grid)
            data: numpy data array
            fillValue: fill value for data grid
            resampleRad: radius in meters to look for nearest neighbors from
                grid pixel in swath/GOES
        Output:
            resampled_data: numpy data array resampled to the proj_def grid
    """
    log.info("Resampling the data to target projection grid")
    
    data_con = image.ImageContainerNearest(data, data_def, 
                                           radius_of_influence=resampleRad, 
                                           fill_value = fillValue, nprocs=2)
        
    image_con = data_con.resample(proj_def)
    resampled_data = image_con.image_data
    
    return resampled_data

def getDynamicGrid(proj, grid_info, data_geom_def):
    """Calculates dynamic grid for partially defined grids, i.e., creates a 
    grid that fits the data. At a minimum, grid information must include the 
    PROJ4 target projection, and either the grid dimensions or pixel size. All 
    other parameters can be calculated using the data's latitude/longitude as 
    a bounding box.
        Input:
            proj: pyproj proj4 object defining the target projection
            grid_info: other grid information such as extents (bounding box) 
                and/or grid size/pixel size
            data_geom_def: The pyresample data geometry definition for the data in the netCDF-4 file.
        Output:
            Updates the grid_info dictionary to completely define the grid and 
            returns the bounding box (lower left and upper right in X/Y coords)
    """
    log.info("Grid is partially defined. Creating dynamic grid based on latitude/longitude to find bounding box")
    

    #Create a bounding box in projection space using the input data boundaries
    boundary_lons, boundary_lats = data_geom_def.get_boundary_lonlats()
        
    #Test the image boundaries. (Possible future updates: check the diagonals as well. Not sure if there are any projections for which that is necessary)
    lon_test = np.concatenate((boundary_lons.side1, boundary_lons.side2, boundary_lons.side3, boundary_lons.side4))
    lat_test = np.concatenate((boundary_lats.side1, boundary_lats.side2, boundary_lats.side3, boundary_lats.side4))

    # Filter out the fill values
    good_ll_mask = (lon_test <= 180) & (lon_test >= -180) & (lat_test <= 90) & (lat_test >= -90)
    lon_test = lon_test[good_ll_mask]
    lat_test = lat_test[good_ll_mask]

    x_test, y_test = proj(lon_test, lat_test, errcheck=True)
    
    #Get grid extents
    if grid_info["ll_corner"][0] is None and grid_info["ur_corner"][0] is None:
        ll_x = x_test.min()
        ll_y = y_test.min()
        ur_x = x_test.max()
        ur_y = y_test.max()
    elif grid_info["ur_corner"][0] is None:
        ur_x = x_test.max()
        ur_y = y_test.max()
        ll_lon, ll_lat = grid_info['ll_corner']
        ll_x, ll_y = proj(ll_lon, ll_lat, inverse=False)
    elif grid_info['ll_corner'][0] is None:
        ll_x = x_test.min()
        ll_y = y_test.min()
        ur_lon, ur_lat = grid_info['ur_corner']
        ur_x, ur_y = proj(ur_lon, ur_lat, inverse=False)
    else:
        ll_lon, ll_lat = grid_info['ll_corner']
        ur_lon, ur_lat = grid_info['ur_corner']
        ll_x, ll_y = proj(ll_lon, ll_lat, inverse=False)
        ur_x, ur_y = proj(ur_lon, ur_lat, inverse=False)
        
    if grid_info["grid_width"] is None:
        grid_info["grid_width"] = math.ceil((ur_x - ll_x) / 
                                            float(grid_info["pixel_size_x"]) )
        grid_info["grid_height"] = math.ceil((ll_y - ur_y) / 
                                             float(grid_info["pixel_size_y"]) )
    elif grid_info["pixel_size_x"] is None:
        grid_info["pixel_size_x"] = (ur_x - ll_x) / float(grid_info["grid_width"])
        # Standard is to use negative y pixel size
        grid_info["pixel_size_y"] = (ll_y - ur_y) / float(grid_info["grid_height"]) 

    log.info("Grid size (width, height) in pixels: {}, {}".
             format(grid_info["grid_width"],grid_info["grid_height"]))
    log.info("Pixel sizes (X, Y) in meters: {}, {}".
             format(grid_info["pixel_size_x"], grid_info["pixel_size_y"]))
    log.info("Bounding box (ll_x, ll_y, ur_x, ur_y): {}, {}, {}, {}".
             format(ll_x, ll_y, ur_x, ur_y))
    
    return [ll_x, ll_y, ur_x, ur_y]

def getProj4GeometryDefinition(projGrid, opsDir, dataGeomDef):
	"""
	Parses the given PROJ4 string projGrid and determines the geometry definition
	for pyresample.

	Will search grids.conf.nde to get the grid information if only a name is
	specified, and determines the grid dynamically if necessary.

	Returns a tuple with eight elements
	  0. proj_geom_def: the geometry definition object
	  1. grid_info: a dictionary - see util.py::parseProjString for details
	  2. proj4_dict: The PROJ4 string converted into a dictionary - see util.py::makeProjDict for details
	  3. p: The proj4 object used for calculating projection extents in X/Y space
	  4-7. ll_x, ll_y, ur_x, ur_y: The lower left & upper right corners of the defined grid
	"""
	#Check if pre-defined grid (grids.conf.nde) and get grid information.
	if not re.search(',', projGrid):
		log.info("Fetching pre-defined grid for: " + projGrid)
	with open(opsDir + '/grids.conf.nde') as gridFile:
		gridContent = gridFile.read().splitlines()
	for line in gridContent:
		if re.match('^' + projGrid, line):
			projGrid = line

	#Parse the grid information provided in the PCF
	grid_info = parseProjString(projGrid)

	#Create proj4 object for calculating projection extents in X/Y space
	p = Proj(grid_info["proj4_str"], errcheck=True)

	#Determine projection grid size and/or pixel size, and lat/lon extents
	# (ll = lower left and ur = upper right)
	# Two cases: static (True) is where the grid is completely defined,
	#    and static (False) where the grid is not completely defined and
	#    must be determined dynamically using the data lat/lon bounds
	if grid_info['static'] == True:
		log.info("Grid is completely defined")
		ll_lon, ll_lat = grid_info['ll_corner']
		ll_x, ll_y = p(ll_lon, ll_lat, inverse=False)
		ur_lon, ur_lat = grid_info['ur_corner']
		ur_x, ur_y = p(ur_lon, ur_lat, inverse=False)
	else:
		[ll_x, ll_y, ur_x, ur_y] = getDynamicGrid(p, grid_info, dataGeomDef)

	#Set the projection extents as Lower Left (x,y) and Upper Right (x,y)
	# for pyresample.
	#Set the grid height and width in pixels as defined (static) or
	# calculated (dynamic) above.
	proj_extents = [ll_x, ll_y, ur_x, ur_y]
	grid_size = {'x': grid_info['grid_width'],
	             'y': grid_info['grid_height']}

	#Get the geometry (area) definition for the projection
	proj4_dict = makeProjDict(grid_info['proj4_str'])
	proj_geom_def = geometry.AreaDefinition(grid_info['grid_name'],
	                                        grid_info['grid_name'],
	                                        grid_info['grid_name'],
	                                        proj4_dict, grid_size['x'],
	                                        grid_size['y'], proj_extents)

	return proj_geom_def, grid_info, proj4_dict, p, ll_x, ll_y, ur_x, ur_y

def processFillValues(vars, arrayName, imdata):
	"""
	vars: The dictionary of variables returned by readNC4
	arrayName: The name of the array to process within the NC file
	imdata: A 2-D containing the data to be processed.

	Computes and applies the fill mask to imdata.
	Returns the fill value that was used.
	"""
	#Resample the data into the projected grid. First deal with filler.
	if vars[arrayName]['_FillValue'] == None:	
		dataFillValue = None
	else:
		dataFillValue = vars[arrayName]['_FillValue'].astype('int')

	#Initialize the fill mask
	fill_mask = np.zeros((imdata.shape),dtype=bool)
	if dataFillValue > 0:
		fill_mask = imdata >= dataFillValue
	elif dataFillValue < 0 and dataFillValue != None:
		fill_mask = imdata <= dataFillValue
	elif np.isnan(np.sum(imdata)):
		fill_mask = np.isnan(imdata)
	if imdata.dtype.name in dataRange:
		defaultFillValue = dataRange[imdata.dtype.name][1]
	else:
		log.error("Data type: " + imdata.dtype.name + " not valid. Exiting.")
		sys.exit(os.EX_SOFTWARE)
	if not defaultFillValue:
		defaultFillValue = int(-999)

	imdata[fill_mask] = defaultFillValue

	return defaultFillValue

	
def writeNC4_CFProj(outputFile, x, y, lon, lat, data, metadata, vars, 
                    proj4Dict, gridInfo, proj4Str, ll_corner, ur_corner):
    """Write the remapped data to an NC4 file with Climate Forecast Conventions
    (v1.6). Must be projected data (not satellite, i.e. lat/lon)
    http://cfconventions.org/Data/cf-conventions/cf-conventions-1.6/build/cf-conventions.html

        Input:
            outputFile: output file name
            x: x projected coordinates of grid [meters]
            y: y projected coordinates of grid [meters]
            lon: longitude of grid pixels [degrees]
            lat: latitude of grid pixels [degrees]
            data: remapped numpy data array
            metadata: metadata (array and global attributes) of NC4 data
            vars: NC4 data array dictionary
            proj4Dict: pyresample proj4 dictionary of projected grid
            gridInfo: other information for the grid (bounding box, 
                grid size, pixel size)
            proj4Str: original proj4 string from the PCF file
            ll_corner: lon, lat of lower left corner of grid 
            ur_corner: lon, lat of upper right corner of grid
        Output:
            Writes the NC4 file (outputFile name) in CF conventions to the 
                working directory
    """
    outputFile = re.sub('\.[tgbnjp].*$','.nc',outputFile)
    
    log.info("Creating netCDF4 file with CF conventions for projected grid: "
             + outputFile)
    
    #Open the NC4 file to write to
    rtgrp = Dataset(outputFile, 'w', format='NETCDF4')
    
    #Create dimensions
    dim2 = rtgrp.createDimension('y', y.shape[0])
    dim1 = rtgrp.createDimension('x', x.shape[0])
    
    #Create variables, first get the fill value
    dataFill = dataRange[data.dtype.name][1]
    if not dataFill:
        dataFill = -999
        
    varName = metadata['arrayName']
    x_proj = rtgrp.createVariable('x', 'f8', ('x'), zlib=True, complevel=1)
    y_proj = rtgrp.createVariable('y', 'f8', ('y'), zlib=True, complevel=1)
    lons = rtgrp.createVariable('lon', 'f4', ('y', 'x'), zlib=True, complevel=1)
    lats = rtgrp.createVariable('lat', 'f4', ('y', 'x'), zlib=True, complevel=1)
    nc4data = rtgrp.createVariable(varName, data.dtype.name, ('y','x'), 
                                   zlib=True, complevel=1, 
                                   fill_value=dataFill)
    
    #Write variables to file
    x_proj[:] = x
    y_proj[:] = y
    lons[:] = lon
    lats[:] = lat
    nc4data[:] = data
    
    #Create grid_mapping variable
    grid_id = proj4Dict['proj']
    grid_mapping_name = grid_map_dictionary[grid_id]['grid_mapping_name']
    grid = rtgrp.createVariable(gridInfo.name, 'i4')
    grid.grid_mapping_name = grid_mapping_name
    keys = proj4Dict.iterkeys()
    for key in keys:
        if key in grid_map_dictionary[grid_id]['grid_parameters']:
           grid.temp = float(proj4Dict[key]) 
           grid.renameAttribute('temp', grid_map_dictionary[grid_id]['grid_parameters'][key])
    grid.ll_x = float(gridInfo.area_extent[0])
    grid.ll_y = float(gridInfo.area_extent[1])
    grid.ur_x = float(gridInfo.area_extent[2])
    grid.ur_y = float(gridInfo.area_extent[3])
    grid.grid_height = int(gridInfo.x_size)
    grid.grid_width = int(gridInfo.y_size)
    grid.proj4_string = proj4Str
    
    #Write array attributes
    x_proj.units = 'meters'
    x_proj.long_name = 'projection x-coordinate'
    x_proj.standard_name = 'projection_x_coordinate'
    y_proj.units = 'meters'
    y_proj.long_name = 'projection y-coordinate'
    y_proj.standard_name = 'projection_y_coordinate'
    
    lats.units = 'degrees_north'
    lats.long_name = 'latitude coordinate'
    lats.standard_name = 'latitude'
    
    lons.units = 'degrees_east'
    lons.long_name = 'longitude coordinate'
    lons.standard_name = 'longitude'
    
    nc4data.long_name = vars[varName]['long_name']
    nc4data.scale_factor = vars[varName]['scale_factor']
    nc4data.add_offset = vars[varName]['add_offset']
    nc4data.coordinates = 'lon lat'
    nc4data.grid_mapping = gridInfo.name
    
    #GOES may not include this attribute (other arrays may not either)
    if 'valid_min' in vars[varName]:
        nc4data.valid_min = vars[varName]['valid_min']
    if 'valid_max' in vars[varName]:
        nc4data.valid_max = vars[varName]['valid_max']
    if 'units' in vars[varName]:
        nc4data.units = vars[varName]['units']
    
    #Write global attributes
    rtgrp.Conventions = metadata['Conventions']
    rtgrp.naming_authority = metadata['naming_authority']
    rtgrp.standard_name_vocabulary = metadata['standard_name_vocabulary']
    rtgrp.title = metadata['title']
    rtgrp.date_created = strftime('%Y-%m-%dT%H:%M:%S',gmtime())
    rtgrp.institution = metadata['institution']
    rtgrp.summary = metadata['summary']
    rtgrp.time_coverage_start = metadata['time_coverage_start']
    rtgrp.time_coverage_end = metadata['time_coverage_end']
    rtgrp.project =  metadata['project']
    rtgrp.keywords =  metadata['keywords']
    rtgrp.cdm_data_type =  'Grid'
    rtgrp.history = 'Created by NDE Data Handling System Data Selection Services'
    rtgrp.processing_level = 'Level 3'
    rtgrp.publisher_name = 'DOC/NOAA/NESDIS/NDE > NPP Data Exploitation, NESDIS, NOAA, U.S. Department of Commerce'
    rtgrp.publisher_email = 'espcoperations@noaa.gov'
    rtgrp.publisher_url = 'http://www.ospo.noaa.gov'
    
    rtgrp.geospatial_lat_min = float(ll_corner[1])
    rtgrp.geospatial_lat_max = float(ur_corner[1])
    rtgrp.geospatial_lon_min = float(ll_corner[0])
    rtgrp.geospatial_lon_max = float(ur_corner[0])
    
    if 'satellite_name' in metadata: 
        rtgrp.platform = metadata['satellite_name']
    if 'platform_ID' in metadata: 
        rtgrp.platform = metadata['platform_ID']
    if 'instrument_name' in metadata: 
        rtgrp.instrument = metadata['instrument_name']
    if 'instrument_ID' in metadata: 
        rtgrp.instrument = metadata['instrument_ID']
    if 'production_site' in metadata:
        rtgrp.production_site =  metadata['production_site']
    if 'production_environment' in metadata:
        rtgrp.production_environment =  metadata['production_environment']
    
    rtgrp.close()
    return outputFile
    
def main(argv):
    try:
        workDir = sys.argv[1]
    except Exception:
        log.exception("Invalid argument. Must be working Directory. Exiting.")
        sys.exit(os.EX_USAGE)
    
    try:
        log.info("Executing remapping...")
        #Get PSF (file name) and PCF information (grid definition, 
        # output file format [opt], layer name [opt])
        nc4File = readPSF(workDir)
        if not nc4File:
            log.error("No NC4 file written to dss.pl.PSF. Exiting.")
            sys.exit(os.EX_SOFTWARE)
        projGrid = readPCF(workDir,"grid")
        file_format = readPCF(workDir,"file_format")
        layerName = readPCF(workDir,"layer_name")
        opsDir = readPCF(workDir,"ops_dir")
        invert = readPCF(workDir,'invert')
        data_type = readPCF(workDir,'dataType')
        mender = readPCF(workDir,'viirs_mend')
        resampleRad = float(readPCF(workDir,'resampleRadius'))
        
        #Open NC4 file and (if applicable) read longitude, 
        # latitude, and image data; and associated filler values
        vars, meta = readNC4(workDir + '/' + nc4File, layerName)
        ikeys = vars.iterkeys()
        for items in ikeys:
            if re.match('lat',items,re.I):
                lat = vars[items]["data"]
            elif re.match('lon',items,re.I):
                lon = vars[items]["data"]
            else:
                arrayName = items
                imdata = vars[items]["data"]
        
        if len(imdata.shape) > 2: #remove singlet
            imdata = np.squeeze(imdata)

        #Mend bow-tie effect for VIIRS channels via ViirsMend class
        if mender:
            log.info("Mending SDR bow-tie effect")
            if mender == 'MOD': res = vm.MOD_RESOLUTION
            elif mender == 'IMG': res = vm.IMG_RESOLUTION
            else:
                log.error('Mending resolution (MOD or IMG) not properly defined in PCF. Exiting.')
                sys.exit(os.EX_SOFTWARE)
            vmr = vm.ViirsMender(lon, lat, res)
            vmr.mend(imdata)
                
        #Get data geometry definition (swath for polar and area for geo)
        if meta['orbit'] == 'polar':
            log.info("Swath data. Obtaining data geometry definition.")
            data_geom_def = geometry.SwathDefinition(lons=lon,lats=lat)
        else: #GEO data
            log.info("Geostationary data. Obtaining data grid definition")
            geo_proj = getGEOProj4(meta)
            geo_p = Proj(geo_proj)
            geo_proj_dict = makeProjDict(geo_proj)
            geo_ll_lat = meta['south_bound']
            geo_ll_lon = meta['west_bound']
            geo_ur_lat = meta['north_bound']
            geo_ur_lon = meta['east_bound']
            #Get the data set size
            keys = vars.iterkeys()
            for items in keys:
                data_size_x = vars[items]['shape'][1]
                data_size_y = vars[items]['shape'][0]
            #Lat/lon box has pixels that are off earth causing an error 
            # when using proj4 api. 
            #Computed as projected coord x,y = scan angle * height
            pixel_size_x = meta['pixel_size_x']
            pixel_size_y = meta['pixel_size_y']
            geo_ul_x = meta['ul_x']
            geo_ul_y = meta['ul_y']
            height = meta['perspective_point_height']
            geo_ul_x *= height
            geo_ul_y *= height
            pixel_size_x *= height
            pixel_size_y *= height
            geo_ll_x = geo_ul_x
            geo_ur_y = geo_ul_y
            geo_ur_x = geo_ul_x + (pixel_size_x * data_size_x)
            geo_ll_y = geo_ul_y + (pixel_size_y * data_size_y)

            geo_proj_extents = [geo_ll_x, geo_ll_y, geo_ur_x, geo_ur_y]
            log.info("GEO Projection: " + geo_proj)
            log.info("GEO Extents (lower left and upper right x, y): {}, {}; {}, {} " . 
                     format(geo_ll_x, geo_ll_y, geo_ur_x, geo_ur_y))
            data_geom_def = geometry.AreaDefinition('geostationary','geos', 
                                                    'geos', geo_proj_dict, 
                                                    data_size_x, data_size_y, 
                                                    geo_proj_extents)
            
        # Get the target grid's geometry definition
        proj_geom_def, grid_info, proj4_dict, p, ll_x, ll_y, ur_x, ur_y = getProj4GeometryDefinition(projGrid, opsDir, data_geom_def)
        
        #Check that data overlaps target projection. If it does not, assume we 
        # don't want to fail the job but will allow exit gracefully with no 
        # produced product. Should define gazatteer in production rule 
        # to narrow any rejected granules based on non-overlap. This check
        # is comparing the grid extents to the data extents. Could still return
        # a positive and not have any data in the remap.
        #if not data_geom_def.overlaps(proj_geom_def): *****DOESNT WORK RIGHT
        if meta['orbit'] == 'polar':
            intersects = False
        else:
            intersects = True #GOES data (full disk extents) never intersect grid
            
        corners=[meta['west_bound'],meta['north_bound']],[meta['east_bound'],meta['north_bound']],[meta['west_bound'],meta['south_bound']],[meta['east_bound'],meta['south_bound']]
        for i in range(0, 4):
            if p(corners[i][0],corners[i][1])[0] >= ll_x and p(corners[i][0],corners[i][1])[0] <= ur_x and p(corners[i][0],corners[i][1])[1] >= ll_y and p(corners[i][0],corners[i][1])[1] <= ur_y:
                intersects = True
                log.info("Satellite data overlaps the target projection grid")
                break
        if not intersects:
            log.warning("Satellite data does not overlap the target projection grid. Exiting with zero return code.")
            writePSF(workDir, ' ',overwrite='yes',close=True)
            sys.exit(os.EX_OK)
        
        #Resample the data into the projected grid. First deal with filler.
        defaultFillValue = processFillValues(vars, arrayName, imdata)
        
        re_imdata = resample(proj_geom_def, data_geom_def, imdata, 
                             defaultFillValue, resampleRad)
        
        #Last check that data was actually in grid. This will also catch GOES
        # data that didn't intersect a grid.
        if np.sum(re_imdata == defaultFillValue) == (re_imdata.shape[0]*re_imdata.shape[1]):
            log.warning("Satellite data does not overlap the target projection grid. Exiting with zero return code.")
            writePSF(workDir, ' ',overwrite='yes',close=True)
            sys.exit(os.EX_OK)            
        
        # For development:
        # plot.show_quicklook(proj_geom_def,re_imdata)
        
        #Update creation time and lop off file format
        # outFile = string.replace(nc4File,'_s','_' + grid_info['grid_name'] 
        #                          + '_s')
        outFile = re.sub('_c\d+', '_c' + strftime('%Y%m%d%H%M%S',gmtime()) 
                         + '0', nc4File)
        outFile = re.sub('%[a-zA-Z]*_','', outFile)
        outFile = workDir + '/' + outFile
        
        #Loop through file formats. Rearrange list so that geotiff and nc4 are
        # created prior to any 8 bit imagery. Scaling to 8 bit and back to 16
        # bit can cause problems.    
        if re.search(',',file_format):
            formats = re.split(',',file_format)
        else:
            formats = [file_format]
        
        for i in range(0,len(formats)):
            file_format=formats[i].strip()
            
            #Write remapped data to file
            #Default format is NC4 CF conventions
            if file_format == None or file_format == 'nc4': 
                log.info("netCDF4 CF conventions format being created")
                #Write resampled data to NC4 file in CF conventions. Per convention, 
                # must include x and y projection coords along with lat/lon 
                # coordinates. Must create a variable/data set that describes the 
                # projection.
                # http://cfconventions.org/Data/cf-conventions/cf-conventions-1.6/build/cf-conventions.html#grid-mappings-and-projections
                
                x, y = proj_geom_def.get_proj_coords()
                x = x[0,:]
                y = y[:,0]
                lon_proj, lat_proj = proj_geom_def.get_lonlats()
                
                if data_type:
                    re_imdata, defaultFillValue = scaleData(re_imdata, defaultFillValue, data_type)
                
                outFile = writeNC4_CFProj(outFile, x, y, 
                                          lon_proj, lat_proj, re_imdata, meta, 
                                          vars, proj4_dict, proj_geom_def, 
                                          grid_info['proj4_str'], 
                                          grid_info['ll_corner'],
                                          grid_info['ur_corner'])
            #geoTIFF is a geo tagged tiff image
            elif file_format == 'geotiff':
                log.info("GEOTIFF format being created")
                if data_type:
                    re_imdata, defaultFillValue = scaleData(re_imdata, defaultFillValue, data_type)
                    
                pixel_size = [grid_info['pixel_size_x'], grid_info['pixel_size_y']]
                ul_x = ll_x
                ul_y = ur_y
                grid_origin = [ul_x, ul_y]
                #outFile = workDir + '/' + meta['arrayName'] + '_' + outFile
                #outFile = re.sub('@.+?_','_',outFile)
                outFile = createGeoTiff(outFile, re_imdata, grid_info['proj4_str'], 
                                        pixel_size, grid_origin)
            #Run-of-the-mill, ho hum, BORING image formats
            else:
                log.info(file_format + " format being created")
                #Always rescale images to 8-bits. Some formats here can handle 
                # 16 bits but why bother (your eye can't see that fool)
                if not re.search('uint8', re_imdata.dtype.name):
                    log.info("Rescaling the image to unsigned 8-bits")
                    re_imdata, defaultFillValue = scaleData(re_imdata, defaultFillValue, 'uint8')
                #outFile = workDir + '/' + meta['arrayName'] + '_' + outFile
                #outFile = re.sub('@.+?_','_',outFile)
                
                if not invert:
                    invert = 'no'
                    
                #Create the image and save as specified format                    
                if file_format == 'jpeg':
                    outFile = writeJPEG(re_imdata,outFile,invert)
                elif file_format == "png":
                    outFile = writePNG(re_imdata,outFile,invert)
                elif file_format == "bmp":
                    outFile = writeBMP(re_imdata,outFile,invert)
                elif file_format == "gif":
                    outFile = writeGIF(re_imdata,outFile,invert)
                elif file_format == "tiff":
                    outFile = writeTIFF(re_imdata,outFile,invert)
                else:
                    log.error("No accepted format defined. Must be jpeg, png, bmp, tiff, or gif. Exiting.")
                    sys.exit(os.EX_USAGE)
                    
            #write output file name to PSF file    
            outFile = os.path.basename(outFile)
            if i == 0:
                with open(workDir + '/dss.pl.PSF', 'w') as psfFile:
                    psfFile.write(outFile + '\n')
            else:
                with open(workDir + '/dss.pl.PSF', 'a') as psfFile:
                    psfFile.write(outFile + '\n')
        
        with open(workDir + '/dss.pl.PSF', 'a') as psfFile:
            psfFile.write('#END-of-PSF')
        log.info("Remapping successful. Exiting.")
        sys.exit(0)
        
    except Exception:
        log.exception("Remapping failed. Exiting.")
        writePSF(workDir, ' ',overwrite='yes',close=True)
        sys.exit(os.EX_SOFTWARE)            

if __name__ == "__main__":
    setuplogger(sys.argv[1])
    log.debug(sys.argv)
    main(sys.argv[1:])     
