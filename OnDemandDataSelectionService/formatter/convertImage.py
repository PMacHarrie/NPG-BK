#!/usr/bin/env python2.7
"""
@author: dcp

Convert 2D netCDF4 data layer to several image formats. All parameters are read 
from the dss.pl.PCF file. Input NC4 file is found from the dss.pl.PSF file. All 
image formats are scaled to 8 bits except geotiff which is written in the native 
(input) data type. Program can be run stand alone as a call from dss.pl where 
the "formatter=convertImage.py" is defined in the PCF. The remapSatelliteData.py
program calls functions from this program.

The capability to remove the bow-tie effect from NPP/JPSS VIIRS SDR imagery is
included and can be invoked by setting the "viirs_mend" parameter in the 
production rule.

convertImage.py <full path to working directory>
Must have PYTHONPATH defined for directory with python code.
 
 PCF Parameters:
    formatter=convertImage.py
 	file_format = jpeg, tiff, gif, bmp, png, and/or 
        geotiff. If listing multiple file formats, separate each with a comma
        [OPTIONAL]
	invert = <yes or no (default)>, if yes then the image will be negated 
	    (e.g., making clouds white in IR brightness temperature maps) [OPTIONAL]
	layer_name = name of NC4 variable for imaging
    viirs_mend = 'MOD' or 'IMG' for mending VIIRS bow tie effect in SDRs. 
            [OPTIONAL]
	grid = One line of comma delimited grid information (below) OR grid name
     from grids.conf.nde (e.g. grid=AWIPS_211e) [OPTIONAL parameter]
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
            LL_lon: lower left corner latitude of grid in degrees [None], 
            LL_lat: lower left corner longitude of grid in degrees [None],
            UR_lon: upper right corner latitude of grid in degrees [None],
            UR_lat: upper right corner longitude of grid in degrees [None]
          
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

 Saves the image file with the same name as the output dss file in dss.pl.PSF 
     except:
 	    1) Appends the appropriate suffix (jpg, png, tiff (not geo tagged), gif, 
 	        png, tif (geotiff), or bmp)

MODIFICATION:
    20141029 dcp, created

"""
from util import *
import viirsmend as vm
import os, sys, string, re, logging
from PIL import Image
from PIL import ImageOps
import numpy as np
from osgeo import gdal
import osr
from time import gmtime, strftime
from pyproj import Proj

log = logging.getLogger("formatting")

def createGeoTiff(outputFile, data, proj4Str, pixelSize, gridOrigin):
    """Creates a geoTIFF file with appropriate geo tags using the GDAL library.
        Input:
                outputFile: output file name with full path to working directory
                data: data as numpy array to be written as raster band in 8 or 
                    16 bits/pixel
                proj4Str: PROJ4 string
                pixelSize: Size in meters of pixels as [pixel size in X 
                    coordinate (east-west), pixel size in Y coordinate (north-south as negative)]
                gridOrigin: upper left X/Y coordinates as [ul_x, ul_y]
        Output:
                Returns after writing geoTIFF file to working directory with 
                    output file name
    """
    outputFile = re.sub('\.[tgbnjp].*$','.tif',outputFile)
    log.info("Creating geoTIFF file: " + outputFile)
    
    #Create geoTIFF tags using grid information and proj4 string
    srs = osr.SpatialReference()
    srs.ImportFromProj4(proj4Str)
    #Grid origin for GDAL is the Upper Left X,Y coordinates
    geotransform = [gridOrigin[0], pixelSize[0], 0, gridOrigin[1], 0, pixelSize[1]]
    
    #Determine data type and set GDAL type
    if data.dtype.name == 'uint8': 
        etype = gdal.GDT_Byte
    elif data.dtype.name == 'uint16': 
        etype = gdal.GDT_UInt16
    elif data.dtype.name == 'int16':
        etype = gdal.GDT_Int16
    elif data.dtype.name == 'uint32':
        etype = gdal.GDT_UInt32
    elif data.dtype.name == 'int32':
        etype = gdal.GDT_Int32
    elif data.dtype.name == 'float32':
        etype = gdal.GDT_Float32
    elif data.dtype.name == 'float':
        etype = gdal.GDT_Float64
    elif data.dtype.name == 'float64':
        etype = gdal.GDT_Float64
    else:
        log.error("Data type: " + data.dtype.name + " not valid for GDAL. Exiting.")
        sys.exit(EX_SOFTWARE)
        
    dataFill = dataRange[data.dtype.name][1]
    if not dataFill:
        dataFill = -999
    
    gtiff_driver = gdal.GetDriverByName("GTIFF")
    gtiff = gtiff_driver.Create(outputFile, data.shape[1], data.shape[0],
                                1, etype, options=['COMPRESS=DEFLATE', 
                                                   'PREDICTOR=1'])
    
    gtiff.SetGeoTransform(geotransform)
    gtiff.SetProjection(srs.ExportToWkt())
    
    gtiff_band = gtiff.GetRasterBand(1)
    gtiff_band.SetNoDataValue(dataFill)
    gtiff_band.WriteArray(data)
    gtiff_band.FlushCache()
    
    return outputFile

def writeJPEG(dataSet, outFile, invert="no"):
	"""Convert NC4 2D data array to JPEG format and save file to working 
	    directory
		Input:
			dataSet: data as numpy array to be written to JPEG
			outFile: output file name with full path to working directory
			invert: "Yes" or "No" (default) for inverting the gray scale image 
			    (e.g. IR channels)
		Output:
                        Returns after writing jpeg to working directory with 
                            output file name
	"""
	outFile = re.sub('\.[tgbnjp].*$','.jpg',outFile)

	log.info("Creating image " + outFile)
	
	im = Image.fromarray(dataSet)
	if re.search('yes',invert):
		im = ImageOps.invert(im)
	im.save(outFile,format='JPEG',quality=95)
	return outFile
	
def writePNG(dataSet, outFile, invert="no"):
	"""Convert NC4 2D data array to PNG format and save file
		Input:
			dataSet: data as numpy array to be written to PNG
			outFile: output file name with full path to working directory
			invert: "Yes" or "No" (default) for inverting the gray scale image 
			    (e.g. IR channels)
		Output:
                        Returns after writing PNG to working directory with 
                            output file name
	"""
	outFile = re.sub('\.[tgbnjp].*$','.png',outFile)
	
	log.info("Creating image " + outFile)
	
	im = Image.fromarray(dataSet)
	if re.search('yes',invert):
		im = ImageOps.invert(im)
	im.save(outFile,format='PNG')
	return outFile

def writeBMP(dataSet, outFile, invert="no"):
	"""Convert NC4 2D data array to BMP format and save file to working 
	    directory
		Input:
			dataSet: data as numpy array to be written to BMP
			outFile: output file name with full path to working directory
			invert: "Yes" or "No" (default) for inverting the gray scale image 
			    (e.g. IR channels)
		Output:
                       Returns after writing BMP to working directory with 
                           output file name
	"""
	outFile = re.sub('\.[tgbnjp].*$','.bmp',outFile)
	
	log.info("Creating image " + outFile)
	
	im = Image.fromarray(dataSet)
	if re.search('yes',invert):
		im = ImageOps.invert(im)
	im.save(outFile,format='BMP')
	return outFile

def writeGIF(dataSet, outFile, invert="no"):
	"""Convert NC4 2D data array to GIF format and save file to working 
	    directory
		Input:
			dataSet: data as numpy array to be written to GIF
			outFile: output file name with full path to working directory
			invert: "Yes" or "No" (default) for inverting the gray scale image 
			    (e.g. IR channels)
		Output:
                       Returns after writing GIF to working directory with 
                           output file name
	"""
	outFile = re.sub('\.[tgbnjp].*$','.gif',outFile)
	
	log.info("Creating image " + outFile)
	
	im = Image.fromarray(dataSet)
	if re.search('yes',invert):
		im = ImageOps.invert(im)
	im.save(outFile,format='GIF')
	return outFile

def writeTIFF(dataSet, outFile, invert="no"):
	"""Convert NC4 2D data array to TIFF format and save file to working 
	    directory
		Input:
			dataSet: data as numpy array to be written to TIFF
			outFile: output file name with full path to working directory
			invert: "Yes" or "No" (default) for inverting the gray scale image 
			    (e.g. IR channels)
		Output:
                       Returns after writing TIFF to working directory with 
                           output file name
	"""
	outFile = re.sub('\.[tgbnjp].*$','.tiff',outFile)
	
	log.info("Creating image " + outFile)
	
	im = Image.fromarray(dataSet)
	if re.search('yes',invert):
		im = ImageOps.invert(im)
	im.save(outFile,format='TIFF')
	return outFile

def main(argv):
    try:
        workDir = sys.argv[1]
    except Exception:
        log.exception("Invalid argument. Must be working Directory.")
        sys.exit(os.EX_USAGE)
        
    try:
        log.info("Executing conversion of NC4 layers to imagery...")
        file_format = readPCF(workDir,'file_format')
        nc4File = readPSF(workDir)
        invert = readPCF(workDir,'invert')
        layerName = readPCF(workDir,"layer_name")
        projGrid = readPCF(workDir,"grid") # for geotiff images
        mender = readPCF(workDir,'viirs_mend')
        
        if not invert:
            invert = 'no'
        
        #Read NC4 file
        vars, meta = readNC4(workDir + '/' + nc4File, layerName)
        
        #Find any layers with 2 dimensions - assume these can be imaged
        log.info("Finding 2D arrays for file " + nc4File)
        x = 1 #Counter for number of arrays
        
        ikeys = vars.iterkeys()
        for items in ikeys:
            if re.match('lat',items,re.I):
                lat = vars[items]["data"]
                continue
            elif re.match('lon',items,re.I):
                lon = vars[items]["data"]
                continue
            else:
                arrayName = items
                imdata = vars[items]["data"]
                if len(imdata.shape) > 2: #remove singlet
                    imdata = np.squeeze(imdata)
                
            #Mend bow-tie effect for VIIRS channels via ViirsMend class if 
            #    mender is set
            if mender:
                log.info("Mending SDR bow-tie effect")
                if mender == 'MOD': res = vm.MOD_RESOLUTION
                elif mender == 'IMG': res = vm.IMG_RESOLUTION
                else:
                    log.error('Mending resolution (MOD or IMG) not properly defined in PCF. Exiting.')
                    sys.exit(os.EX_SOFTWARE)
                vmr = vm.ViirsMender(lon, lat, res)
                vmr.mend(imdata)    
            
            #Rescale to 8-bit image. First deal with filler data
            #Initialize the fill mask
            fill_mask = np.zeros((imdata.shape),dtype=bool)
            if vars[arrayName]['_FillValue'] == None:
                dataFillValue = None
            else:
                dataFillValue = vars[arrayName]['_FillValue'].astype('int')
        
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
            if not defaultFillValue or defaultFillValue is None:
                defaultFillValue = int(-999)
            
            imdata[fill_mask] = defaultFillValue
            
            if not re.search('int8',imdata.dtype.name) and not file_format == 'geotiff':
                log.info("Rescaling the image to 8-bits")
                rescl, defaultFillValue = scaleData(imdata, defaultFillValue, 'uint8')
            else:
                rescl = imdata[:]
                
            #Update creation time and lop off file format
            outFile = re.sub('_c\d{15}', '_c' + strftime('%Y%m%d%H%M%S',gmtime()) 
                         + '0', nc4File)
            outFile = re.sub('%[a-zA-Z]*_','', outFile)
            outFile = workDir + '/' + outFile
            
            #Loop through file formats. Move geotiff to top of list in case
            # it's 16 bit (downscaling to 8 bit and back to 16 bit can cause
            # problems
            if re.search(',',file_format):
                formats = re.split(',',file_format)
            else:
                formats = [file_format]
            
            for i in range(0,len(formats)):
                file_format=formats[i].strip()
                #Create the image and save as specified format                    
                if file_format == 'jpeg':
                    outFile = writeJPEG(rescl,outFile,invert)
                elif file_format == "png":
                    outFile = writePNG(rescl,outFile,invert)
                elif file_format == "bmp":
                    outFile = writeBMP(rescl,outFile,invert)
                elif file_format == "gif":
                    outFile = writeGIF(rescl,outFile,invert)
                elif file_format == "tiff":
                    outFile = writeTIFF(rescl,outFile,invert)
                elif file_format == "geotiff":                  
                    # Grid must be static, i.e., completely defined
                    #Parse the grid information provided in the PCF
                    grid_info = parseProjString(projGrid)
                    
                    if grid_info['static'] == False:
                        log.error('Grid must be completely defined to generate geoTIFF output file without remapping. Exiting.')
                        sys.exit(os.EX_SOFTWARE)
                    
                    ll_lon, ll_lat = grid_info['ll_corner']
                    ur_lon, ur_lat = grid_info['ur_corner']
                    
                    if re.search('latlong', grid_info['proj4_str']):
                        grid_origin = [ll_lon, ur_lat]
                    else:
                        #Create proj4 object for calculating projection extents in X/Y space
                        p = Proj(grid_info["proj4_str"], errcheck=True)
                        ll_x, ll_y = p(ll_lon, ll_lat, inverse=False)
                        ur_x, ur_y = p(ur_lon, ur_lat, inverse=False)
                        ul_x = ll_x
                        ul_y = ur_y
                        grid_origin = [ul_x, ul_y]
                    
                    pixel_size = [grid_info['pixel_size_x'], 
                                      grid_info['pixel_size_y']]
                    outFile = createGeoTiff(outFile, rescl.astype(np.float32), grid_info['proj4_str'], 
                                  pixel_size, grid_origin)
                else:
                    log.error("No accepted image format defined in PCF. Must be jpeg, png, bmp, tiff, geotiff or gif. Exiting.")
                    sys.exit(os.EX_USAGE)
                    
                #write output file name to PSF file    
                outFile = os.path.basename(outFile)
                log.info("Writing new image file to dss.pl.PSF: " + outFile)
                if i == 0 and x == 1:
                    with open(workDir + '/dss.pl.PSF', 'w') as psfFile:
                        psfFile.write(outFile + '\n')
                else:
                    with open(workDir + '/dss.pl.PSF', 'a') as psfFile:
                        psfFile.write(outFile + '\n')
            x += 1        
        
        with open(workDir + '/dss.pl.PSF', 'a') as psfFile:
            psfFile.write('#END-of-PSF')    
        
        log.info("Image conversion successful. Exiting.")
    except Exception:
        log.exception("Creation of image file failed. Exiting.")
        sys.exit(os.EX_SOFTWARE)
        
if __name__ == "__main__":
    setuplogger(sys.argv[1])
    log.debug(sys.argv)
    main(sys.argv[1:])
