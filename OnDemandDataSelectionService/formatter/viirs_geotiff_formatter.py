#!/usr/bin/env python2.7
"""
This script was originally written to re-format the VIIRS geotiffs for the
National Ice Center (ENTR-4006). It is similar to remapSatellite.py, except 
that it only outputs data in geoTIFF format, it edits some of the keys in the 
resulting geoTIFF file, and most importantly, it can processes multiple 
channels of imagery that share the same geolocation data at the same time.

This script calls the binary geotifcp_inc_gdal, which is a version of geotifcp
that has been modified so that it also recognizes and copies GDAL tags into the
output file; the extra tags recognized are:
	42112 (GDAL_METADATA)
	42113 (GDAL_NODATA)
The source for the modified geotifcp is on ndepgs-dev3-1 contained in
/home/jboss/libgeotiff-1.4.2

PCF Parameters
	ops_dir - (REQUIRED) The path to directory which contains this script; most
		      likely $dm/pgs/dss/formatter.

	grid - (REQUIRED) As in remapSatelliteData.py

	layer_name - (REQUIRED) A comma delimeted list of layers to process in the
	             netCDF4 file.

	output_filename_prefix - (REQUIRED) A comma delimeted list, parallel to the
	                         list provided in layer_name, specifying the output
	                         filename (minus the d%t%e%b%.tif portion)

	resampleRadius - (REQUIRED) As in remapSatelliteData.py

	input1 - Technically an input file, not a parameter, but this program 
	         assumes that it is set to any of the IDPS .h5 input files; it does
	         not matter which one.

	GTCitationGeoKey - (OPTIONAL) Set the geoTIFF key of the same name to this
	                   value. If not provided, the key is not changed.

	GeogCitationGeoKey - (OPTIONAL) Set the geoTIFF key of the same name to
	                     this value. If not provided, the key is not changed.

This program expects to be given the working directory as the 1st command line
argument; all other arguments are ignored. It also assumes that it is being run
in the working directory.

revision history:
	20170814 jhansford, creation (ENTR-4006)
	20170915 jhansford, significantly rewritten (ENTR-4162)
	20171003 jhansford, moved code from main() into functions.
	20171116 jhansford, added validity check to ensure the latitude/longitude data in the file is valid and intersects with the desired region. (ENTR-4240)
	20171213 jhansford, added "reduce_data=False" to call to kd_tree.get_neighbour_info, to avoid executing code in the pyresample module that incorrectly filters out some of the input data.
	20180118 jhansford, modified the SQL query introduced in ENTR-4240 to use bind variables (ENTR-4475).
	20180420 jhansford, changed the database query to work with Postgres instead of Oracle (for NDE-in-the-cloud). Database hostname / credentials are hard-coded.
	20180514 jhansford, changed the database query to use the psycopg2 library, instead of the command line tool 'psql'. Database hostname / credentials are still hard-coded.
"""

from util import readPCF, readPSF, writePSF, setuplogger, readNC4, scaleData
from remapSatelliteData import getProj4GeometryDefinition, processFillValues
from convertImage import createGeoTiff
from pyresample import geometry, kd_tree
from pyproj import Proj
import numpy
import os
import sys
import logging
import subprocess
import string
import re
import psycopg2

LISTGEO_FILENAME_POSTFIX = 'listgeo.txt'
SQL_QUERY_INPUT_PIPE = 'sqlplus_query.pipe'
SQL_QUERY_RESULT_PIPE = 'sqlplus_result.pipe'

log = logging.getLogger("formatting");

def main(argv):
	try:
		workDir = argv[0]
	except Exception:
		log.exception("Invalid argument. Must be working Directory.")
		sys.exit(os.EX_USAGE)

	ops_dir = readPCF(workDir, "ops_dir")
	nc4_filename = readPSF(workDir)

	# Clear the PSF (prevents NDE from trying to ingest the .nc file if this job fails or succeeds but does not produce any images.
	writePSF(workDir, "", overwrite='yes', close=False)

	# Read data and get pyresample data geometry definitions.
	vars, proj_geom_def, grid_info, ll_x, ur_y, data_geom_def = getNC4VariablesAndGeometryDefinitions(workDir, ops_dir, nc4_filename)

	# Get desired layers and corresponding output filenames
	layer_names, output_filenames = getOutputLayersAndFilenames(workDir, nc4_filename)

	# Make the images
	imagesMade = regridAllImages(workDir, ops_dir, vars, proj_geom_def, grid_info, ll_x, ur_y, data_geom_def, layer_names, output_filenames, overwrite_psf='no')

	# Close PSF and finish
	writePSF(workDir, "", overwrite='no', close=True)
	log.info("Finished successfully.")


def editGeoTiffKeys(geotiff_filename, workDir, ops_dir, gtCitationGeoKey=None, geogCitationGeoKey=None):
	"""
	Sets the geoTIFF keys "GTCitationGeoKey" and "GeogCitationGeoKey" on the specified geoTIFF file.
	If one of those arguments is not provided, or are set to none, then the corresponding key is left unchanged.
	Returns the status code of whatever executable failed, or os.EX_OK if everything went fine.
	"""
	# Run listgeo on the file.
	log.info("Running `listgeo " + geotiff_filename + "`")
	try:
		listgeo_result = subprocess.check_output(["listgeo", geotiff_filename])
	except subprocess.CalledProcessError as err:
		log.exception("listgeo returned a non-zero exit code: " + str(err.returncode) + ". Function returning failure.")
		return err.returncode

	listgeo_result = str(listgeo_result)
	lines = listgeo_result.split('\n')

	# Edit the geoTIFF keys
	for i in xrange(0, len(lines)):
		if gtCitationGeoKey is not None and "GTCitationGeoKey" in lines[i]:
			lines[i] = lines[i][0:lines[i].index("GTCitationGeoKey")] + 'GTCitationGeoKey (Ascii,' + str(len(gtCitationGeoKey)+1) + '): "' + gtCitationGeoKey + '"'
		elif geogCitationGeoKey is not None and "GeogCitationGeoKey" in lines[i]:
			lines[i] = lines[i][0:lines[i].index("GeogCitationGeoKey")] + 'GeogCitationGeoKey (Ascii,' + str(len(geogCitationGeoKey)+1) + '): "' + geogCitationGeoKey + '"'

	# Write the modified listgeo information to a file.
	listgeo_file = geotiff_filename.split('.')[0] + "_" + LISTGEO_FILENAME_POSTFIX
	log.info("Writing (possibly edited) listgeo output to " + listgeo_file)
	with open(listgeo_file, 'w') as fd:
		fd.write(string.join(lines, '\n'))

	# Rename the GeoTiff file in preparation to run geotifcp_inc_gdal
	temporary_filename = "temp_" + geotiff_filename
	log.info("Renaming '" + geotiff_filename + "' to '" + temporary_filename + "'.")
	os.rename(geotiff_filename, temporary_filename)

	# Run the modified geotifcp to copy the GDAL tags while also changing the geoTIFF tags
	log.info("Running geotifcp_inc_gdal.")
	geotifcpStatus = subprocess.call([ops_dir + "/bin/geotifcp_inc_gdal", "-g", listgeo_file, temporary_filename, geotiff_filename])
	if geotifcpStatus != 0:
		log.exception("geotifcp_inc_gdal returned a non-zero exit code: " + str(geotifcpStatus) + ". Function returning failure.")
		return geotifcpStatus

	return os.EX_OK


def getNC4VariablesAndGeometryDefinitions(workDir, ops_dir, nc4File):
	"""
	Reads the netCDF-4 file named in the PSF file and computes the geometry definitions for both the data and the desired grid.
	Returns a tuple with the following six elements.
	  0. vars: A hash containing the data read from the netCDF-4 file.
	  1. proj_geom_def: The geometry definition for the target grid.
	  2. grid_info: a dictionary - see util.py::parseProjString for details
	  3-4. ll_x, ur_y: The lower left X coordinate and upper right Y coordinate of the projection geometry definition. (I.e.: the upper left corner).
	  5. data_geom_def: The geometry definition for the data.
	"""
	# read the intermediate netCDF4 file and obtain the latitude/longitude data
	log.info("Reading NC4 file.")
	vars, meta = readNC4(nc4File)
	for variable_name in vars.iterkeys():
		if re.match('lat', variable_name, re.I):
			lat = vars[variable_name]["data"]
		elif re.match('lon', variable_name, re.I):
			lon = vars[variable_name]["data"]

	checkInputValidity(workDir, meta)

	# Obtain the data's geometry definition for pyresample.
	log.info("Obtaining input data geometry definition.")
	data_geom_def = geometry.SwathDefinition(lons=lon, lats=lat)

	# Read in the proj4 string and obtain the grid's geometry definition for pyresample
	log.info("Obtaining target grid geometry definition.")
	proj4_string = readPCF(workDir, "grid")
	proj_geom_def, grid_info, proj4_dict, p, ll_x, ll_y, ur_x, ur_y = getProj4GeometryDefinition(proj4_string, ops_dir, data_geom_def)

	return vars, proj_geom_def, grid_info, ll_x, ur_y, data_geom_def


def getOutputLayersAndFilenames(workDir, nc4_intermediate_filename):
	"""
	Returns a tuple containing two elements:
	  0. layer_names: An array of strings containing the layers that are requested in the PCF
	  1. output_filenames: A dictionary mapping (layer_name -> output_filename)
	"""
	# Determine which layer(s) of the netCDF file to output as images, and the corresponding filenames.
	log.info("Determining output layer(s) and filename(s).")
	layer_names = readPCF(workDir, "layer_name").split(',')
	output_filename_prefixes = readPCF(workDir, "output_filename_prefix").split(',')

	if len(layer_names) != len(output_filename_prefixes):
		log.exception("Invalid arguments supplied to PCF. The number of layer_name(s) and output_filename_prefix(es) specified are not the same.")
		sys.exit(os.EX_USAGE)

	output_filenames = {}
	VERSION_NO = "v1r0"

	h5_input_filename = readPCF(workDir, "input1")
	match = re.search(".*_d\d{8}_t\d{7}_e\d{7}_b(\d*)_c.*\.h5", h5_input_filename)
	if match is not None:
		orbit_id = match.group(1)
	else:
		log.exception("Failed to determine output filename(s); couldn't parse IDPS .h5 filename. Exiting with failure.")
		sys.exit(os.EX_SOFTWARE)

	match = re.search(".*_(.*)_s(\d{15})_e(\d{15})_c(\d{15})\.nc", nc4_intermediate_filename)
	if match is not None:
		platform_name = match.group(1)
		start_time = match.group(2)
		end_time = match.group(3)
		creation_time = match.group(4)		
	else:
		log.exception("Failed to determine output filename(s); couldn't parse intermediate .nc filename. Exiting with failure.")
		sys.exit(os.EX_SOFTWARE)

	for i in xrange(0, len(layer_names)):
		output_filenames[layer_names[i]] = output_filename_prefixes[i] + "-b" + orbit_id + "_" + VERSION_NO + "_" + platform_name + "_s" + start_time + "_e" + end_time + "_c" + creation_time + ".tif"

	return layer_names, output_filenames


def regridAllImages(workDir, ops_dir, vars, proj_geom_def, grid_info, ul_x, ul_y, data_geom_def, layer_names, output_filenames, overwrite_psf='yes', scale_data_types=None, scale_data_ranges=None):
	"""
	Uses pyresample to regrid all of the images. Each image that is created is written to the PSF. If one or 
	more images cannot be created, they will be skipped, but the other images will be created if possible.

	  Parameters:
	    workDir: 
	    vars: 
	    proj_geom_def:
	    grid_info:
	    ul_x:
	    ul_y:
	    data_geom_def:
	    layer_names: The names of the layers (in the vars dictionary) to convert into geoTIFF format.
	    output_filenames: A dictionary, mapping (layer_name -> output_filename).
		overwrite_psf: A If not specified, defaults to yes.
		scale_data_types: A dictionary mapping (layer_name -> data_type).
		scale_data_ranges: A dictionary mapping (layer_name -> tuple(min_valid_value_in_layer, max_valid_value_in_layer))
	
	  Returns: 
	    True if any images were written to the PSF, False if not.
	"""

	if not (overwrite_psf == 'yes' or overwrite_psf == 'no'):
		log.exception("Invalid value specified for overwrite_psf: '" + str(overwrite_psf) + "'. Must be 'yes' or 'no'.")

	# compute the information needed to re-project the data based on the input and output geometry definitions.
	log.info("Calculating re-gridding based on lat/lon information.")
	resampleRadius = float(readPCF(workDir, "resampleRadius"))
	valid_input_index, valid_output_index, index_array, distance_array = kd_tree.get_neighbour_info(data_geom_def, proj_geom_def, resampleRadius, neighbours=1, reduce_data=False)

	# Actually reproject the images, using the information computed above.
	# If one image fails to be re-projected, this script will return failure, but will still attempt to convert the others if possible.
	gtCitationGeoKey = readPCF(workDir, "GTCitationGeoKey")
	geogCitationGeoKey = readPCF(workDir, "GeogCitationGeoKey")
	last_failure_status = os.EX_OK
	for layer in layer_names:
		if not layer in vars:
			log.warning("The layer '" + layer + "' was not found in the NC4 file. Skipping.")
			continue
		output_filename = output_filenames[layer]
		original_data = vars[layer]["data"]

		fill_value = processFillValues(vars, layer, original_data)
		if numpy.sum(original_data == fill_value) == (original_data.shape[0] * original_data.shape[1]):
			log.info("The input layer '" + layer + "' is all fill values. Skipping.")
			continue

		log.info("Regridding layer: '" + layer + "'")
		resampled_data = kd_tree.get_sample_from_neighbour_info('nn', proj_geom_def.shape, original_data, valid_input_index, valid_output_index, index_array, fill_value=fill_value)

		if numpy.sum(resampled_data == fill_value) == (resampled_data.shape[0] * resampled_data.shape[1]):
			log.warning("Output file: '" + output_filename + "' was not produced. The result of re-sampling was all fill values. The input data probably missed the grid.")
			continue
		
		# If requested, do rescaling of the data.
		if scale_data_types is not None:
			if scale_data_ranges is not None:
				resampled_data, fill_value = scaleData(resampled_data, fill_value, scale_data_types[layer], min_in=scale_data_ranges[layer][0], max_in=scale_data_ranges[layer][1])
			else:
				resampled_data, fill_value = scaleData(resampled_data, fill_value, scale_data_types[layer])

		log.info("Creating geoTIFF file: '" + output_filename + "'.")
		createGeoTiff(output_filename, resampled_data, grid_info['proj4_str'], [grid_info['pixel_size_x'], grid_info['pixel_size_y']], [ul_x, ul_y])

		# Edit the GeoTIFF keys.
		editStatus = editGeoTiffKeys(output_filename, workDir, ops_dir, gtCitationGeoKey=gtCitationGeoKey, geogCitationGeoKey=geogCitationGeoKey)
		if editStatus != os.EX_OK:
			last_failure_status = editStatus
		else:
			writePSF(workDir, output_filename, overwrite=overwrite_psf, close=False)
			overwrite_psf = 'no'

	if last_failure_status != os.EX_OK:
		log.exception("There was an error creating one or more of the geoTIFF output products. Exiting with failure.")
		sys.exit(last_failure_status)

	return (overwrite_psf == 'no')


def checkInputValidity(workDir, meta):
	"""
	Checks to see if the input is from a polar orbiting satellite, contains valid latitude/longitude values, 
	and intersects with the region defined by the Gazetteer for the production rule of this job.
	This function does not return anything. It exits if the input is invalid, so if it returns, then all validity checks were passed.

	The 'meta' parameter is the metadata object returned by util.py's readNC4 function
	"""
	# Checks for validity of input data
	if meta['orbit'] != 'polar':
		log.exception("Input data was not from a polar orbiting satellite. Exiting with failure.")
		sys.exit(os.EX_SOFTWARE)

	for latitude_bound in ["north_bound", "south_bound"]:
		if meta[latitude_bound] < -90.0 or meta[latitude_bound] > 90.0:
			log.exception("The file " + nc4File + " contains an invalid latitude value: " + str(meta[latitude_bound]))
			sys.exit(os.EX_SOFTWARE)

	for longitude_bound in ["east_bound", "west_bound"]:
		if meta[longitude_bound] < -180.0 or meta[longitude_bound] > 180.0:
			log.exception("The file " + nc4File + " contains an invalid longitude value: " + str(meta[longitude_bound]))
			sys.exit(os.EX_SOFTWARE)

	# TODO: Remove hard-coded password argument to 'connect' function
	database_connection = psycopg2.connect(host="nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com", user="nde_dev1", password="nde")
	result = None
	
	with database_connection:
		with database_connection.cursor() as cursor:
			cursor = database_connection.cursor()
			sql_query = "SELECT COUNT(gz.GZID) FROM PRODUCTIONJOB pj, PRODUCTIONJOBSPEC pjs, PRODUCTIONRULE pr, GAZETTEER gz WHERE " \
				+ "pj.PRJOBID = %(jobId)s and " \
				+ "pjs.PRODPARTIALJOBID = pj.PRODPARTIALJOBID and " \
				+ "pr.PRID = pjs.PRID and " \
				+ "gz.GZID = pr.GZID and " \
				+ "ST_Intersects (gz.GZLOCATIONSPATIAL, st_geogFromText('POLYGON((%(eastBound)s %(northBound)s, %(westBound)s %(northBound)s, %(westBound)s %(southBound)s, %(eastBound)s %(southBound)s, %(eastBound)s %(northBound)s ))'));"

			jobId = workDir.split('/')[-1]
			sql_query_parameters = {'jobId': int(jobId), 'eastBound':float(meta["east_bound"]), 'westBound':float(meta["west_bound"]), 'northBound':float(meta["north_bound"]), 'southBound':float(meta["south_bound"])}

			log.info("The query being sent is: " + cursor.mogrify(sql_query, sql_query_parameters))

			cursor.execute(sql_query, sql_query_parameters)
			result = cursor.fetchone()

			if result is None:
				log.error("The SQL query did not return anything.")
				sys.exit(os.EX_SOFTWARE)
			else:
				result = int(result[0])
				log.info("The result of the query is: %d" % result)

	database_connection.close()

	if result is None:
		log.error("The SQL query somehow failed. Exiting with failure.")
		sys.exit(os.EX_SOFTWARE)
	elif result != 1:
		log.warning("Bounding box of input data did not intersect the gazetteer for this production rule. No output will be produced. Exiting with success.")
		sys.exit(os.EX_OK)


if __name__ == "__main__":
	setuplogger(sys.argv[1])
	log.debug(sys.argv)
	main(sys.argv[1:])

