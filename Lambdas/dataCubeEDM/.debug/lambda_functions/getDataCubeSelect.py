"""
Author: Jonathan Hansford, Peter MacHarrie, Hieu Phung; SOLERS INC
Contact: hieu.phung@solers.com
Last modified: 2019-01-02
Python Version: 3.6


This lambda function processes ad-hoc requests to dss.
The ad-hoc request is handled by creating an on-demand job with the appropriate odJobSpec.

The input body should take the following form:
{
    "select": {
        "measures": [
            {
                "name":
                "from":
                (OPTIONAL) "outputArrayName":
                (OPTIONAL) "outputDimName":
                (OPTIONAL) "transform":
                (OPTIONAL) "bitOffset":
                (OPTIONAL) "bitLength":
                (OPTIONAL) "applyScale":
            },
            ... (one or more measures, as many as desired)
        ],
        (OPTIONAL) "where": {
            (IF "where" is provided, at least one of "subSample" and "filter" must be included)

            "subSample": {
                "dimensions": [
                    {
                        "name":
                        "start":
                        "stride":
                        "count":
                    },
                    ... (one or more dimensions, as desired)
                ]
            }
            "filter": {
                "args": [
                    {
                        "measureName":
                        "from":
                        "operator": 'lt' | 'le' | 'gt' | 'ge' | 'eq' | 'ne' | 'between' | 'in'
                        "operands": <A string, a comma seperated list of one or more numbers> 
                    },
                    ... (one or more args, as desired)
                ]
            }
        }
    }
    "inputSpec": {
        "satellite": 
        
        (Note: At least one of "startTime" and "startOrbitNumber" must be provided, and at least one of "endTime" and "endOrbitNumber" must be provided)
        "startTime": <A string specifying a date and time, in ISO 8601 format>
        "endTime": <A string specifying a date and time, in ISO 8601 format>
        "startOrbitNumber": <A number>
        "endOrbitNumber": <A number>
        
        (OPTIONAL) "spatialArea": <A string, to be fed into st_geogFromText. Probably should be of the form POLYGON(( lon1 lat1, lon2 lat2, ... lonN latN, lon1 lat1))>
        (OPTIONAL) "dayNightFlag": [ 'day' | 'night' | 'both', (multiple entries as desired) ] 
        (OPTIONAL) "ascDescIndicator": [ 0 | 1 | 2, (multiple entries as desired) ]
    }
    "outputSpec": {
        "productName": <a string that will be used in the output filename>
        "fileType": 'netcdf' | 'jpeg' | 'tiff' | 'gif' | 'bmp' | 'png' | 'geotiff'
        
        (OPTIONAL. If "fileType" is 'geotiff', then "mapProjection" must be provided. If "mapProjection" is provied, then latitude and longitude measures must be selected.)
        "mapProjection": <Name of a grid defined in predefinedProjections.txt> | <A proj4 string>
        
        (OPTIONAL. Must be provided if and only if "mapProjection" is provided)
        "mapResolution": <Resolution of grid to remap the data to, in the units of the projection you specify (typically meters)>
        
        (OPTIONAL. May be specified only if "fileType" == 'netcdf' and "mapProjection" is not provided)
        (Note: remapSatelliteData.py does not check this parameter...)
        "nc4_compression_level": <An integer in the range 0 through 9 inclusive.>
        
        (OPTIONAL. May be specified only if "fileType" != 'netcdf' or "mapProjection" is provided)
        "viirs_mender": 'IMG' | 'MOD'
        
        (OPTIONAL. May be specified only if "fileType" == 'jpeg' | 'png' | 'bmp' | 'gif' | 'tiff')
        "invert": 'yes' | 'no'
        
        (OPTIONAL. May be specified only if "mapProjection" is provided and "fileType" == 'netcdf' | 'geotiff')
        (Note: Other image formats are always written as 8 bits)
        "data_type": 'int8' | 'uint8' | 'int16' | 'uint16' | 'int32' | 'uint32' | 'int64' | 'uint64' | 'float32' | 'float64'
        
        (OPTIONAL. May be specified only if "mapProjection" is provided. If not provided, the resampleRadius will be set based on an estimate of the input file resolution.)
        "resampleRadius": <resample radius to use for the nearest-neighbor regridding, in meters>
    }
}

The response object takes the form:
{
    "jobId": <an integer>
}
"""
import datetime
import json
import math
import os
import re
import sys

import boto3
import psycopg2
import pyproj

import lambda_functions.createJob as createJob

PREDEFINED_PROJECTIONS_FILENAME = 'predefinedProjections.txt'

def lambda_handler(event, context):
    print("Starting up. Event is %s" % str(event))
    
    inputBody = None
    if 'resource' in event:
        if event['body'] is None:
            pass # TODO: error
        elif isinstance(event['body'], dict):
            inputBody = event['body']
        else:
            inputBody = json.loads(event['body'])
    else:
        inputBody = event
        
    # Note: this is a dict
    odJobSpec = getOdJobSpec(inputBody)
    
    # For now, invoke create job using local python code, can't invoke lambda inside VPC
    #response = invokeCreateJobLambda(odJobSpec)
    cJobBodyObj = invokeCreateJobLibrary(odJobSpec, context)
    
    response = {
        "isBase64Encoded": False,
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps({
            "message": "success, created on demand job",
            **cJobBodyObj
        })
    }

    return response


def getOdJobSpec(inputBody):
    odJobSpec = {
        'algorithm': {
            'name': 'dss ad-hoc',
            'version': '1.0'
        },
        'outputs': {
            'fileNamePrefix': inputBody['outputSpec']['productName']
        },
        'prDataSelectionJSON': inputBody['select'],
        'jobType': 'onDemand',
        'satellite': inputBody["inputSpec"]["satellite"]
    }
    print("In getOdJobSpec. Attempting to connect to database.")
    databaseConnection = psycopg2.connect(host=os.environ['RD_HOST'], dbname=os.environ['RD_DBNM'], user=os.environ['RD_USER'], password=os.environ['RD_PSWD'])
    print("A database connection object was created.")
    
    inputProductIds = find_input_products(databaseConnection, inputBody["inputSpec"]["satellite"], inputBody["select"],)
    
    print ("inputProductIds=", inputProductIds)
    
    inputProductIdsToFileIds = find_input_files(databaseConnection, inputProductIds, inputBody["inputSpec"])
    
    print ("inputProductIdsToFileIds=", inputProductIdsToFileIds)
    
    odJobSpec['inputs'] = []
    
    for productId, fileIds in inputProductIdsToFileIds.items():
        odJobSpec['inputs'].append({
            'prisFileHandle': 'input',
            'prisFileHandleNumbering': 'N',
            'productId': productId,
            'fileIds': fileIds
        })
    
    jobCoverageStart, jobCoverageEnd = find_adhoc_job_coverage_interval(databaseConnection, inputProductIdsToFileIds)
    
    odJobSpec['obsStartTime'] = jobCoverageStart.isoformat()
    odJobSpec['obsEndTime'] = jobCoverageEnd.isoformat()
    
    odJobSpec['parameters'] = getOdJobSpecParameters(databaseConnection, inputBody, inputProductIdsToFileIds)


    # Add productShortName to inputspec
    for input in odJobSpec['inputs']:
        myProductId = input['productId']

        with databaseConnection:
            with databaseConnection.cursor() as database_cursor:
                database_cursor.execute("""
                    SELECT
                        productShortName
                        
                    FROM
                        productdescription
                    WHERE
                        productId = %s
                """, (myProductId,))
            
                row = database_cursor.fetchone()
                if row == None:
                    raise Exception("Failed to obtain productShortName.")
                else:
                    input['productShortName']=row[0]

        
    return odJobSpec
    

def getOdJobSpecParameters(databaseConnection, inputBody, inputProductIdsToFileIds):
    parameters = {}
    
    latitudeMeasureName = None
    longitudeMeasureName = None
    
    # Add netCDF global attributes, to all netCDF files (remapped or not)
    if inputBody['outputSpec']['fileType'] == 'netcdf':
        parameters['title'] = inputBody['outputSpec']['productName']
        parameters['satellite_name'] = inputBody['inputSpec']['satellite']

    # Added, cheat for plotter and geoPlotter.py formatters
    
#    if 'formatter' in $inputBody['outputSpec']:
#        parameters['formatter']=$inputBody['outputSpec']['formatter']
        
    # Add parameters relevant to convertImage.py and remapSatelliteData.py, if either script is going to be called
    if inputBody['outputSpec']['fileType'] != 'netcdf' or 'mapProjection' in inputBody['outputSpec']:
        parameters['ops_dir'] = '/opt/apps/nde/' + os.environ['NDEMODE'] + '/pgs/dss/formatter'
        
        if inputBody['outputSpec']['fileType'] != 'netcdf':
            parameters['file_format'] = inputBody['outputSpec']['fileType']
        else:
            parameters['file_format'] = 'nc4'

        if 'viirs_mender' in inputBody['outputSpec']:
            parameters['viirs_mend'] = inputBody['outputSpec']['viirs_mender']
        if 'invert' in inputBody['outputSpec']:
            parameters['invert'] = inputBody['outputSpec']['invert']
            
        # The layer name parameter must be inferred from the output measure name. The layer of interest is the layer that isn't latitude or longitude.
        # Also, grab the latitude and longitude measure names, if they are available, for later use. 
        # The regular expression used to identify Latitude/Longitude fields is the same as the ones used by remapSatelliteData.py)
        layerName = None
        for measureObject in inputBody['select']['measures']:
            
            measureObjectLayerName = None
            if 'outputArrayName' in measureObject:
                measureObjectLayerName = measureObject['outputArrayName']
            else:
                measureObjectLayerName = measureObject['name']
            
            if re.match('lat', measureObjectLayerName, re.I) is not None:
                if latitudeMeasureName is None:
                    latitudeMeasureName = measureObjectLayerName
                else:
                    raise Exception("Multiple latitude measures in output file (%s and %s). Specify the outputArrayName to disambiguate if necessary." % (latitudeMeasureName, measureObjectLayerName))
            elif re.match('lon', measureObjectLayerName, re.I) is not None:
                if longitudeMeasureName is None:
                    longitudeMeasureName = measureObjectLayerName
                else:
                    raise Exception("Multiple longitude measures in output file (%s and %s). Specify the outputArrayName to disambiguate if necessary." % (longitudeMeasureName, measureObjectLayerName))
            else: # This is not a latitude or longitude layer.
                if layerName is None:
                    layerName = measureObjectLayerName
                else:
                    raise Exception("Multiple layers in output file (%s and %s). Cannot infer output layer for file type %s." % (layerName, measureObjectLayerName, inputBody['outputSpec']['fileType']))
                    
        if layerName is None:
            raise Exception("Could not find an output layer.")
        else:
            parameters['layer_name'] = layerName
    
    
    if 'mapProjection' not in inputBody['outputSpec']:
        # Compression level for netCDF is only configurable for non-remapped files; otherwise, it will be set to 1 by remapSatelliteData.py
        if inputBody['outputSpec']['fileType'] == 'netcdf': 
            if 'nc4_compression_level' in inputBody['outputSpec']:
                parameters['nc4_compression_flag'] = 'on'
                parameters['compression_level'] = inputBody['outputSpec']['nc4_compression_level']
                
        # Converting file to some image format, without remapping, must call convertImage.py
        else:
            if 'formatter' in inputBody['outputSpec']:
                parameters['formatter'] = inputBody['outputSpec']['formatter']
            else:
                parameters['formatter'] = 'convertImage.py'
    
    # Reprojecting data, must call remapSatelliteData.py, and add the grid and resampleRadius parameters.
    else:
        parameters['formatter'] = 'remapSatelliteData.py'
        
        if 'data_type' in inputBody['outputSpec']:
            parameters['dataType'] = inputBody['outputSpec']['data_type']
        
        if 'resampleRadius' in inputBody['outputSpec']:
            parameters['resampleRadius'] = str(inputBody['outputSpec']['resampleRadius'])
        else:
            # Resample radius is the length of the diagonal of the square formed by one grid cell.
            parameters['resampleRadius'] = str(guessInputResolution(databaseConnection, latitudeMeasureName, inputProductIdsToFileIds) * math.sqrt(2.0))
        
        gridName = inputBody['outputSpec']['mapProjection']
        proj4String = getPredefinedProjection(gridName)
        if proj4String is None:
            gridName = 'custom_grid'
            proj4String = inputBody['outputSpec']['mapProjection']
        parameters['grid'] = "{0}, proj4, {1}, None, None, {2}, {3}, None, None, None, None".format(gridName, proj4String, inputBody['outputSpec']['mapResolution'], -inputBody['outputSpec']['mapResolution'])
        
        
    return parameters


def getPredefinedProjection(targetName):
    """
    Reads predefinedProjections.txt to determine the grid with the specified name.
    If there is no grid with that name specified, return None.
    
    The projections file are stored as a comma separated pair (one pair per line)
    name, proj4 string
    """
    with open(os.environ['LAMBDA_TASK_ROOT'] + '/' + PREDEFINED_PROJECTIONS_FILENAME, 'r') as gridConfigFd:
        for line in gridConfigFd:
            if re.match('^' + re.escape(targetName), line, re.IGNORECASE):
                return line.split(',')[1].strip() 
    return None


def guessInputResolution(database_connection, latitudeMeasureName, product_id_to_file_ids):
    """
    
    targetProjection is a pyproj.Proj object
    """
    
    allInputFileIds = []
    numberOfGranules = 0 # The # of granules being used is set to the maximum number of files available for any of the input products.
    for file_ids in product_id_to_file_ids.values():
        for fileId in file_ids:
            allInputFileIds.append(fileId)
        if len(file_ids) > numberOfGranules:
            numberOfGranules = len(file_ids)
            
    dimension_sizes = None
    fileSpatialAreaText = None
    
    with database_connection:
        with database_connection.cursor() as database_cursor:
            # Find the dimensions of the latitude measure array (should be a 2-d array)
            database_cursor.execute("""
                SELECT 
                    ed.E_DIMENSIONSTORAGESIZE
                FROM
                    ENTERPRISEMEASURE em,
                    ENTERPRISEDIMENSIONLIST edl,
                    ENTERPRISEORDEREDDIMENSION eod,
                    ENTERPRISEDIMENSION ed,
                    MEASURE_H_ARRAY_XREF mhax,
                    HDF5_ARRAY ha,
                    HDF5_GROUP hg
                WHERE
                    em.MEASURENAME = %(enterpriseMeasureName)s and
                    em.E_DIMENSIONLISTID = edl.E_DIMENSIONLISTID and
                    edl.E_DIMENSIONLISTID = eod.E_DIMENSIONLISTID and
                    eod.E_DIMENSIONID = ed.E_DIMENSIONID and
                    em.MEASUREID = mhax.MEASUREID and
                    mhax.H_ARRAYID = ha.H_ARRAYID and
                    ha.H_GROUPID = hg.H_GROUPID and
                    hg.PRODUCTID in %(inputProductIds)s
                UNION
                SELECT 
                    ed.E_DIMENSIONSTORAGESIZE
                FROM
                    ENTERPRISEMEASURE em,
                    ENTERPRISEDIMENSIONLIST edl,
                    ENTERPRISEORDEREDDIMENSION eod,
                    ENTERPRISEDIMENSION ed,
                    MEASURE_N_ARRAY_XREF mnax,
                    NC4_ARRAY na,
                    NC4_GROUP ng
                WHERE
                    em.MEASURENAME = %(enterpriseMeasureName)s and
                    em.E_DIMENSIONLISTID = edl.E_DIMENSIONLISTID and
                    edl.E_DIMENSIONLISTID = eod.E_DIMENSIONLISTID and
                    eod.E_DIMENSIONID = ed.E_DIMENSIONID and
                    em.MEASUREID = mnax.MEASUREID and
                    mnax.N_ARRAYID = na.N_ARRAYID and
                    na.N_GROUPID = ng.N_GROUPID and
                    ng.PRODUCTID in %(inputProductIds)s
            """, {'enterpriseMeasureName': latitudeMeasureName, 'inputProductIds': tuple(product_id_to_file_ids.keys()) })
            
            dimensions_for_measure = database_cursor.fetchall()
            if len(dimensions_for_measure) != 2:
                raise Exception("Cannot infer resampleRadius. Measure %s is %d-dimensional (must be 2-dimensional)." % (latitudeMeasureName, len(dimensions_for_measure)))
            else:
                dimension_sizes = [(dimensions_for_measure[0])[0], (dimensions_for_measure[1])[0]]
            
            # Find input products which have geospatial metadata available for all granules.
            database_cursor.execute("""
                SELECT
                    PRODUCTID,
                    COUNT(FILEID)
                FROM
                    FILEMETADATA
                WHERE
                    FILEID in %(fileIdsList)s and
                    FILESPATIALAREA is not null
                GROUP BY
                    PRODUCTID
                HAVING
                    COUNT(FILEID) = %(numberOfGranules)s
            """, { 'numberOfGranules': numberOfGranules, 'fileIdsList': tuple(allInputFileIds) })
            
            productsWithGeospatial = database_cursor.fetchall()
            
            # Of those products, choose the one with the lowest productID, to be consistent across different executions. 
            minimumProductIdWithGeospatial = None
            if len(productsWithGeospatial) == 0:
                raise Exception("No product contained geospatial metadata for all input granules.")
            else:
                minimumProductIdWithGeospatial = (productsWithGeospatial[0])[0]
                for i in range(1, len(productsWithGeospatial)):
                    if (productsWithGeospatial[i])[0] < minimumProductIdWithGeospatial:
                        minimumProductIdWithGeospatial = (productsWithGeospatial[i])[0]
            
            # Now, get the geospatial metadata from an input file. Always choose the minimum fileId, to be consistent across different executions.
            fileIdToGetGeospatialFor = min(product_id_to_file_ids[minimumProductIdWithGeospatial])
            database_cursor.execute("""
                SELECT
                    ST_AsText(FILESPATIALAREA)
                FROM
                    FILEMETADATA
                WHERE
                    FILEID = %(fileId)s
            """, { 'fileId': fileIdToGetGeospatialFor})
            
            fileSpatialAreas = database_cursor.fetchall()
            if len(fileSpatialAreas) != 1:
                raise Exception("Failed to obtain geospatial metadata for fileId: %s" % fileIdToGetGeospatialFor)
            else:
                fileSpatialAreaText = (fileSpatialAreas[0])[0]
    # end with database_connection
    
    # Parse the fileSpatialArea into an array, multipolygonPoints, consisting of tuples (longitude, latitude)
    multipolygonMatchObject = re.match(r'MULTIPOLYGON\(\(\(\s*(.+)\s*\)\)\)', fileSpatialAreaText)
    multipolygonPoints = []
    if multipolygonMatchObject is not None:
        coordinatePairs = multipolygonMatchObject.group(1).split(',')
        for coordinatePair in coordinatePairs:
            longitude, latitude = coordinatePair.strip().split(' ')
            longitude = float(longitude)
            latitude = float(latitude)
            if longitude < - 180 or longitude > 180:
                raise Exception("Invalid longitude in fileSpatialAreaText: %s" % fileSpatialAreaText)
            elif latitude < -90 or latitude > 90:
                raise Exception("Invalid latitude in fileSpatialAreaText: %s" % fileSpatialAreaText)
            else:
                multipolygonPoints.append((longitude, latitude))
    else:
        raise Exception("Could not parse fileSpatialAreaText: %s" % fileSpatialAreaText)
        
    # Compute the perimeter of the granule (the total distance around the edge) in meters. 
    geod = pyproj.Geod(ellps='WGS84')
    perimeter = 0
    for i in range(0, len(multipolygonPoints) - 1):
        forwardAzimuth, backAzimuth, distance = geod.inv((multipolygonPoints[i])[0], (multipolygonPoints[i])[1], (multipolygonPoints[i+1])[0], (multipolygonPoints[i+1])[1])
        perimeter += distance
        
    # Resolution is the perimeter in meters, divided by the perimeter in array cells: the distance between two adjacent array cells.
    # Note: this calculation assumes that the horizontal and vertical resolutions are the same. If they are not, the result is incorrect.
    resolution = perimeter / (2 * (dimension_sizes[0] + dimension_sizes[1]))
    
    print("Guessed resolution: %f" % resolution)
    
    return resolution


def find_input_products(database_connection, platform_name, select_statement):
    """
    Finds the products that will be needed to fulfill this tailoring request.
    Returns a set of the product IDs for those products.
    """
    product_ids_for_tailoring_request = set()
    measures_needed = set()
    
    measures_selected = select_statement['measures']
    
    for measure_object in measures_selected:
        measures_needed.add(measure_object['name'])
        
    if 'where' in select_statement:
        where_statement = select_statement['where']
        if 'filter' in where_statement:
            filter_args = where_statement['filter']['args']
            for filter_arg_object in filter_args:
                measures_needed.add(filter_arg_object['measureName'])
    if database_connection is None:
        raise Exception ("database_connection is None!")
    
    with database_connection:
        with database_connection.cursor() as database_cursor:
            for measure_needed in measures_needed:
                database_cursor.execute("""
                    SELECT
                        pd.PRODUCTID
                    FROM
                        ENTERPRISEMEASURE em,
                        MEASURE_H_ARRAY_XREF mhax,
                        HDF5_ARRAY ha,
                        HDF5_GROUP hg,
                        PRODUCTDESCRIPTION pd,
                        PRODUCTPLATFORM_XREF ppx,
                        PLATFORM p
                    WHERE
                        em.MEASURENAME = %(enterpriseMeasureName)s and
                        em.MEASUREID = mhax.MEASUREID and
                        mhax.H_ARRAYID = ha.H_ARRAYID and
                        ha.H_GROUPID = hg.H_GROUPID and
                        hg.PRODUCTID = pd.PRODUCTID and
                        pd.PRODUCTID = ppx.PRODUCTID and
                        ppx.PLATFORMID = p.PLATFORMID and
                        p.PLATFORMNAME = %(platformName)s
                    UNION 
                    SELECT
                        pd.PRODUCTID
                    FROM
                        ENTERPRISEMEASURE em,
                        MEASURE_N_ARRAY_XREF mnax,
                        NC4_ARRAY na,
                        NC4_GROUP ng,
                        PRODUCTDESCRIPTION pd,
                        PRODUCTPLATFORM_XREF ppx,
                        PLATFORM p
                    WHERE
                        em.MEASURENAME = %(enterpriseMeasureName)s and
                        em.MEASUREID = mnax.MEASUREID and
                        mnax.N_ARRAYID = na.N_ARRAYID and
                        na.N_GROUPID = ng.N_GROUPID and
                        ng.PRODUCTID = pd.PRODUCTID and
                        pd.PRODUCTID = ppx.PRODUCTID and
                        ppx.PLATFORMID = p.PLATFORMID and
                        p.PLATFORMNAME = %(platformName)s and
                        -- This portion of the query removes products which are outputs of dss (only the "original" product should be selected.)
                        pd.PRODUCTID not in (
                            SELECT
                                aop.PRODUCTID
                            FROM
                                ALGOOUTPUTPROD aop,
                                ALGORITHM a
                            WHERE
                                aop.ALGORITHMID = a.ALGORITHMID and
                                a.ALGORITHMNAME = 'dss'
                        )
                """, { 'enterpriseMeasureName': measure_needed, 'platformName': platform_name })
            
                product_ids_for_measure = database_cursor.fetchall()

                if len(product_ids_for_measure) == 0:
                    raise Exception("No products found for (measure, platform): %s, %s" % (measure_needed, platform_name))
                elif len(product_ids_for_measure) > 1:
                    raise Exception("Multiple products found for (measure, platform): %s, %s" % (measure_needed, platform_name))
                else:
                    product_ids_for_tailoring_request.add((product_ids_for_measure[0])[0])
    # end of with database_connection:

    return product_ids_for_tailoring_request


def find_input_files(database_connection, input_product_ids, input_spec):
    """
    Returns the file IDs matching the input product IDs and input spec.
    The return value is a dictionary, mapping the product short name to a list of file IDs.
    """
    with database_connection:
        with database_connection.cursor() as database_cursor:
            database_query = """
                SELECT
                    PRODUCTID, FILEID
                FROM
                    FILEMETADATA
                WHERE
                    PRODUCTID in %(productIdList)s
            """
            query_bind_variables = { 'productIdList': tuple(input_product_ids) }
            
            if 'startTime' in input_spec:
                database_query += " and FILEENDTIME > %(jobCoverageStart)s"
                query_bind_variables['jobCoverageStart'] = input_spec['startTime']
            
            if 'endTime' in input_spec:
                database_query += " and FILESTARTTIME < %(jobCoverageEnd)s"
                query_bind_variables['jobCoverageEnd'] = input_spec['endTime']
                
            if 'startOrbitNumber' in input_spec:
                database_query += " and FILEENDORBITNUM >= %(jobStartOrbitNum)s"
                query_bind_variables['jobStartOrbitNum'] = input_spec['startOrbitNumber']
                
            if 'endOrbitNumber' in input_spec:
                database_query += " and FILEBEGINORBITNUM <= %(jobEndOrbitNum)s"
                query_bind_variables['jobEndOrbitNum'] = input_spec['endOrbitNumber']
                
            if 'spatialArea' in input_spec:
                database_query += " and ST_Intersects(FILESPATIALAREA, st_geogFromText(%(jobSpatialArea)s))"
                query_bind_variables['jobSpatialArea'] = input_spec['spatialArea']
                
            if 'dayNightFlag' in input_spec:
                database_query += " and lower(FILEDAYNIGHTFLAG) in %(dayNightFlagList)s"
                query_bind_variables['dayNightFlagList'] = tuple(input_spec['dayNightFlag'])
                
            if 'ascDescIndicator' in input_spec:
                database_query += " and FILEASCDESCINDICATOR in %(ascDescIndicatorList)s"
                query_bind_variables['ascDescIndicatorList'] = tuple(input_spec['ascDescIndicator'])
                
            database_cursor.execute(database_query, query_bind_variables)
            
            database_result = database_cursor.fetchall()
            
            print ("query=", database_query, query_bind_variables)
            print ("query=", database_cursor.mogrify(database_query, query_bind_variables))
            
            product_id_to_file_ids = {}
            for row in database_result:
                product_id, file_id = row
                if product_id in product_id_to_file_ids:
                    product_id_to_file_ids[product_id].append(file_id)
                else:
                    product_id_to_file_ids[product_id] = [file_id]
                
            return product_id_to_file_ids


def find_adhoc_job_coverage_interval(database_connection, product_id_to_file_ids):
    """
    Finds the job coverage interval given the input File IDs.
    Returns a tuple (job_coverage_start, job_coverage_end)
    
    The job coverage interval is defined by the earliest and latest observation times among the input files.
    """
    file_ids = []
    
    for file_id_array in product_id_to_file_ids.values():
        for file_id in file_id_array:
            file_ids.append(file_id)
    
    with database_connection:
        with database_connection.cursor() as database_cursor:
            database_cursor.execute("""
                SELECT
                    MIN(FILESTARTTIME),
                    MAX(FILEENDTIME)
                FROM
                    FILEMETADATA
                WHERE
                    FILEID in %(fileIdList)s
            """, {'fileIdList': tuple(file_ids)})
            
            results = database_cursor.fetchall()
            if len(results) != 1:
                raise Exception("Failed to obtain earliest and latest observation times.")
            else:
                return ((results[0])[0], (results[0])[1])


# def invokeCreateJobLambda(odJobSpec):
#     """
#     Returns the result of the lambda function, as a dictionary
#     """
#     AWS_LAMBDA_CLIENT = boto3.client('lambda', region_name='us-east-1')

#     request_event = json.dumps({ 'body': odJobSpec })
#     print(request_event)
    
#     response = AWS_LAMBDA_CLIENT.invoke(
#         FunctionName = os.environ['CR8_JOB'],
#         InvocationType = 'RequestResponse',
#         LogType = "None",
#         Payload = request_event
#     )
#     print(response)
    
#     if response['StatusCode'] // 100 != 2:
#         raise Exception("Lambda call failed with status code: %d." % response['StatusCode'] )
#     else:
#         return json.loads(response['Payload'].read())
        

def invokeCreateJobLibrary(odJobSpec, context):
    request_event = {'body': odJobSpec}
    
    cJobResponse = createJob.lambda_handler(request_event, context)
    print("cJobResponse=" + str(cJobResponse))
    
    return json.loads(cJobResponse['body'])
    
    
    
    