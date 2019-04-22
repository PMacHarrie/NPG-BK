"""
    Author: Peter MacHarrie, Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import sys
from netCDF4 import Dataset
import numpy as np
import json


def getDims(d, v={}):
    v['dimensions'] = {}
    for dim in d.dimensions.keys():
        #       print dim, "size=", len(d.dimensions[dim])
        v['dimensions'][dim] = len(d.dimensions[dim])
    return v['dimensions']


def getGroupAttrs(d, v={}):
    v['attributes'] = {}
    for gattr in d.ncattrs():
        #       print "***** gattr=", gattr, "<<<<, getattr()=", getattr(d,gattr), "<<<<"
        v['attributes'][gattr] = str(getattr(d, gattr))
    #        print ("gattr=", gattr, "type=", type(edmDict['objectMetadata']['attributes'][gattr]))
    return v['attributes']


def getVars(d, v={}):
    v['variables'] = {}
    for var in d.variables:
        #       print var, d[var]
        # print "dtype=", str(d[var].dtype)
        #       print "dimensions=", d[var].dimensions
        # print "shape=", d[var].shape
        # print "scale=", d[var].scale
        # print "size=", d[var].size
        dimArray = []
        for dim in d[var].dimensions:
            dimArray.append(dim)

        shapeArray = []

        for shape in d[var].shape:
            shapeArray.append(int(shape))

        varJ = {
            var: {
                'datatype': str(d[var].dtype),
                'shape': shapeArray,
                'size': int(d[var].size),
                'dimensions': dimArray,
                'attributes': {}
            }
        }
        #       if d[var].size == 1:
        #               print "Array size = 1, array=", d[var][:]
        #               varJ[var]['data']=d[var][0]

        for attr in d[var].ncattrs():
            #               print "attribute=", attr
            #               print "value=", d[var].getncattr(attr)
            x = d[var].getncattr(attr)
            #               print "Type->", attr, "x1=", type(x)
            #               print "isinstance np.array->", attr, "x1=", isinstance(x, np.ndarray)
            y = x
            if isinstance(x, np.ndarray):
                y = x.tolist()
            #                       print "dtype =", x.dtype.type
            #                       print "dtype=", x.dtype.type
            #                       print "x=", x
            #                       for z in x:
            #                               if isinstance(z, np.int8):
            #                                       "it's an int16!!!!"
            #                                       y = int(z)
            #                               if isinstance(z, np.float32):
            #                                       "it's an int16!!!!"
            #                                       y = float(z)
            #                               print "z=", z
            else: # its not an array but it might be a type that is not json serializable
                if isinstance(y, np.uint8) or isinstance(y, np.uint16) or isinstance(y, np.uint32) or isinstance(y, np.uint64):
                    y = int(y)
                if isinstance(y, np.int8):
                    y = int(y)
                if isinstance(y, np.int16):
                    y = int(y)
                if isinstance(y, np.int32):
                    y = int(y)
                if isinstance(y, np.int64):
                    y = int(y)
                if isinstance(y, np.float32):
                    y = float(y)
                if isinstance(y, np.float64):
                    y = float(y)

            #               print "attr=", attr, "y=", y
            #               varJ[var]['attributes'].append({attr:y})

            varJ[var]['attributes'][str(attr)] = y

        #                if type(y) == "list":
        #                    for x in y:
        #                        print ("attr=", str(attr), "value type=", type(x))
        #                else:
        #                    print ("attr=", str(attr), "value type=", type(y))
        #               print "dtype of y=", type(y)
        # print varJ
        # print "***************"
        v['variables'][var] = varJ[var]

    # print "Dimensions:", edmDict['Dimensions']
    # for dim in edmDict['Dimensions']:
    #       print "DDDD", dim

    # print "Variables:"
    # for var in edmDict['Variables']:
    #       print "var=", var
    #       for elem in edmDict['Variables'][var]:
    #               print "***", elem, edmDict['Variables'][var][elem]

    # print "globalAttributes:"
    # for attr in edmDict['globalAttributes']:
    #       print "AAAA", attr, "=", edmDict['globalAttributes'][attr]

    return v['variables']


def walktree(d):
    values = d.groups.values()
    yield values
    # print ("values=", values)
    for value in d.groups.values():
        for children in walktree(value):
            yield children


def main(filePath):

    try:
        d = Dataset(filePath, 'r')
    
        edmDict = {
            'objectMetadata': {
                'attributes': {},
                'dimensions': {},
                'variables': {}
            }
        }
    
        # print ("d", d)
        # print ("*********")
    
        # Get root objects.
    
        edmDict['objectMetadata']['dimensions'] = getDims(d)
        edmDict['objectMetadata']['variables'] = getVars(d)
        edmDict['objectMetadata']['attributes'] = getGroupAttrs(d)
        edmDict['objectGroupAttrs'] = getGroupAttrs(d)
    
        # Get levels below root group.
    
        for x in walktree(d):
            for y in x:
                # print ("y.name=", y.name)
                edmDict['objectMetadata'][y.name] = {
                    "dimensions": {},
                    "variables": {},
                    "attributes": {}
                }
                r = getDims(y)
                edmDict['objectMetadata'][y.name]['dimensions'] = getDims(y)
                edmDict['objectMetadata'][y.name]['variables'] = getVars(y)
                tmp_attrs = getGroupAttrs(y)
                edmDict['objectMetadata'][y.name]['attributes'] = tmp_attrs
                for a in tmp_attrs:
                    edmDict['objectGroupAttrs'][a] = tmp_attrs[a]
    
        d.close()
    
        # For NUP spatial, implementation is inconsistent between NDE (dss) and NUPs, so
        # If these are present
        #        "geospatial_first_scanline_first_fov_lat":"-47.25",
        #        "geospatial_first_scanline_first_fov_lon":"60.99",
        #        "geospatial_first_scanline_last_fov_lat":"-42.41",
        #        "geospatial_first_scanline_last_fov_lon":"28.93",
        #        "geospatial_last_scanline_first_fov_lat":"-48.94",
        #        "geospatial_last_scanline_first_fov_lon":"60.86",
        #        "geospatial_last_scanline_last_fov_lat":"-43.96",
        #        "geospatial_last_scanline_last_fov_lon":"27.86",
        # THEN Create box (multi-polygon.)
        # ELSE
        # IF these are present:
        #      "g_ring_latitude":"[30.17313194 32.75935364 35.21888733 33.73912811 30.25631332 27.92463493\n 25.45748138 28.75181389 30.17313194]",
        #      "g_ring_longitude":"[12.90319252 13.15038395 13.38558578 30.03662682 45.75285721 44.67107773\n 43.6255455  28.64189148 12.90319252]",
        # THEN Create box (multi-polygon.)
        # ELSE Spatial = None.
    
        polyS = "SRID=4326;MULTIPOLYGON((("
        fileSpatialArea = False
        closedBoxStr = ""
    
        if "geospatial_first_scanline_first_fov_lat" in edmDict['objectMetadata']["attributes"]:
            fileSpatialArea = True
            closedBoxStr += edmDict['objectMetadata']["attributes"]["geospatial_first_scanline_first_fov_lon"] \
                + ' ' + \
                edmDict['objectMetadata']["attributes"]["geospatial_first_scanline_first_fov_lat"]
    
            closedBoxStr += ", " + \
                edmDict['objectMetadata']["attributes"]["geospatial_first_scanline_last_fov_lon"] \
                + " " + \
                edmDict['objectMetadata']["attributes"]["geospatial_first_scanline_last_fov_lat"]
    
            closedBoxStr += ", " + \
                edmDict['objectMetadata']["attributes"]["geospatial_last_scanline_last_fov_lon"] \
                + " " + \
                edmDict['objectMetadata']["attributes"]["geospatial_last_scanline_last_fov_lat"]
    
            closedBoxStr += ", " + \
                edmDict['objectMetadata']["attributes"]["geospatial_last_scanline_first_fov_lon"] \
                + " " + \
                edmDict['objectMetadata']["attributes"]["geospatial_last_scanline_first_fov_lat"]
    
            closedBoxStr += ", " + \
                edmDict['objectMetadata']["attributes"]["geospatial_first_scanline_first_fov_lon"] \
                + " " + \
                edmDict['objectMetadata']["attributes"]["geospatial_first_scanline_first_fov_lat"]
        elif "g_ring_latitude" in edmDict['objectMetadata']["attributes"] or "G-RING_LATITUDE" in edmDict['objectMetadata']["attributes"]:
            # ACSPO_SST uses "G-RING_LATITUDE" instead of "g_ring_latitude"
            fileSpatialArea=True
            #print ("wtf=",edmDict['objectMetadata']["attributes"]["g_ring_longitude"])
            tmpx=edmDict['objectMetadata']["attributes"].get("g_ring_longitude", edmDict['objectMetadata']["attributes"].get("G-RING_LONGITUDE")).replace("[", "")
            tmpx=tmpx.replace("]", "")
            tmpy=edmDict['objectMetadata']["attributes"].get("g_ring_latitude", edmDict['objectMetadata']["attributes"].get("G-RING_LATITUDE")).replace("[", "")
            tmpy=tmpy.replace("]", "")
            #print ("wtf=",edmDict['objectMetadata']["attributes"]["g_ring_longitude"])
            x=tmpx.split()
            y=tmpy.split()
            #print (x, len(x))
            #print (y, len(y))
            coordinate_string_list = []
            for i in range(len(x)):
                    coordinate_string_list.append(x[i] + " " + y[i])
            
            # If the last coordinate string does not equal the first, then the polygon is not closed.
            # If that is the case, we must close the polygon by adding another copy of the first coordinate pair to the end of the list
            if coordinate_string_list[len(coordinate_string_list) - 1] != coordinate_string_list[0]:
                    coordinate_string_list.append(coordinate_string_list[0])
            
            closedBoxStr += ', '.join(coordinate_string_list)
        elif "geospatial_lat_max" in edmDict['objectMetadata']["attributes"]:
            fileSpatialArea = True
            closedBoxStr += edmDict['objectMetadata']["attributes"]["geospatial_lon_min"] \
                + ' ' \
                + edmDict['objectMetadata']["attributes"]["geospatial_lat_min"]
                    
            closedBoxStr += ", " \
                + edmDict['objectMetadata']["attributes"]["geospatial_lon_max"] \
                + ' ' \
                + edmDict['objectMetadata']["attributes"]["geospatial_lat_min"]
            
            closedBoxStr += ", " \
                + edmDict['objectMetadata']["attributes"]["geospatial_lon_max"] \
                + ' ' \
                + edmDict['objectMetadata']["attributes"]["geospatial_lat_max"]
                    
            closedBoxStr += ", " \
                + edmDict['objectMetadata']["attributes"]["geospatial_lon_min"] \
                + ' ' \
                + edmDict['objectMetadata']["attributes"]["geospatial_lat_max"]
                    
            closedBoxStr += ", " \
                + edmDict['objectMetadata']["attributes"]["geospatial_lon_min"] \
                + ' ' \
                + edmDict['objectMetadata']["attributes"]["geospatial_lat_min"]
    
        # print ("Bbox=", closedBoxStr)
        if fileSpatialArea:
            polyS += closedBoxStr + ")))"
        else:
            polyS = None
        # print (polyS)
        # print ("n4=", json.dumps(edmDict, indent = 3))
    
        time_coverage_start_attribute = edmDict['objectMetadata']['attributes'].get('time_coverage_start', 
                                            edmDict['objectMetadata']['attributes'].get('TIME_COVERAGE_START'))
        time_coverage_end_attribute = edmDict['objectMetadata']['attributes'].get('time_coverage_end', 
                                            edmDict['objectMetadata']['attributes'].get('TIME_COVERAGE_END'))
                                            
        if time_coverage_start_attribute is None:
            raise Exception("Cannot obtain time_coverage_start!")
        elif time_coverage_end_attribute is None:
            raise Exception("Cannot obtain time_coverage_end!")
    
        edmDict['edmCore'] = {
            # 'fileName': filePath.split("/")[-1], # Will be populated later
            "platformNames": [],
            'fileStartTime': time_coverage_start_attribute,
            'fileEndTime': time_coverage_end_attribute,
            'fileSpatialArea': polyS, # need to convert to geoJSON (current cheat in persistIRO)
            'filebeginorbitnum': edmDict['objectMetadata']['attributes'].get('start_orbitnumber', 
                                        edmDict['objectMetadata']['attributes'].get('START_ORBIT_NUMBER')),
            'fileendorbitnum': edmDict['objectMetadata']['attributes'].get('end_orbit_number', 
                                        edmDict['objectMetadata']['attributes'].get('END_ORBIT_NUMBER')),
            'fileascdescindicator': edmDict['objectMetadata']['attributes'].get('ascend_descend_data_flag', 
                                            edmDict['objectMetadata']['attributes'].get('ASCEND_DESCEND_DATA_FLAG')),
            'fileDayNightFlag': edmDict['objectMetadata']['attributes'].get('day_night_data_flag', 
                                        edmDict['objectMetadata']['attributes'].get('DAY_NIGHT_DATA_FLAG'))
        }
        
        # vSQL = {
        #     "fileMetadata": {
                # "productId": None,
                # "fileSize": None,
                # "fileStartTime": time_coverage_start_attribute,
                # "fileEndTime": time_coverage_end_attribute,
                # "filebeginorbitnum": edmDict['objectMetadata']['attributes'].get('start_orbitnumber', 
                #                         edmDict['objectMetadata']['attributes'].get('START_ORBIT_NUMBER')),
                # "fileendorbitnum": edmDict['objectMetadata']['attributes'].get('end_orbit_number', 
                #                         edmDict['objectMetadata']['attributes'].get('END_ORBIT_NUMBER')),
                # "fileSpatialArea": polyS,
                # "fileName": filePath.split("/")[-1],
                # "fileascdescindicator": edmDict['objectMetadata']['attributes'].get('ascend_descend_data_flag', 
                #                             edmDict['objectMetadata']['attributes'].get('ASCEND_DESCEND_DATA_FLAG')),
                # "fileDayNightFlag": edmDict['objectMetadata']['attributes'].get('day_night_data_flag', 
                #                         edmDict['objectMetadata']['attributes'].get('DAY_NIGHT_DATA_FLAG')),
                # "fileProductSupplementMetadata": None
        #     },
        #     "qualitySummaries": {}
        # }
    
        # if 'geospatial_bounds' in edmDict['objectMetadata']['attributes']:
        #        print 
        #        vSQL['fileMetadata']['fileSpatialArea'] = edmDict['objectMetadata']['attributes']['geospatial_bounds']
    
        #        vSQL['fileMetadata']['fileSpatialArea']=vSQL['fileMetadata']['fileSpatialArea'].replace("POLYGON", "SRID=4326;MULTIPOLYGON(")
        #        vSQL['fileMetadata']['fileSpatialArea']=vSQL['fileMetadata']['fileSpatialArea'].replace("))", ")))")
    
        # vSQL['fileMetadata']['fileStartTime'] = vSQL['fileMetadata']['fileStartTime'].replace("T", " ")
        # vSQL['fileMetadata']['fileEndTime'] = vSQL['fileMetadata']['fileEndTime'].replace("T", " ")
        # vSQL['fileMetadata']['fileStartTime'] = vSQL['fileMetadata']['fileStartTime'].replace("Z", "")
        # vSQL['fileMetadata']['fileEndTime'] = vSQL['fileMetadata']['fileEndTime'].replace("Z", "")
        # vSQL['fileMetadata']['fileStartTime'] = vSQL['fileMetadata']['fileStartTime'].replace("-", "")
        # vSQL['fileMetadata']['fileEndTime'] = vSQL['fileMetadata']['fileEndTime'].replace("-", "")
        # vSQL['fileMetadata']['fileStartTime'] = vSQL['fileMetadata']['fileStartTime'].replace(":", "")
        # vSQL['fileMetadata']['fileEndTime'] = vSQL['fileMetadata']['fileEndTime'].replace(":", "")
    
        # print ("vsql=", vSQL)
    
        # vedmCore = {
        #     "productShortName": None,
        #     "platformNames": [],
        #     "fileId": None,
        #     "fileName": None,
        #     "fileStartTime": time_coverage_start_attribute,
        #     "fileEndTime": time_coverage_end_attribute,
        #     "fileInsertTime": None,
        #     "fileSpatialArea": vSQL['fileMetadata']['fileSpatialArea'] # need to convert to geoJSON (current cheat in persistIRO)
        # }
    
        # edmJson = {
        #     "SQL": vSQL,
        #     "noSQL": {
        #         # "edmCore": vedmCore,
        #         "objectMetadata": edmDict['objectMetadata'],
        #         "objectGroupAttrs": edmDict['objectGroupAttrs']
        #     }
        # }
        # print (json.dumps(edmDict, indent = 2, separators=(',', ':')))
        
        return edmDict
        
    except Exception as e:
        eType, eValue, eTraceback = sys.exc_info()
        print("Error in edm_nupNc4:", eType, eValue, "line: " + str(eTraceback.tb_lineno))
        raise e


if __name__ == "__main__":
    try:
        filePath = sys.argv[1]
    except Exception:
        raise Exception('ERROR: Invalid arguments. Usage: edm_nupNc4.py <filename>')

    main(filePath)
