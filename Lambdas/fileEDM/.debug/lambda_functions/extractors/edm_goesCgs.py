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


def main(fileName):

    try:
        d = Dataset(fileName, 'r')
    
        edmDict = {
            'objectMetadata': {
                'attributes': {},
                'dimensions': {},
                'variables': {}
            }
        }
    
        for dim in d.dimensions.keys():
            # print(dim, "size=", len(d.dimensions[dim]))
            edmDict['objectMetadata']['dimensions'][dim] = len(d.dimensions[dim])
    
        for group in d.groups:
            print ("group=", group)
    
        for var in d.variables:
            # print(var, d[var])
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
                else:  # its not an array but it might be a type the is not json serializalbe
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
            edmDict['objectMetadata']['variables'][var] = varJ[var]
    
        for gattr in d.ncattrs():
            #       print "***** gattr=", gattr, "<<<<, getattr()=", getattr(d,gattr), "<<<<"
            edmDict['objectMetadata']['attributes'][gattr] = str(getattr(d, gattr))
        #        print ("gattr=", gattr, "type=", type(edmDict['objectMetadata']['attributes'][gattr]))
    
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
        d.close()
    
        polyS = (
            "SRID=4326;MULTIPOLYGON(((" +
            str(edmDict['objectMetadata']['variables']['geospatial_lat_lon_extent']['attributes']['geospatial_westbound_longitude']) + " " + 
            str(edmDict['objectMetadata']['variables']['geospatial_lat_lon_extent']['attributes']['geospatial_southbound_latitude']) + ",  " +
            str(edmDict['objectMetadata']['variables']['geospatial_lat_lon_extent']['attributes']['geospatial_westbound_longitude']) + " " + 
            str(edmDict['objectMetadata']['variables']['geospatial_lat_lon_extent']['attributes']['geospatial_northbound_latitude']) + ",  " +
            str(edmDict['objectMetadata']['variables']['geospatial_lat_lon_extent']['attributes']['geospatial_eastbound_longitude']) + " " + 
            str(edmDict['objectMetadata']['variables']['geospatial_lat_lon_extent']['attributes']['geospatial_northbound_latitude']) + ",  " +
            str(edmDict['objectMetadata']['variables']['geospatial_lat_lon_extent']['attributes']['geospatial_eastbound_longitude']) + " " + 
            str(edmDict['objectMetadata']['variables']['geospatial_lat_lon_extent']['attributes']['geospatial_southbound_latitude']) + ",  " +
            str(edmDict['objectMetadata']['variables']['geospatial_lat_lon_extent']['attributes']['geospatial_westbound_longitude']) + " " + 
            str(edmDict['objectMetadata']['variables']['geospatial_lat_lon_extent']['attributes']['geospatial_southbound_latitude']) +
            ")))"
        )
    
        edmDict['edmCore'] = {
            # 'fileName': fileName.split("/")[-1], # Will be populated later
            'fileSpatialArea': polyS,
            'platformNames': [edmDict['objectMetadata']['attributes']['platform_ID']],
            'fileStartTime': edmDict['objectMetadata']['attributes']['time_coverage_start'],
            'fileEndTime': edmDict['objectMetadata']['attributes']['time_coverage_end']
            # "fileBeginOrbitNum" # Not applicable for GOES,
            # "fileEndOrbitNum" # Not applicable for GOES,
            # "fileDayNightFlag" # Not applicable for GOES,
            # "fileAscDescIndicator" # Not applicable for GOES,
        }
    
        # Create objectGroupAttrs
        edmDict['objectGroupAttrs'] = edmDict['objectMetadata']['attributes']
    
        # print ("n4=", edmDict)
    
        # edmJson = {
        #     "SQL": {},
        #     "noSQL": edmDict
        # }
    
        # print (json.dumps(edmDict, indent = 2, separators=(',', ':')))
        
        return edmDict
        
    except Exception as e:
        eType, eValue, eTraceback = sys.exc_info()
        print("Error in edm_goesCgs:", eType, eValue, "line: " + str(eTraceback.tb_lineno))
        raise e


if __name__ == "__main__":
    
    try:
        fileName = sys.argv[1]
    except Exception:
        raise Exception('ERROR: Invalid arguments. Usage: edm_goesCgs.py <filename>')
        
    main(fileName)