
# coming from util
# import requests

from .util import *

class DataCubeClient(object):
    
    def __init__(self, baseUrl):
        # print('hi datacube')
        if baseUrl.endswith("/"):
            self.url = baseUrl + "datacube/"
        else:
            self.url = baseUrl + "/datacube/"
    
    
    def getDataCubeList(self):
        # print('getDataCubeList')

        r = requests.get(self.url)
        
        return getReturnDict(r)
    
    
    def getDataCubeMetadata(self, **kwargs):
        # print('getDataCubeMetadata')
        
        if 'datacubeId' in kwargs:
            try: int(kwargs['datacubeId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("datacubeId kwarg required")
        
        r = requests.get(self.url + str(kwargs['datacubeId']) + "/metadata")
        
        return getReturnDict(r)


    def getDataCubeSelect(self, **kwargs):
        """
        getDataCubeSelect processes ad-hoc requests to dss.
        The ad-hoc request is handled by creating an on-demand job with the appropriate on-demand job spec.
        
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
        # print('getDataCubeSelect')
        
        if 'jsonInput' in kwargs:
            jsonStr = validateJson(kwargs['jsonInput'])
        else:
            raise ValueError("Must specify jsonInput kwarg")
        
        r = requests.put(self.url + "select", data = jsonStr)
        
        return getReturnDict(r)



# d = DataCubeClient("https://vunlor9i2m.execute-api.us-east-1.amazonaws.com/nde-rest")
# print("=============================================================================")
# d.getDataCubeList()

# print("=============================================================================")
# d.getDataCubeMetadata(43)