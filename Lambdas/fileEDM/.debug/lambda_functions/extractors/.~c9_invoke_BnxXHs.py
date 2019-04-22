"""
    Author: Peter MacHarrie, Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import h5py
import sys
import json
import numpy
from datetime import datetime

from lambda_functions.extractors.h52json import h52json


# group -> dim, attributes, 
def getGroupAttrs(d, edmDict):
    for k, v in d.items():
        if isinstance(v, dict):
            if k == "attributes":
                for x in v:
                    # print ({x : v[x]})
                    edmDict['objectGroupAttrs'][x] = v[x]
            getGroupAttrs(v, edmDict)
    #   else:
    #       print("{0} : {1}".format(k, v))


# Input:
#   filePath (and file)
# Output:
#   json object containing sql and noSQL data


def main(filePath):

    try:
        # print ("filePath = ", filePath)
    
        edmDict = {}
        edmDict['objectMetadata'] = h52json(filePath)
        print(edmDict)
        f = edmDict['objectMetadata']['Data_Products']
        # print ("f=", json.dumps(f, indent = 1))
    
        i = 0
        for x in f:
            i += 1
            # print ("keys() of Data_Products", x)
    
        # Use x as the key for the particular product
    
        f = edmDict['objectMetadata']['Data_Products'][x]['attributes']
        # print ("f=", json.dumps(f, indent = 1))
        # node = f["/Data_Products/" + x + "/" + x + "_Gran_0"]
        # print ("node=", node)
    
        # for k,v in node.attrs.items():
        #    print (k,v)
    
        startDate = f['Beginning_Date']
        startTime = f['Beginning_Time']
        # print (startTime)
        endDate = f['Ending_Date']
        endTime = f['Ending_Time']
    
        ascDscI = None
        if 'Ascending/Descending_Indicator' in f:
            ascDscI = int(f['Ascending/Descending_Indicator'])
    
        dnf = None
        # print ("f=", f)
        if 'N_Day_Night_Flag' in f:
            dnf = f['N_Day_Night_Flag']
    
        begOrbNum = int(f['N_Beginning_Orbit_Number'])
        endOrbNum = int(f['N_Beginning_Orbit_Number'])
    
        qSumNames = []
        qSumValues = []
    
        if 'N_Quality_Summary_Names' in f:
            qsN = f['N_Quality_Summary_Names']
            qsV = f['N_Quality_Summary_Values']
    
            # print ("qsN=", qsN, type(qsN))
            if type(qsN) is list:
                for n in qsN:
                    qSumNames.append(n)
            else:
                qSumNames.append(qsN)
    
            # print ("qsV=", qsV, type(qsV))
            if type(qsV) is list:
                for v in qsV:
                    # print ("v=",v)
                    qSumValues.append(float(v))
            else:
                qSumValues.append(float(qsV))
    
        fileSpatialArea = "SRID=4326;MULTIPOLYGON((("
    
        fsLongs = []
        fsLats = []
        fileSpatialArea = None
    
        if 'G-Ring_Longitude' in f:
            longs = f['G-Ring_Longitude']
            lats = f['G-Ring_Latitude']
    
            for lon in longs:
                fsLongs.append(float(lon))
    
            for lat in lats:
                fsLats.append(float(lat))
    
            fileSpatialArea = "SRID=4326;MULTIPOLYGON((("
    
            latlon = []
    
            for i in range(len(fsLongs)):
                latlon.append(str(fsLongs[i]) + " " + str(fsLats[i]))
    
            polygonStr = ",".join(latlon)
    
            # Close polygon
    
            polygonStr += "," + latlon[0]
    
            # print (polygonStr)
            fileSpatialArea += polygonStr + ")))"
    
        # edmDict['SQL'] = {
        #     "fileMetadata": {
        #         "productId": None,
        #         "fileStartTime": startDate + " " + str(startTime)[:8],
        #         "fileEndTime": endDate + " " + str(endTime)[:8],
        #         "fileascdescindicator": ascDscI,
        #         "filebeginorbitnum": begOrbNum,
        #         "fileendorbitnum": endOrbNum,
        #         "fileSpatialLongitude": fsLongs,
        #         "fileSpatialLatitude": fsLats,
        #         "fileDayNightFlag": dnf,
        #         "fileSpatialArea": fileSpatialArea,
        #         "fileName": filePath.split("/")[-1],
        #         "fileProductSupplementMetadata": None
        #     }
        # }
    
        # print ('qSumNames=', qSumNames)
        # print ('qSumValues=', qSumValues)
    
        # No longer doing fileQualitySummarries
        # for i in range(len(qSumNames)):
        #     edmDict['SQL']['qualitySummaries'][qSumNames[i]] = qSumValues[i]
    
        # if edmDict['SQL']['fileMetadata']['fileDayNightFlag'] == None:
        #     edmDict['SQL']['fileMetadata']['fileDayNightFlag'] = 'None'
    
        # edmDict['SQL']='fileMetadata']['fileName']=filePath.split("/")[-1]
        # print (edmDict)
    
        # Get EDM Core
        
        # print(startDate + " " + startTime)
        fst = datetime.strptime(startDate + " " + startTime, '%Y%m%d %H%M%S.%fZ')
        print(fst)
        
        # print(endDate + " " + endTime)
        fet = datetime.strptime(endDate + " " + endTime, '%Y%m%d %H%M%S.%fZ')
        print(fet)
        
        edmDict['edmCore'] = {
            # "fileId": None, # Will be populated later
            # "fileName": filePath.split("/")[-1], # Will be populated later
            # "productShortName": None,
            "platformNames": [edmDict['objectMetadata']['attributes']['Platform_Short_Name']],
            "fileSpatialArea": fileSpatialArea, # need to convert to geoJSON (current cheat in persistIRO),
            "fileStartTime": str(fst),
            "fileEndTime": str(fet),
            # "fileInsertTime": None, # Will be populated later
            "fileBeginOrbitNum": begOrbNum,
            "fileEndOrbitNum": endOrbNum,
            "fileDayNightFlag": dnf,
            "fileAscDescIndicator": ascDscI
        }
    
        # Iterate through objectMetadata and scrap out the group attributes. (Only group atts. will be indexed in ES.)
        edmDict['objectGroupAttrs'] = {}
        getGroupAttrs(edmDict['objectMetadata'], edmDict)
    
        # print (json.dumps(edmDict, indent = 2))
        
        return edmDict
        
    except Exception as e:
        eType, eValue, eTraceback = sys.exc_info()
        print("Error in edm_idpsH5:", eType, eValue, "line: " + str(eTraceback.tb_lineno))
        raise e


if __name__ == "__main__":
    
    try:
        filePath = sys.argv[1]
    except Exception:
        raise Exception('ERROR: Invalid arguments. Usage: edm_idpsH5.py <filename>')
    
    main(filePath)

