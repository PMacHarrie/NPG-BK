
#coming from util
#import requests
#import json
#import os

import numbers
import time

from .util import *


class FileClient(object):
    
    def __init__(self, baseUrl):
        # print('hi file')
        if baseUrl.endswith("/"):
            self.url = baseUrl + "file/"
        else:
            self.url = baseUrl + "/file/"
    
    
    def getFile(self, **kwargs):
        # print('getFile')
        
        if 'fileId' in kwargs:
            try: int(kwargs['fileId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("fileId kwarg required")
        
        # check if dirs exist before making the call
        outDirsList = checkOutDirs(**kwargs)
        
        # r = requests.get(self.url + str(kwargs['fileId']))
        
        # if outDirsList is not None:
        #     returnDict = getReturnDict(r, outDirsList = outDirsList)
        # else:
        #     returnDict = getReturnDict(r)
        
        if 'retryOnFailure' in kwargs and kwargs['retryOnFailure']:
            numOfRetries = 3
        else:
            numOfRetries = 1
        
        finalTryDict = {}
        retryDict = {}
        for i in range(1, numOfRetries + 1):
            
            tempDict = self.getFileHelper(str(kwargs['fileId']), outDirsList)
            finalTryDict = tempDict
            if i < numOfRetries:
                retryDict['attempt' + str(i)] = tempDict
            
            if tempDict.get('exception') is not None or tempDict.get('statusCode') != 200:
                print(str(datetime.utcnow()), "NdeRestClient getFile: try", i, "failed:", tempDict)
                # print("NdeRestClient getFile: try", i, "failed:", tempDict.get('statusCode'), 
                #     tempDict.get('exception', ''))
                
            else:
                break
            
            time.sleep(i - 1);
        
        if numOfRetries > 1:
            returnDict = finalTryDict.copy()
            returnDict.update(retryDict)
            return returnDict
        else:
            return finalTryDict
        
        # returnDict = self.getFileHelper(str(kwargs['fileId']), outDirsList)
        
        # if returnDict.get('exception') is not None or returnDict.get('statusCode') != 200:
        #     if 'retryOnFailure' in kwargs and kwargs['retryOnFailure']:
        #         print("NdeRestClient getFile: first try failed.", returnDict.get('statusCode'), 
        #             returnDict.get('exception', '') ,"retrying...")
                
        #         returnDict2 = self.getFileHelper(str(kwargs['fileId']), outDirsList)
        #         returnDict2['firstAttempt'] = returnDict
                
        #         print('returnDict2', returnDict2)
        #         return returnDict2
        #     else:
        #         return returnDict
        # else:
        #     return returnDict
    
    
    def getFileHelper(self, fileIdStr, outDirsList):
        returnDict = {}
        try:
            # dt1 = datetime.now()
            r = requests.get(self.url + fileIdStr)
            # print("NdeRestClient getFile dur: %s" % str(datetime.now() - dt1))
            
            if outDirsList is not None:
                returnDict = getReturnDict(r, outDirsList = outDirsList)
            else:
                returnDict = getReturnDict(r)
        except Exception as e:
            print("NdeRestClient getFile exception:", e)
            returnDict["exception"] = str(e)
        return returnDict
        
        
        
    def getFileMetadata(self, **kwargs):
        # print('getFileMetadata')
        
        if 'fileId' in kwargs:
            try: int(kwargs['fileId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("fileId kwarg required")
        
        r = requests.get(self.url + str(kwargs['fileId']) + "/metadata")
        
        return getReturnDict(r)
        
        
    def getFileArray(self, **kwargs):
        # print('getFileArray')
        
        if 'fileId' in kwargs and 'arrayName' in kwargs:
            try: 
                int(kwargs['fileId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("fileId and arrayName kwarg required")
        
        # check if dirs exist before making the call
        outDirsList = checkOutDirs(**kwargs)
        
        r = requests.get(self.url + str(kwargs['fileId']) + "/array/" + kwargs['arrayName'])
        
        if outDirsList is not None:
            return getReturnDict(r, outDirsList = outDirsList)
        else:
            return getReturnDict(r)
    
    
    def ingestFile(self, **kwargs):
        # print('ingestFile')
        
        if 'filename' not in kwargs or 's3Bucket' not in kwargs or 's3Key' not in kwargs:
            raise ValueError("fileId and arrayName kwarg required")
        
        jsonObj = {
            "filename": kwargs['filename'],
            "filestoragereference": {
                "bucket": kwargs['s3Bucket'],
                "key": kwargs['s3Key']
            }
        }
        
        url = "https://mlf6ufvhl6.execute-api.us-east-1.amazonaws.com/dev_w_cors/file"
        r = requests.put(url, data = json.dumps(jsonObj))
        
        return getReturnDict(r)
    
    
    def searchFile(self, **kwargs):
        # print('searchFile')
        
        if 'jsonInput' in kwargs:
            jsonStr = validateJson(kwargs['jsonInput'])
        else:        
            query = {}
            edmCore = {}
            result = {}
            
            if 'fullText' in kwargs:
                query['fullText'] = kwargs['fullText'].strip()
            if 'productShortNames' in kwargs:
                if isinstance(kwargs['productShortNames'], list):
                    snList = kwargs['productShortNames']
                else:
                    snList = list(filter(None, [s.strip() for s in kwargs['productShortNames'].split(',')]))
                edmCore['productShortNames'] = snList
            if 'fileIds' in kwargs:
                if isinstance(kwargs['fileIds'], list):
                    fiList = kwargs['fileIds']
                else:
                    fiList = list(filter(None, [s.strip() for s in kwargs['fileIds'].split(',')]))
                edmCore['fileIds'] = fiList
            if 'fileNames' in kwargs:
                if isinstance(kwargs['fileNames'], list):
                    fnList = kwargs['fileNames']
                else:
                    fnList = list(filter(None, [s.strip() for s in kwargs['fileNames'].split(',')]))
                edmCore['fileNames'] = fnList
            if 'timeRange' in kwargs:
                temp = strToDict(kwargs['timeRange'], "datetime")
                if 'startTime' not in temp and 'endTime' not in temp:
                    raise ValueError("enqueueTime arg must contain startTime or endTime keys or both")
                edmCore['timeRange'] = temp
            if 'spatialArea' in kwargs:
                try:
                    arg = kwargs['spatialArea']
                    if isinstance(arg, str):
                        items = arg.split(';')
                        temp = {}
                        for item in items:
                            parts = item.strip().split(' ')
                            temp[parts[0]] = [float(parts[1]), float(parts[2])]
                        edmCore['spatialArea'] = temp
                    else:
                        if 'topLeft' not in arg or 'bottomRight' not in arg:
                            raise ValueError("spatialArea arg must contain topLeft and bottomRight keys")
                        if not isinstance(arg['topLeft'], list) or not isinstance(arg['bottomRight'], list):
                            raise ValueError("topLeft and bottomRight should be an array with lon/lat numeric")
                        for coord in arg['topLeft'] + arg['bottomRight']:
                            if not isinstance(coord, numbers.Real):
                                raise ValueError("coordinate should be numeric: " + coord)
                        edmCore['spatialArea'] = arg
                except Exception as e:
                    raise ValueError('Error parsing spatialArea arg: ' + str(e))
            if 'orbitRange' in kwargs:
                arg = kwargs['orbitRange']
                temp = strToDict(arg, "int")
                if 'fileBeginOrbitNum' not in temp and 'fileEndOrbitNum' not in temp:
                    raise ValueError("resultLimit arg must contain fileBeginOrbitNum and fileEndOrbitNum keys")
                edmCore['orbitRange'] = temp
            if 'fileDayNightFlag' in kwargs:
                if kwargs['fileDayNightFlag'] not in ['Day', 'Night', 'Both']:
                    raise ValueError("fileDayNightFlag arg should be Day or Night or Both")
                edmCore['fileDayNightFlag'] = kwargs['fileDayNightFlag']
            if 'fileAscDescIndicator' in kwargs:
                if kwargs['fileAscDescIndicator'] not in ['0', '1', 0, 1]:
                    raise ValueError("fileAscDescIndicator arg should be 0 or 1")
                edmCore['fileAscDescIndicator'] = int(kwargs['fileAscDescIndicator'])
            if 'objectGroupAttrs' in kwargs:
                temp = strToDict(kwargs['objectGroupAttrs'])
                query['objectGroupAttrs'] = temp
            if 'resultFormat' in kwargs:
                arg = kwargs['resultFormat']
                if arg in ['full', 'edmCore']:
                    result['format'] = arg
                else:
                    result['format'] = "attrList"
                    result['attrList'] = list(filter(None, [s.strip() for s in kwargs['attrList'].split(',')]))
            if 'resultSort' in kwargs:
                tempDict = strToDict(kwargs['resultSort'])
                temp = []
                for k,v in tempDict.items():
                    if v not in ['asc', 'desc']:
                        raise ValueError("resultSort values must be asc or desc only")
                    temp.append({k: v})
                result['sort'] = temp
            if 'resultLimit' in kwargs:
                result['limit'] = handleResultLimit(kwargs['resultLimit'])
            
            if len(edmCore) > 0:
                query['edmCore'] = edmCore
            
            if len(query) == 0:
                raise ValueError("at least one query arg must be specified")
            
            jsonObj = {"query": query}
            if len(result) > 0:
                jsonObj['result'] = result
                
            jsonStr = json.dumps(jsonObj)
        
        # print(jsonStr)
        r = requests.post(self.url + "search", data = jsonStr)
        
        return getReturnDict(r)




# f = FileClient("https://vunlor9i2m.execute-api.us-east-1.amazonaws.com/nde-rest")

# print("=============================================================================")
# print(f.getFileMetadata(fileId = 43521700))

# print("=============================================================================")
# print(f.getFile(fileId = 76200572))
# print(f.getFile(fileId = 73549149, outDirs='./a, b'))

# print("=============================================================================")
# f.getFileArray(fileId = 43521700, arrayName='t')

# print("=============================================================================")
# f.searchFile(productShortNames = "VIIRS-M12-SDR", resultFormat="edmCore", resultLimit="from 0; size 1")
# f.searchFile(jsonStr = "{\"query\": {\"edmCore\": {\"productShortNames\": [\"VIIRS-M12-SDR\"]}}, \"result\": {\"format\": \"attrList\", \"attrList\": [\"edmCore\"], \"limit\": {\"from\": \"0\", \"size\": \"1\"}}}")

# f.searchFile(productShortNames = "VIIRS-M12-SDR, VIIRS-M13-SDR", resultFormat="productShortName", resultSort="productShortName asc", resultLimit="from 0; size 10")

# f.searchFile(productShortNames="VIIRS_M15_EDR", spatialArea="topLeft -132.84461975 83.72389221; bottomRight 6.00234079 65.84177399", resultFormat="fileid, fileSpatialArea", resultSort="productShortName asc", resultLimit="from 0; size 10")

# f.searchFile(productShortNames="VIIRS_M15_EDR", spatialArea="topLeft -132.84461975 83.72389221; bottomRight 6.00234079 65.84177399", resultFormat="fileId, fileSpatialArea", resultSort="productShortName asc", resultLimit="from 0; size 10")

# f.searchFile(productShortNames="VIIRS_M14_EDR, VIIRS_M15_EDR", spatialArea="topLeft -132.84461975 83.72389221; bottomRight 6.00234079 65.84177399", resultFormat="fileId, fileSpatialArea", resultSort="productShortName asc", resultLimit="from 0; size 10")

# ff = f.searchFile(productShortNames="VIIRS_M14_EDR", timeRange="startTime 2018-10-18T09:10:01.000Z; endTime 2018-10-18T10:10:01.000Z", resultFormat="fileId, fileEndTime", resultSort="productShortName asc", resultLimit="from 0; size 10")
# print(ff)

# print("=============================================================================")
# f.ingestFile("NPR-MIRS-IMG_v11r1_npp_s201809191909226_e201809191909543_c201809241740470.nc", "ndepg", "incoming_input/VIIRS_CTH_CCL_GEO_EDR_j01_s201809270555281_e201809270556526_c201809281625500.nc")



