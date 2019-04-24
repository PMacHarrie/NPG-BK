
from datetime import datetime
import json
import re
import requests
import os

def validateJson(jsonResource):
    
    if isinstance(jsonResource, dict):
        try:
            return json.dumps(jsonResource)
        except Exception as e:
            raise ValueError("error parsing json resource: " + str(e))
    
    errStr = ""
    jsonStr = jsonResource
    
    if '{' not in jsonResource:
        try:
            with open(jsonResource, 'r') as f:
                jsonStr = f.read()
        except IOError as ie:
            errStr = str(ie)
            # print(str(e))
    
    try:
        json.loads(jsonStr)
    except ValueError as ve:
        if errStr:
            raise ValueError("error parsing json resource: " + errStr)
        else:
            raise ValueError("error parsing json resource: " + str(ve))
    
    return jsonStr
    

def handleResultLimit(arg):

    ret = None
    if isinstance(arg, int):
        ret = {"from": 0, "size": arg}
    else:
        if isinstance(arg, str):
            try:
                int(arg)
                ret = {"from": 0, "size": arg}
            except ValueError as ve:
                # print("str couldn't be casted as int")
                pass    
    if ret is None:
        temp = strToDict(arg, "int")
        if 'from' not in temp and 'size' not in temp:
            raise ValueError("resultLimit arg must contain from or size keys or both")
        ret = temp
        
    return ret


def strToDict(input, *datatype):
    # print('strToDict')
    try:
        if isinstance(input, str):
            out = {}
            items = input.split(';')
            for item in items:
                parts = item.strip().split(' ')
                if len(parts) != 2:
                    raise ValueError("invalid arg (incorrect number of parts): " + input)
                out[parts[0]] = parts[1]
        elif isinstance(input, dict):
            out = input
        else:
            raise ValueError("invalid argument type: " + input)
            
        if len(datatype) > 0:
            if datatype[0] == "datetime":
                for k,v in out.items():
                    if isinstance(v, str):
                        try:
                            datetime.strptime(v,'%Y-%m-%dT%H:%M:%S.%fZ')
                        except ValueError as ve:
                            raise ValueError("invalid format for date string: " + v)
                    elif isinstance(v, datetime):
                        out[k] = datetime.strftime(v,'%Y-%m-%dT%H:%M:%S.%fZ')
                    else:
                        raise ValueError("invalid argument type")
            elif datatype[0] == "int":
                temp = {}
                for k,v in out.items():
                    try:
                        temp[k] = int(v)
                    except ValueError as ve:
                        raise ValueError(v + " should be a number")
                out = temp
        # sort: "jobStatus asc; enqueueTime desc"
        # sort: {"jobStatus": "asc", "enqueueTime": "desc"}
        # print(out)
        return out
    except Exception as e:
        raise ValueError('Error parsing input: ' + str(e))
        
        
def getReturnDict(r, **kwargs):
    
    retDict = {}
    
    # print(r.request.url)
    # print(r.request.body)
    
    if r.history:
        retDict['reqUrl']  = r.history[0].request.url
        retDict['reqBody'] = r.history[0].request.body
    else:
        retDict['reqUrl']  = r.request.url
        retDict['reqBody'] = r.request.body
    
    # print(r.status_code)
    # print(r.headers)
    # print(r.headers['Content-Type'])
    # print(r.json())
    # print(r.history)
    # print(r.url)
    # print(r.request.url)
    
    retDict['statusCode'] = r.status_code
    retDict['headers']    = r.headers

    if r.headers['Content-Type'] == "binary/octet-stream":
        retDict['result'] = fileWriteHelper(r, kwargs.get('outDirsList'))
    elif r.headers['Content-Type'] == "application/json":
        # print(r.text)
        # print(r.content)
        retDict['result'] = r.text
    else:
        # print(r.text)
        # print(r.content)
        retDict['result'] = r.text
        
    # print(retDict)
    
    return retDict


def checkOutDirs(**kwargs):
    # check if dirs exist before making the call
    if 'outDirs' in kwargs:
        if isinstance(kwargs['outDirs'], str):
            outDirsList = [x.strip() for x in kwargs['outDirs'].split(',')]
        elif isinstance(kwargs['outDirs'], list) :
            outDirsList = kwargs['outDirs']
        else:
            raise IOError("outDirs kwargs must be list or comma delimited string")
            
        for outDir in outDirsList:
            if not os.path.exists(outDir):
                raise IOError("Directory does not exist: " + outDir)
                
        return outDirsList
    else:
        return None
    
    
def downloadFileUtil(**kwargs):
    
    if 'url' not in kwargs:
        raise ValueError("missing url kwarg")
        
    outDirsList = checkOutDirs(**kwargs)
    
    r = requests.get(kwargs['url'])
    
    return fileWriteHelper(r, outDirsList)


def fileWriteHelper(r, outDirsList):
    m = re.search('([^/\\&\?]+\.\w+)(?=([\?&].*$|$))', r.url)
    if m:
        filename = m.group(1)
        # print(r.history[0].content)
    else:
        filename = 'temp.out'

    outStatus = {'filename': filename, "output": [] }
    
    if outDirsList is None:
        outFile = filename
        # with open(outFile, 'wb') as f:
        #     f.write(r.content)
        with open(outFile, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=256):
                fd.write(chunk)
        outStatus['output'].append(outFile)
    else:
        for outDir in outDirsList:
            outFile = str(outDir) + '/' + filename
            with open(outFile, 'wb') as fd:
                for chunk in r.iter_content(chunk_size=256):
                    fd.write(chunk)
            outStatus['output'].append(outFile)
            
    # print(outStatus)
    return outStatus
