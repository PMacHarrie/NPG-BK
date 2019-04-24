
# coming from util
# import requests
# import json
# import datetime

from .util import *


class JobClient(object):
    
    def __init__(self, baseUrl):
        # print('hi job')
        if baseUrl.endswith("/"):
            self.url = baseUrl + "job/"
        else:
            self.url = baseUrl + "/job/"
    
    
    def createJob(self,  **kwargs):
        # print('createJob')
        
        jsonStr = validateJson(kwargs)
        
        r = requests.put(self.url, data = jsonStr)
        
        return getReturnDict(r)
    
    
    def getJobsSummary(self):
        # print('getJobsSummary')

        r = requests.get(self.url)
        
        return getReturnDict(r)
        
    
    def getJobDetails(self, **kwargs):
        # print('getJobDetails')
        
        if 'jobId' in kwargs:
            try: int(kwargs['jobId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("jobId kwarg required")

        r = requests.get(self.url + str(kwargs['jobId']))
        
        return getReturnDict(r)
        
        
    def updateJobStatus(self, **kwargs):
        # print('updateJobStatus')
        
        if 'jobId' in kwargs and 'newStatus' in kwargs:
            try: int(kwargs['jobId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("jobId kwarg required")
        
        jsonStr = json.dumps({"newJobStatus": newStatus})
        
        r = requests.patch(self.url + str(jobId), data = jsonStr)
        
        return getReturnDict(r)
        
        
    def searchJob(self, **kwargs):
        # print('searchJob')
        
        if 'jsonInput' in kwargs:
            jsonStr = validateJson(kwargs['jsonInput'])
        else:
            jsonObj = {}
            query = {}
            result = {}
            if 'jobStatus' in kwargs:
                query['jobStatus'] = list(filter(None, [s.strip() for s in kwargs['jobStatus'].split(',')]))
            if 'algorithm' in kwargs:
                if isinstance(kwargs['algorithm'], str):
                    algoParts = kwargs['algorithm'].split(';')
                    if len(algoParts) != 2:
                        raise ValueError("algorithm arg must be: <name>; <version>")
                    query['algorithm'] = { "name": algoParts[0].strip(), "version": algoParts[1].strip() }
                elif isinstance(kwargs['algorithm'], dict):
                    if 'name' not in kwargs['algorithm'] or 'version' not in kwargs['algorithm']:
                        raise ValueError("algorithm arg must contain name and version keys")
                    query['algorithm'] = kwargs['algorithm']
                else:
                    raise ValueError("algorithm arg must be str or dict")
            if 'enqueueTime' in kwargs:
                temp = strToDict(kwargs['enqueueTime'], "datetime")
                if 'gte' not in temp and 'lte' not in temp:
                    raise ValueError("enqueueTime arg must contain gte or lte keys or both")
                query['enqueueTime'] = temp
            if 'startTime' in kwargs:
                temp = strToDict(kwargs['startTime'], "datetime")
                if 'gte' not in temp and 'lte' not in temp:
                    raise ValueError("startTime arg must contain gte or lte keys or both")
                query['startTime'] = temp
            if 'completionTime' in kwargs:
                temp = strToDict(kwargs['completionTime'], "datetime")
                if 'gte' not in temp and 'lte' not in temp:
                    raise ValueError("completionTime arg must contain gte or lte keys or both")
                query['completionTime'] = temp
            if 'resultSort' in kwargs:
                temp = strToDict(kwargs['resultSort'])
                if not any(i in list(temp.values()) for i in ['asc', 'desc']):
                    raise ValueError("resultSort values must be asc or desc only")
                result['sort'] = [temp]
            if 'resultLimit' in kwargs:
                result['limit'] = handleResultLimit(kwargs['resultLimit'])

            if len(query) == 0:
                raise ValueError("at least one query arg must be specified")
            
            jsonObj['query'] = query
            if len(result) > 0:
                jsonObj['result'] = result
                
            jsonStr = json.dumps(jsonObj)
                
        r = requests.post(self.url + "search", data = jsonStr)
        
        return getReturnDict(r)
        
        
    def cancelJob(self):
        print('TODO')
        
        return None
        
    



# a = JobClient("https://vunlor9i2m.execute-api.us-east-1.amazonaws.com/nde-rest")
# print("=============================================================================")
# print(a.getJobsSummary())

# print("=============================================================================")
# a.getJobDetails(349)

# # print("=============================================================================")
# # a.updateJobStatus(349, "COMPLETE")

# # print("=============================================================================")
# # a.getJobDetails(349)

# print("=============================================================================")
# strToDict("jobStatus asc; enqueueTime desc")
# strToDict({"jobStatus": "asc", "enqueueTime": "desc"})
# strToDict({"jobStatus": "1", "enqueueTime": "2"}, "int")
# strToDict({"enqueueTime": "2018-09-30T14:46:12.100Z", "enqueueTime1": "2018-09-30T14:46:12.100Z"}, "datetime")
# n = datetime.datetime.now()
# strToDict({"jobStatus": n, "enqueueTime": n})

# # a.searchJob(jobStatus = "QUEUED", resultSort = "enqueueTime desc")
# # a.searchJob(jobStatus = "QUEUED", enqueueTime="gte 2018-10-16T18:39:51.000Z; lte 2018-10-16T18:39:53.000Z", resultSort = "enqueueTime desc")
# # a.searchJob(jobStatus = "QUEUED", enqueueTime= {"gte": "2018-10-16T18:39:51.000Z", "lte": "2018-10-16T18:39:53.000Z"}, resultSort = "enqueueTime desc")
# # a.searchJob(jobStatus = "COMPLETE", algorithm="dss ad-hoc; 1.0", resultSort = "odalgorithmreturncode desc")
# a.searchJob(jobStatus = "COMPLETE", algorithm="dss ad-hoc; 1.0", resultSort = "jobId desc", resultLimit="from 1; size 1")