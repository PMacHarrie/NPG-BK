
# coming from util
# import requests

from .util import *

class AlgoRuleClient(object):
    
    def __init__(self, baseUrl):
        # print('hi algorithm')
        if baseUrl.endswith("/"):
            self.url = baseUrl + "algorithm/"
        else:
            self.url = baseUrl + "/algorithm/"
    
    
    def getAlgorithmList(self):
        # print('getAlgorithmList')
        
        r = requests.get(self.url)
        
        return getReturnDict(r)
    
    
    def getAlgorithm(self, **kwargs):
        # print('getAlgorithm')
        
        if 'algoId' in kwargs:
            try: int(kwargs['algoId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("algoId kwarg required")
        
        r = requests.get(self.url + str(kwargs['algoId']))
        
        return getReturnDict(r)


    def registerAlgorithm(self, **kwargs):
        # print('registerAlgorithm')
        
        if 'jsonInput' in kwargs:
            jsonStr = validateJson(kwargs['jsonInput'])
        else:
            raise ValueError("Must specify jsonInput kwarg")

        r = requests.put(self.url, data = jsonStr)
        
        return getReturnDict(r)
        
    
    def updateAlgorithm(self, **kwargs):
        # print('updateAlgorithm')
        
        if 'algoId' in kwargs:
            try: int(kwargs['algoId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("algoId kwarg required")
            
        if 'jsonInput' in kwargs:
            jsonStr = validateJson(kwargs['jsonInput'])
        else:
            raise ValueError("Must specify jsonInput kwarg")

        r = requests.put(self.url + str(kwargs['algoId']), data = jsonStr)
        
        return getReturnDict(r)
        
    
    def getProductionRuleList(self, **kwargs):
        # print('getProductionRuleList')
        
        if 'algoId' in kwargs:
            try: int(kwargs['algoId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("algoId kwarg required")
        
        r = requests.get(self.url + str(kwargs['algoId']) + "/rule")
        
        return getReturnDict(r)
    
    
    def getProductionRule(self, **kwargs):
        # print('getProductionRule')
        
        if 'algoId' in kwargs and 'ruleId' in kwargs:
            try: 
                int(kwargs['algoId'])
                int(kwargs['ruleId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("algoId and ruleId kwarg required")
        
        r = requests.get(self.url + str(kwargs['algoId']) + "/rule/" + str(kwargs['ruleId']))
        
        return getReturnDict(r)


    def registerProductionRule(self, **kwargs):
        # print('registerProductionRule')
        
        if 'algoId' in kwargs:
            try: int(kwargs['algoId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("algoId kwarg required")
        
        if 'jsonInput' in kwargs:
            jsonStr = validateJson(kwargs['jsonInput'])
        else:
            raise ValueError("Must specify jsonInput kwarg")

        r = requests.put(self.url + str(kwargs['algoId']) + "/rule", data = jsonStr)
        
        return getReturnDict(r)
        
    
    def updateProductionRule(self, **kwargs):
        # print('updateProductionRule')
        
        if 'algoId' in kwargs and 'ruleId' in kwargs:
            try: 
                int(kwargs['algoId'])
                int(kwargs['ruleId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("algoId and ruleId kwarg required")
        
        if 'jsonInput' in kwargs:
            jsonStr = validateJson(kwargs['jsonInput'])
        else:
            raise ValueError("Must specify jsonInput kwarg")

        r = requests.put(self.url + str(kwargs['algoId']) + "/rule/" + str(kwargs['ruleId']), data = jsonStr)
        
        return getReturnDict(r)
        
    
    def turnProductionRuleOnOff(self, **kwargs):
        # print('turnProductionRuleOnOff')
        
        if 'algoId' in kwargs and 'ruleId' in kwargs and 'newPrFlag' in kwargs:
            try: 
                int(kwargs['algoId'])
                int(kwargs['ruleId'])
            except Exception as e: 
                raise ValueError("Invalid id: " + str(e))
            if kwargs['newPrFlag'] not in [0, 1, '0', '1']:
                raise ValueError("new PR flag must be 0 or 1")
        else:
            raise ValueError("algoId, ruleId, and newPrFlag kwarg required")
        
        jsonObj = {"newPrActiveFlag": kwargs['newPrFlag']}
        
        r = requests.patch(self.url + str(kwargs['algoId']) + "/rule/" + str(kwargs['ruleId']), data = json.dumps(jsonObj))
        
        return getReturnDict(r)
        


# a = AlgoRuleClient("https://vunlor9i2m.execute-api.us-east-1.amazonaws.com/nde-rest")
# print("=============================================================================")
# print(a.getAlgorithmList())

# print("=============================================================================")
# print(a.getAlgorithm(algoId = 3))

# print("=============================================================================")
# print(a.getProductionRuleList(algoId = 3))

# print("=============================================================================")
# print(a.getProductionRule(algoId = 3, ruleId = 11))

