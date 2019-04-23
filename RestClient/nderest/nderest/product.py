
# coming from util
# import requests
# import json

from .util import *

class ProductClient(object):
    
    def __init__(self, baseUrl):
        # print('hi product')
        if baseUrl.endswith("/"):
            self.url = baseUrl + "product/"
        else:
            self.url = baseUrl + "/product/"
    
    
    def getProductList(self):
        # print('getProductList')
        
        r = requests.get(self.url)
        
        return getReturnDict(r)
    
    
    def getProduct(self, **kwargs):
        # print('getProduct')
        
        if 'prodId' in kwargs:
            try: int(kwargs['prodId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("prodId kwarg required")

        r = requests.get(self.url + str(kwargs['prodId']))
        
        return getReturnDict(r)
        
        
    def registerProduct(self, **kwargs):
        # print('registerProduct')
        
        jsonStr = validateJson(kwargs)
        if jsonStr is None:
            raise ValueError("Must specify jsonStr or jsonFile arg")

        if 'jsonInput' in kwargs:
            jsonStr = validateJson(kwargs['jsonInput'])
        else:
            raise ValueError("Must specify jsonInput kwarg")
            
        r = requests.put(self.url, data = jsonStr)
        
        return getReturnDict(r)
        
    
    def updateProduct(self, **kwargs):
        # print('updateProduct')
        
        if 'prodId' in kwargs:
            try: int(kwargs['prodId'])
            except Exception as e: raise ValueError("Invalid id: " + str(e))
        else:
            raise ValueError("prodId kwarg required")
        
        if 'jsonInput' in kwargs:
            jsonStr = validateJson(kwargs['jsonInput'])
        else:
            raise ValueError("Must specify jsonInput kwarg")
        
        r = requests.put(self.url + str(kwargs['prodId']), data = jsonStr)
        
        return getReturnDict(r)
        
        
    def getProductFromFilename(self, **kwargs):
        # print('getProductFromFilename')
        
        if 'filename' not in kwargs:
            raise ValueError("filename kwarg required")
        
        queryParams = {'filenamePattern': filename}
        
        r = requests.get(self.url + 'search', params=queryParams)
        
        return getReturnDict(r)





# p = ProductClient("https://vunlor9i2m.execute-api.us-east-1.amazonaws.com/nde-rest")
# print("=============================================================================")
# p.getProductList()

# print("=============================================================================")
# p.getProduct("19")

# print("=============================================================================")
# # p.updateProduct(18, json_file='prod.json')

# print("=============================================================================")
# p.getProductFromFilename('SCRIS_npp_d20180927_t1502319_e1503017_b04444_c20180927153802227889_niic_int.h5')