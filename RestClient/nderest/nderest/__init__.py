
from .algo import AlgoRuleClient
from .datacube import DataCubeClient
from .file import FileClient
from .job import JobClient
from .product import ProductClient
from .util import downloadFileUtil
import json

class NdeRestClient(object):
    
    def __init__(self, baseUrl):
        # print('hi NdeRest')
        if not baseUrl.endswith("/"):
            baseUrl = baseUrl + "/"
            
        self.baseUrl = baseUrl
        
        self.ac = AlgoRuleClient(baseUrl)
        self.dc = DataCubeClient(baseUrl)
        self.fc = FileClient(baseUrl)
        self.jc = JobClient(baseUrl)
        self.pc = ProductClient(baseUrl)
            
    
    def retHelper(self, res, **kwargs):
        if 'resultDetailsAsDict' in kwargs and kwargs.get('resultDetailsAsDict') == True:
            return res
        else:
            if 'result' in res:
                if 'resultAsDict' in kwargs and kwargs.get('resultAsDict') == True:
                    if not isinstance(res['result'], dict):
                        try:
                            # try to return result as dict
                            return json.loads(res['result'])
                        except Exception as e:
                            # couldn't parse as JSON, so just wrap it as dict with 'result' key
                            return {'result': res['result']}
                    else:
                        return res['result']
                else:
                    try:
                        # if res['result'] is json str, prettify it, return as json str
                        return json.dumps(json.loads(res['result']), indent = 4)
                    except Exception as e:
                        # res['result'] was not a json str, so just return it as is (may be str/dict)
                        return res['result']
            else:
                return res
                
    
    def getAlgorithmList(self, **kwargs):
        return self.retHelper(self.ac.getAlgorithmList(), **kwargs)
    
    def getAlgorithm(self, **kwargs):
        return self.retHelper(self.ac.getAlgorithm(**kwargs), **kwargs)

    def registerAlgorithm(self, **kwargs):
        return self.retHelper(self.ac.registerAlgorithm(**kwargs), **kwargs)
    
    def updateAlgorithm(self, **kwargs):
        return self.retHelper(self.ac.updateAlgorithm(**kwargs), **kwargs)
        
    def getProductionRuleList(self, **kwargs):
        return self.retHelper(self.ac.getProductionRuleList(**kwargs), **kwargs)
    
    def getProductionRule(self, **kwargs):
        return self.retHelper(self.ac.getProductionRule(**kwargs), **kwargs)

    def registerProductionRule(self, **kwargs):
        return self.retHelper(self.ac.registerProductionRule(**kwargs), **kwargs)
    
    def updateProductionRule(self, **kwargs):
        return self.retHelper(self.ac.updateProductionRule(**kwargs), **kwargs)
        
    def turnProductionRuleOnOff(self, **kwargs):
        return self.retHelper(self.ac.turnProductionRuleOnOff(**kwargs), **kwargs)
        
    def getDataCubeList(self, **kwargs):
        return self.retHelper(self.dc.getDataCubeList(), **kwargs)
        
    def getDataCubeMetadata(self, **kwargs):
        return self.retHelper(self.dc.getDataCubeMetadata(**kwargs), **kwargs)
        
    def getDataCubeSelect(self, **kwargs):
        return self.retHelper(self.dc.getDataCubeSelect(**kwargs), **kwargs)
            
    def getFile(self, **kwargs):
        return self.retHelper(self.fc.getFile(**kwargs), **kwargs)
        
    def getFileMetadata(self, **kwargs):
        return self.retHelper(self.fc.getFileMetadata(**kwargs), **kwargs)
        
    def getFileArray(self, **kwargs):
        return self.retHelper(self.fc.getFileArray(**kwargs), **kwargs)
    
    def ingestFile(self, **kwargs):
        return self.retHelper(self.fc.ingestFile(**kwargs), **kwargs)
    
    def searchFile(self, **kwargs):
        return self.retHelper(self.fc.searchFile(**kwargs), **kwargs)
        
    def createJob(self, **kwargs):
        return self.retHelper(self.jc.createJob(**kwargs), **kwargs)
    
    def getJobsSummary(self, **kwargs):
        return self.retHelper(self.jc.getJobsSummary(), **kwargs)
    
    def getJobDetails(self, **kwargs):
        return self.retHelper(self.jc.getJobDetails(**kwargs), **kwargs)
        
    def updateJobStatus(self, **kwargs):
        return self.retHelper(self.jc.updateJobStatus(**kwargs), **kwargs)
        
    def searchJob(self, **kwargs):
        return self.retHelper(self.jc.searchJob(**kwargs), **kwargs)
        
    def getProductList(self, **kwargs):
        return self.retHelper(self.pc.getProductList(), **kwargs)
    
    def getProduct(self, **kwargs):
        return self.retHelper(self.pc.getProduct(**kwargs), **kwargs)
        
    def registerProduct(self, **kwargs):
        return self.retHelper(self.pc.registerProduct(**kwargs), **kwargs)
    
    def updateProduct(self, **kwargs):
        return self.retHelper(self.pc.updateProduct(**kwargs), **kwargs)
        
    def getProductFromFilename(self, **kwargs):
        return self.retHelper(self.pc.getProductFromFilename(**kwargs), **kwargs)
    
    def downloadFileUtil(self, **kwargs):
        return downloadFileUtil(**kwargs)
    
# print('hi')
# nrc = NdeRestClient("https://vunlor9i2m.execute-api.us-east-1.amazonaws.com/nde-rest")
# r = nrc.downloadFileUtil(url = "https://ndepg.s3.amazonaws.com/outgoing/659/myDataProduct.nc_j01_s201810291155340_e201810291200170_c201810311923190.nc?AWSAccessKeyId=AKIAIXLNTH7Q4YGVGBWA&Expires=1541017412&Signature=GmHOqVV5TMCsgosMMxFb4ya%2FPsI%3D")
# # r = nrc.getAlgorithmList()
# print(r)
# print('bye')

