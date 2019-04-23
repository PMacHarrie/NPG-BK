
import sys
import json
import subprocess
import os

def main(executableName, optionalParameter, fileName):
    
    try:
        # print (os.listdir('.'))
        out = ""
        try:
            if executableName.endswith("py"):
                os.environ['PYTHONPATH'] = os.environ.get('PYTHONPATH', '') + os.pathsep + os.getcwd()
                spCmd = ["python3", "./extractors/"+ executableName]
            elif executableName.endswith("pl"):
                spCmd = ["perl", "./extractors/" + executableName]
            else:
                raise Exception("Only Python or Perl executable supported: " + executableName)
            
            if optionalParameter is not None and len(optionalParameter.strip()) > 0  \
                and not optionalParameter.startswith('-subString'):
                spCmd.append(optionalParameter)
            
            spCmd.append(fileName)
            
            print("Executing:", spCmd)
            out = subprocess.check_output(spCmd)
            
        except subprocess.CalledProcessError as cpe:
            s = "EME failed with rc: " + str(cpe.returncode) + ", output: " + str(cpe.output)
            print(s)
            raise RuntimeError(s)
        
        
        # If didn't fail parse out the SQL/NOSQL Json and return that
        # tmej = {"status" : "Passed", "SQL" : {}, "noSQL" : { "edmCore" : {} } }
    
        # tmej['SQL'] = {
        #     "fileMetadata" :{
        #         "productId" : productId,
        #         "fileStartTime" : None,
        #         "fileEndTime"   : None,
        #         "fileascdescindicator" :  None,
        #         "filebeginorbitnum" :  None,
        #         "fileendorbitnum" :  None,
        #         "fileSpatialLongitude" : None,
        #         "fileSpatailLatitude"  : None,
        #         "fileDayNightFlag"     : None,
        #         "fileSpatialArea"      : None,
        #         "fileName"      : fileName.split("/")[-1],
        #         "fileProductSupplementMetadata" : None
        #     },
        #     "qualitySummaries" : {}
        # }   
    
        # Return: "YYYY-MM-DD HH24:MI:SS&YYYY-MM-DD HH24:MI:SS&(poly)"
        
        out = out.decode()
        print("EME output:", out)
        
        (fileStartTime, fileEndTime, filePoints) = None, None, None
        
        if out.count('&') == 1:
            (fileStartTime, fileEndTime) = out.split('&')
        else:
            (fileStartTime, fileEndTime, filePoints) = out.split('&')
        
        # fileStartTime = fileStartTime.replace(":", "")
        # fileStartTime = fileStartTime.replace("-", "")
        # fileEndTime   = fileEndTime.replace(":", "")
        # fileEndTime   = fileEndTime.replace("-", "")
        
        fileStartTime = fileStartTime.replace(" ", "T")
        fileEndTime   = fileEndTime.replace(" ", "T")
    
        edmDict = {
            "edmCore": {
                # "fileName": fileName.split("/")[-1], # Will be populated later
                "fileSpatialArea": None,
                "fileStartTime": fileStartTime,
                "fileEndTime": fileEndTime,
                "platformNames": [],
                # "fileBeginOrbitNum" # Not applicable here,
                # "fileEndOrbitNum" # Not applicable here,
                # "fileDayNightFlag" # Not applicable here,
                # "fileAscDescIndicator" # Not applicable for here,
            }
        }
        
        # tmej['SQL']['fileMetadata']['fileStartTime'] = fileStartTime
        # tmej['SQL']['fileMetadata']['fileEndTime']   = fileEndTime
        
        # if filePoints != None:
        #     tmej['SQL']['filePoints']=filePoints
    
        # fileStartTime = fileStartTime.replace(" ", "T")
        # fileEndTime   = fileEndTime.replace(" ", "T")
        # tmej['noSQL']['edmCore']['fileName']     = fileName
        # tmej['noSQL']['edmCore']['fileStartTime']= fileStartTime
        # tmej['noSQL']['edmCore']['fileEndTime']  = fileEndTime
        # tmej['noSQL']['edmCore']['objectMetadata']  = {}
        
        # print ("tmej=", tmej)
        
    
    # Presumed format is: [StartTime, EndTime, {Optional Polygon}] => SQL 
    # Latest hack is one ETE, adeck_tcExtractor.pl has an optional string that is included in the supplementalMetadata column.
    
        # print(edmDict)
        
        return edmDict
    
    except Exception as e:
        eType, eValue, eTraceback = sys.exc_info()
        print("Error in edm_eme:", eType, eValue, "line: " + str(eTraceback.tb_lineno))
        raise e
        
        
if __name__ == "__main__":
    
    try:
        executableName = sys.argv[1]
        fileName = sys.argv[2]
    except Exception:
        raise Exception('ERROR: Invalid arguments. Usage: edm_goesCgs.py <filename>')
        
    main(executableName, '', fileName)
