"""
    Author: Hieu Phung; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import json
import xmltodict
import sys
import os
import re

def parseArgsAndGo():
    argsLen = len(sys.argv)
    if argsLen == 2 or argsLen == 3:
        outPath = ''
        if argsLen == 3:
            if not os.path.exists(sys.argv[2]):
                os.makedirs(sys.argv[2])
            if os.path.isdir(sys.argv[2]):
                outPath = os.path.abspath(sys.argv[2])
            else:
                print("Output arg must be directory")
                sys.exit()
                
        if os.path.isfile(sys.argv[1]):
            if outPath == '': outPath = os.path.dirname(os.path.abspath(sys.argv[1]))
            convertXmlFile(sys.argv[1], outPath)
        elif os.path.isdir(sys.argv[1]):
            if outPath == '': outPath = os.path.abspath(sys.argv[1])
            traverseDir(sys.argv[1], outPath)
        else:
            print("Invalid input XML file or directory")
            sys.exit()
    else:
        print("Usage: xml2json.py <input: XML file or directory (recursive)> <optional: output directory (creates same structure as input dir)>")
        sys.exit()


def traverseDir(currDir, outPath):
    for rootPath, subdirs, files in os.walk(currDir):
        # print(rootPath)
        # print(subdirs)
        # print(files)
        # for dir in subdirs:
            # print(os.path.join(rootPath, dir))
        for file in files:
            # print(os.path.join(rootPath, file))
            if not file.startswith('.') and file.endswith('xml'):
                print("Processing file:", os.path.join(rootPath, file))
                convertXmlFile( os.path.join(rootPath, file), 
                    os.path.join(outPath, *rootPath.split(os.path.sep)[1:]) )
            else:
                print("Skipping file:", os.path.join(rootPath, file))
            

def convertXmlFile(inFilename, outPath):
    
    try:
        outFilename = os.path.basename(inFilename)[:-3] + "json"
    
        with open(inFilename, 'r') as f:
            xmlString = f.read()
    
        if '</ProductionRule>' in xmlString:
            print("  >ProductionRule")
            # print(xmlString)
            # update Production rule XMLs to wrap PR Input Products
            newXmlString = re.sub(r'(<PRInputSpec[\S\s]*?>)([\S\s]*?)(</PRInputSpec>)', 
                r'\g<1><PRInputProducts>\g<2></PRInputProducts>\g<3>', xmlString)
            # print(newXmlString)
    
            xmlDict = xmltodict.parse(newXmlString)
            outDict = parseXmlDictRecur(xmlDict)
        
            #store dss as an xml string
            r = re.search("^\s*<dss>.*</dss>", newXmlString, re.MULTILINE | re.DOTALL)
            if r:
                #print(r.group(0))
                outDict['ProductionRule']['TailoringSection'] = r.group(0)
                print("  >Replaced TailoringSection with XML")
            
            for prInput in outDict['ProductionRule']['ProductionRuleInputs']:
                for inputProd in prInput['PRInputProducts']:
                    if inputProd.get('platformName') is None:
                        plats = prodXplat.get(inputProd.get('productShortName'))
                        if plats is None:
                            inputProd['platformName'] = 'N/A'
                        elif len(plats) == 1:
                            inputProd['platformName'] = plats[0]
                        elif len(plats) > 1 and 'SNPP' in plats:
                            inputProd['platformName'] = 'SNPP'
                        print("  >Added " + inputProd['platformName'] + " for inProd: " + 
                            inputProd.get('productShortName'))
            
            for outputProd in outDict['ProductionRule']['ProductionRuleOutputs']:
                if outputProd.get('platformName') is None:
                    plats = prodXplat.get(outputProd.get('productShortName'))
                    if plats is None:
                        outputProd['platformName'] = 'N/A'
                    elif len(plats) == 1:
                        outputProd['platformName'] = plats[0]
                    elif len(plats) > 1 and 'SNPP' in plats:
                        outputProd['platformName'] = 'SNPP'
                    print("  >Added " + outputProd['platformName'] + " for outProd: " + 
                        outputProd.get('productShortName'))
        else:
            xmlDict = xmltodict.parse(xmlString)
            outDict = parseXmlDictRecur(xmlDict)
    
        jsonOut = json.dumps(outDict, indent=4)
        # print(jsonOut)

        if not os.path.exists(outPath):
            os.makedirs(outPath)
            
        print(" >>", os.path.join(outPath, outFilename))
        with open(os.path.join(outPath, outFilename), 'w') as f:
            f.write(jsonOut)
    except Exception as e:
        exc_type, exc_obj, tb = sys.exc_info()
        print("ERROR: uanble to process:", inFilename, "line:", tb.tb_lineno, str(e))


def parseXmlDictRecur(inDict):
    outDict = {}
    #print('===============START===============')
    for key, val in inDict.items():
        #print(key, val)
        if isinstance(val, dict):
            if len(val) == 1:
                # if child is a list, put it in an array
                childVal = next(iter(val.values()))
                #print(childVal)
                if isinstance(childVal, list):
                    arr = []
                    for cval in childVal:
                        arr.append(parseXmlDictRecur(cval))
                    outDict[key] = arr
                else:
                    outDict[key] = [parseXmlDictRecur(childVal)]
            else:
                outDict[key] = parseXmlDictRecur(val)
        elif isinstance(val, str):
            if len(key) > 0 and key[0] == '@':
                key = key[1:]
            r = re.search("interval[ ]*'(-*\d*\.*\d+)'[ ]*(second|minute|hour|day)", val, re.IGNORECASE)
            if r:
                if r.group(2).lower() == "second":
                    val = 'PT' + r.group(1) + 'S'
                elif r.group(2).lower() == "minute":
                    val = 'PT' + r.group(1) + 'M'
                elif r.group(2).lower() == "hour":
                    val = 'PT' + r.group(1) + 'H'
                elif r.group(2).lower() == "day":
                    val = 'P' + r.group(1) + 'D'
                elif r.group(2).lower() == "month":
                    val = 'P' + r.group(1) + 'M'
                elif r.group(2).lower() == "year":
                    val = 'P' + r.group(1) + 'Y'
            else:
                r2 = re.search("interval[ ]*'(-*)(\d{2}):(\d{2}):(\d{2}\.\d+)'[ ]*hour to second", val, re.IGNORECASE)
                if r2:
                    val = 'PT' + r2.group(1) + r2.group(2) + 'H' + r2.group(1) + r2.group(3) + 'M' + r2.group(1) + r2.group(4) + 'S'
            outDict[key] = val
    return outDict
    

with open('./prodXplat.json', 'r') as f:
    prodXplat = json.load(f)

parseArgsAndGo()
