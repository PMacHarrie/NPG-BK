import json
import xmltodict
import sys
import os
import re

def parseArgsAndGo(argv):
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
            if not file.startswith('.') and file.endswith('json'):
                print("Processing file:", os.path.join(rootPath, file))
                updateJsonFile( os.path.join(rootPath, file), 
                    os.path.join(outPath, *rootPath.split(os.path.sep)[1:]) )
            else:
                print("Skipping file:", os.path.join(rootPath, file))
            

def updateJsonFile(inFilename, outPath):
    
    try:
        outFilename = inFilename
    
        with open(inFilename, 'r') as f:
            data = json.load(f)
        
        prInputs = data.get('ProductionRule').get('ProductionRuleInputs')
        
        for prInput in prInputs:
            prInput['PRInputProducts'] = [prInput.get('PRInputProduct')]
    
        jsonOut = json.dumps(outDict, indent=4)
        # print(jsonOut)

        if not os.path.exists(outPath):
            os.makedirs(outPath)
            
        print(">>", os.path.join(outPath, outFilename))
        with open(os.path.join(outPath, outFilename), 'w') as f:
            f.write(jsonOut)
    except Exception as e:
        print("ERROR: uanble to process:", inFilename, str(e))
    

if __name__ == "__main__":
    try:
        fileName = sys.argv[1]
    except Exception:
        print('ERROR: Invalid arguments. Usage: edm_idpsH5.py <filename>')
        sys.exit(os.EX_USAGE)
    
    parseArgsAndGo(fileName)
