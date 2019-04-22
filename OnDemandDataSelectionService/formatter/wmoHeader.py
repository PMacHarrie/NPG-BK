#!/usr/bin/env python2.7
"""
@author: dcp

Program wraps dss.pl in order to add WMO header to binary data file
    WMO header is defined in the product short name (production rule)
 revised: 20111220 dcp, created
          20120327 dcp, added error code handling
          20120612 dcp, added compression capability (must be turned "On" 
              in production rule)
          20141023 dcp, made a part of dss (no longer wraps it)
          20141105 dcp, ported code to python
"""
import os, sys, re, string
import logging
from util import readPCF
from util import readPSF
from util import writePSF
from util import setuplogger
from util import gzipFile

log = logging.getLogger("formatting")

def main(argv):
    try:
        workDir = sys.argv[1]
    except Exception:
        log.exception("Invalid argument. Must be working Directory.")
        sys.exit(os.EX_USAGE)
        
    try:
        log.info("Executing WMO header formatting...")
        #Get tailored AWIPS NC4 file name       
        nc4File = readPSF(workDir)
        
        #Get PCF parameters
        gzipFlag = readPCF(workDir,"Compression_Flag")
        jobStart = readPCF(workDir,"job_coverage_start")
        
        #Compress the file
        if gzipFlag and re.match("On",gzipFlag,re.I):
            gzFile = nc4File + '.gz'
            log.debug("Compressing file, " + gzFile +" , using gzip")
            gzipFile(workDir + '/' + nc4File)
        else:
            gzFile = nc4File
               
        #Find the WMO header string from file name
        idx = string.index(gzFile,"KNES")
        wmoHeader = string.join(string.split(gzFile[idx-7:idx+4],"_"))
        day = jobStart[6:8]
        hour = jobStart[8:10]
        min = jobStart[10:12]
        wmoHeader += " " + day + hour + min
        log.info("FOUND WMO header: " + wmoHeader)
        
        #Open and read in binary file, write wmo header to new file and wrote 
        # contents of file
        wmoFile = gzFile + '.wmo'
        log.info("Writing WMO header file: " + wmoFile)
        with open(workDir + '/' + gzFile,'rb') as old, open(workDir + '/' + wmoFile,'wb') as new:
            new.write(wmoHeader + "\r\r\n")
            for chunk in iter(lambda: old.read(65536), b""):
                new.write(chunk)
        
        #Write new file name to PSF         
        writePSF(workDir,wmoFile)
        
        log.info("WMO header formatting successful. Exiting.")
    except Exception:
        log.exception("Writing WMO header failed. Exiting.")
        sys.exit(os.EX_SOFTWARE)

if __name__ == "__main__":
    setuplogger(sys.argv[1])
    log.debug(sys.argv)
    main(sys.argv[1:])        
