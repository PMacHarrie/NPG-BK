#!/usr/bin/env python2.7
"""
20141029, created

@author: dcp

Formatter modifies a tailored ACSPO GHRSST NC4 file and adds/modifies various 
attributes to meet GHRSST Data Specification (GDS): 
https://www.ghrsst.org/documents/q/category/ghrsst-data-processing-specification-gds/

"""
import os, sys, re
import h5py
import numpy as np
import logging
from util import readPCF
from util import readPSF
from util import writePSF
from util import setuplogger

log = logging.getLogger("formatting")

def main(argv):
    try:
        workDir = sys.argv[1]
    except Exception:
        log.exception("Invalid argument. Must be working directory.")
        sys.exit(os.EX_USAGE)
        
    try:
        log.info("Executing GDS formatting for GHRSST tailored products...")
        
        #Open tailored NC4 GHRSST file        
        nc4File = readPSF(workDir)
        nc4f = h5py.File(nc4File,'r+')
        log.info("Formatting: " + nc4File + " to GHRSST Data Specification")
        
        #Open original GHRSST file
        origFile = readPCF(workDir,"input1")
        origf = h5py.File(origFile,'r')
        
        #Read in attributes to be updated
        log.info("Reading GDS attributes from GHRSST file: " + origFile)
        creationDate = nc4f.attrs.get("date_created")
        creationDate = re.sub('[-:]','',creationDate)
        startTime = origf.attrs.get("start_time")
        stopTime = origf.attrs.get("stop_time")
        nlat = origf.attrs.get("northernmost_latitude")
        slat = origf.attrs.get("southernmost_latitude")
        elon = origf.attrs.get("easternmost_longitude")
        wlon = origf.attrs.get("westernmost_longitude")
        origf.close()
        log.debug("Reading GHRSST attributes: ")
        log.debug("\tstart_time: " + startTime)
        log.debug("\tstop_time: " + stopTime)
        log.debug("\tnorthernmost_latitude: " + nlat)
        log.debug("\tsouthernmost_latitude: " + slat)
        log.debug("\teasternmost_longitude: " + elon)
        log.debug("\twesternmost_longitude: " + wlon)
        log.debug("\tdate_created: " + creationDate)
        
        #Write attributes to nc4 tailored file
        log.info("Writing GDS attributes")
        nc4f.attrs.modify("date_created",creationDate)
        nc4f.attrs.create("start_time",startTime)
        nc4f.attrs.create("stop_time",stopTime)
        nc4f.attrs.create("northernmost_latitude",nlat,dtype='float32')
        nc4f.attrs.create("southernmost_latitude",slat,dtype='float32')
        nc4f.attrs.create("easternmost_longitude",elon,dtype='float32')
        nc4f.attrs.create("westernmost_longitude",wlon,dtype='float32')
        
        #Change array attribute name "missing_value" to "_FillValue"
        log.info("Changing all missing_value array attribute to _FillValue")
        grp = nc4f.iteritems()
        for items in grp:
            missingValue = nc4f[items[0]].attrs.get("missing_value")
            if missingValue:
                arrayType = nc4f[items[0]].dtype.name
                nc4f[items[0]].attrs.__delitem__("missing_value")
                nc4f[items[0]].attrs.create("_FillValue",missingValue,dtype=arrayType)
        
        #Delete NDE global attributes (N/A) - don't want to confuse anyone
        log.info("Deleting any NDE specific global attributes")
        nc4f.attrs.__delitem__("nc4_compression_flag")
        nc4f.attrs.__delitem__("compression_level")
        nc4f.attrs.__delitem__("Metadata_Link")
        
        nc4f.close()
        
        log.info("GHRSST formatting successful. Exiting.")
    except Exception:
        log.exception("Updating GHRSST tailored NC4 to GDS specification failed. Exiting.")
        sys.exit(os.EX_SOFTWARE)

if __name__ == "__main__":
    setuplogger(sys.argv[1])
    log.debug(sys.argv)
    main(sys.argv[1:])        
