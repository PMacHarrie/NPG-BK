"""
Author: Jonathan Hansford @Solers Inc. <jonathan.hansford@noaa.gov>
Revision History:
        - 10/15/2018 - created <jhansford>

This script is used to extract metadata from the GOES FLS Cloud files.
The GOES FLS Cloud files are netCDF with containing a global attribute "secTimeStr"
The "secTimeStr" attribute is formatted like:
        s201805141742215_e201805141744588_c201808150817040

Note: this script assumes that the file being ingested is in $incomingdir, unless a full or relative path to it is supplied.
This means that if the file is in the current directory, you must provide the script ./<filename> as the argument.
"""

import netCDF4
import os
import re
import sys

def formatTimestampString(timestampString):
    """
    Takes a string of the form YYYYMMDDhhmmsst and converts it to the form YYYY-MM-DD hh:mm:ss.t.
    """
    return "%s-%s-%s %s:%s:%s.%s" % (timestampString[0:4], timestampString[4:6], timestampString[6:8], timestampString[8:10], timestampString[10:12], timestampString[12:14], timestampString[14])

# __main__:

if len(sys.argv) != 2:
    print("Usage: %s <file>" % sys.argv[0])
    raise Exception("Usage: %s <file>" % sys.argv[0])

file = sys.argv[1]

path_to_file = file
if '/' not in file and 'incomingdir' in os.environ:
    path_to_file = os.environ['incomingdir'] + '/' + file

if not os.path.exists(path_to_file):
    print("Unable to find file: " + path_to_file)
    raise Exception("Unable to find file: " + path_to_file)

netcdf_dataset = netCDF4.Dataset(path_to_file, mode='r')
try:
    secTimeStr = netcdf_dataset.getncattr('secTimeStr')
    matchObject = re.match(r's(\d{15})_e(\d{15})', secTimeStr)
    if matchObject is None:
        raise Exception("secTimeStr attribute was not in expected format. secTimeStr was: %s" % secTimeStr)
    else:
        startingTime, endingTime = matchObject.group(1, 2)
        sys.stdout.write("%s&%s" % (formatTimestampString(startingTime), formatTimestampString(endingTime)))
finally:
    netcdf_dataset.close()
