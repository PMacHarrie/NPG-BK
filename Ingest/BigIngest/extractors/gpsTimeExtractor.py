#!/usr/bin/env python
"""
@author: dcp

Extractor pulls GPS time from file name and converts to UTC time. Assumes GPS
time is in seconds from GPS EPOCH. Included is a function for converting
GPS Week and Seconds of Week (SOW) to UTC time but no need to implement at this
time.

Call:
    gpsTimeExtractor GPSSEC_TIME_s1081207756_e1081219633.example

As of 2015, there have been 17 leap seconds since the GPS epoch. This will need
to be updated as leap seconds are added.

Modification:
        20150903 dcp, created
"""
import sys, os, re
import time, math

secsInWeek = 604800
secInDay = 86400
gpsEpoch = (1980, 1, 6, 0, 0, 0) #(year, month, day, hour, min, sec)

def UTCFromGpsWeek(gpsWeek, SOW, leapSecs=17):
    """converts gps week and seconds to UTC
    SOW = seconds of week
    gpsWeek is the full number (not modulo 1024)
    As of 2015, there have been 17 leap seconds since the GPS epoch
    """
    secFract = SOW % 1
    epochTuple = gpsEpoch + (-1, -1, 0)
    t0 = time.mktime(epochTuple) - time.timezone  #mktime is localtime, correct for UTC
    tdiff = (gpsWeek * secsInWeek) + SOW - leapSecs
    t = t0 + tdiff
    (year, month, day, hh, mm, ss, dayOfWeek, julianDay, daylightsaving) = time.gmtime(t)
    #use gmtime since localtime does not allow to switch off daylighsavings correction!!!
    utcTime = [year, month, day, hh, mm, ss + secFract]
    return utcTime

def UTCFromGpsSec(gpsSOW, leapSecs=17):
    """converts gps seconds to UTC
    gpsSOW = GPS seconds since GPS epoch
    As of 2015, there have been 17 leap seconds since the GPS epoch
    """
    secFract = int(gpsSOW) % 1
    epochTuple = gpsEpoch + (-1, -1, 0)
    t0 = time.mktime(epochTuple) - time.timezone  #mktime is localtime, correct for UTC
    tdiff = int(gpsSOW) - leapSecs
    t = t0 + tdiff
    (year, month, day, hh, mm, ss, dayOfWeek, julianDay, daylightsaving) = time.gmtime(t)
    #use gmtime since localtime does not allow to switch off daylighsavings correction!!!
    utcTime = [year, month, day, hh, mm, ss + secFract]
    return utcTime

def main(argv):
    try:
        fileName = sys.argv[1]
    except Exception:
        print ('ERROR: Invalid arguments. Must pass file name')
        sys.exit(os.EX_USAGE)

    try:
        startGPSTime = None
        endGPSTime = None
        utcStartTime = None
        utcEndTime = None

        if re.search('s\d{10}',fileName):
            startGPSTime = re.search('s\d{10}',fileName).group(0)[1:]
            utcStartTime = UTCFromGpsSec(startGPSTime)
        if re.search('e\d{10}',fileName):
            endGPSTime = re.search('e\d{10}',fileName).group(0)[1:]
            utcEndTime = UTCFromGpsSec(endGPSTime)

        if not utcStartTime: utcStartTime = utcEndTime
        if not utcEndTime: utcEndTime = utcStartTime
        if not utcStartTime and not utcEndTime:
            print ("ERROR: No UTC time returned from GPS time")
            sys.exit(os.EX_SOFTWARE)

        # Format for JAVA TIMESTAMP (SQL) data type
        if utcStartTime[1] < 10: utcStartTime[1] = '0' + str(utcStartTime[1])
        if utcStartTime[2] < 10: utcStartTime[2] = '0' + str(utcStartTime[2])
        if utcStartTime[3] < 10: utcStartTime[3] = '0' + str(utcStartTime[3])
        if utcStartTime[4] < 10: utcStartTime[4] = '0' + str(utcStartTime[4])
        if utcStartTime[5] < 10: utcStartTime[5] = '0' + str(utcStartTime[5])
        if utcEndTime[1] < 10: utcEndTime[1] = '0' + str(utcEndTime[1])
        if utcEndTime[2] < 10: utcEndTime[2] = '0' + str(utcEndTime[2])
        if utcEndTime[3] < 10: utcEndTime[3] = '0' + str(utcEndTime[3])
        if utcEndTime[4] < 10: utcEndTime[4] = '0' + str(utcEndTime[4])
        if utcEndTime[5] < 10: utcEndTime[5] = '0' + str(utcEndTime[5])

        print(str(utcStartTime[0]) +'-' + str(utcStartTime[1]) + '-' + str(utcStartTime[2])
            + " " + str(utcStartTime[3]) + ":" + str(utcStartTime[4]) + ":" + str(utcStartTime[5])
            + "&" + str(utcEndTime[0]) +'-' + str(utcEndTime[1]) + '-' + str(utcEndTime[2])
            + " " + str(utcEndTime[3]) + ":" + str(utcEndTime[4]) + ":" + str(utcEndTime[5]))

        sys.exit(0)

    except Exception:
        print ('ERROR: Failed extraction of GPS time and conversion to UTC obs time')
        sys.exit(os.EX_SOFTWARE)

if __name__ == "__main__":
    main(sys.argv[1:])

