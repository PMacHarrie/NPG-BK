#!/usr/bin/env perl

#
# name:    ordinalDateExtractor.pl 
# revised: 
# 20161110 klr creation (ENTR-3390)
# 20170525 klr ENTR-3890 for GOES: grab decimal part of seconds, and don't 
#              add extra 0 if hh,mm,ss are lt 10
# 20180404 jrh ENTR-4648 for LSA: filename contains only one date of the form yyyyjjj
# 20180620 jrh (For NDE-in-the-Cloud): Changed DMW file pattern to support all GOES ABI products.
# 20180727 jrh combined changes made on 20180404 and 20180620 (the NDE-in-the-Cloud changes were not built on top of the ENTR-4648 changes).
# 20181102 jrh added the ability to accept IMS Snow mask filenames (two different forms).
#
# this extracts ordinal day metadata out of filenames like:
# nps_VIIRS_2013122_0126_49_ALL_3305_3305.nc (for VPW)
#    or
# DR_ABI-L2-DMWF-M4C08_G16_s20161961921025_e20161961925434_c20161961933452.nc (for GOES-R DMW)
#    or
# VIIRS_LSA_2014100_h35v13.nc (for LSA offline primary tiles)
#    or
# h20v17_2015009_FLT.nc (for LSA offline filtered tiles)
#    or
# ims2018134_4km_v1.3.asc
#    or
# NIC.IMS_v3_201825700_4km.asc.gz
#
# and puts it in the calendar form:
#     yyyy-mm-dd 00:00:00&yyyy-mm-dd 23:59:59&polygon
#     ^^-----begin-----^^ ^^------end------^^ ^^(for VPW)^^
#
#
# NOTES:
# - The script references julian days, but technically they are ordinal days, 
#   or days-of-the-year.
# - There isn't any range checking in this script.
# 

use strict;
use Time::Local;


# Declare variables

my $startYear;
my $startJulianDay;
my $startMonth;
my $startDay;
my $startHour;
my $startMin;
my $startSec;

my $endYear;
my $endJulianDay;
my $endMonth;
my $endDay;
my $endHour;
my $endMin;
my $endSec;

# only for VPW
my $hemi;
my $poly = "";    


# VPW
if ($ARGV[0] =~ /(.*)_VIIRS_(\d{4})(\d{3})_(\d{2})(\d{2}).*/) {

  $hemi           = $1;
  $startYear      = int($2);
  $startJulianDay = int($3);
  $startHour      = $4;
  $startMin       = $5;
  $startSec       = "00";
  
  ($startMonth,$startDay) = julian2date($startYear,$startJulianDay);
  
  # the end time will be 10 min later than the start time
  # (time is start time for granule directly over the pole)
  my $month = $startMonth - 1;
  my $year  = $startYear - 1900;
  my $startTimeLocal = timelocal($startSec,$startMin,$startHour,$startDay,$month,$year);
  my $endTimeLocal = $startTimeLocal + 600;  # add 10 min
  my ($wday,$yday,$isdst);
  ($endSec,$endMin,$endHour,$endDay,$endMonth,$endYear,$wday,$yday,$isdst) = localtime($endTimeLocal);
  
  $endMonth = $endMonth + 1;
  $endYear  = $endYear + 1900;
  
  # create polygon string
  $poly = "&";
  # Northern hemisphere
  if ($hemi =~ m/nps/) { 
    $poly .= "0,70,90,70,90,90,0,90,0,70";
  } else {
  # Southern hemisphere
    $poly .= "0,-70,90,-70,90,-90,0,-90,0,-70";
  }
  
  
} elsif ($ARGV[0] =~ /.*ABI-L(.*)_s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d{1})_e(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d{1})_.*/) {
# GOES ABI

  $startYear      = int($2);
  $startJulianDay = int($3);
  $startHour      = $4;
  $startMin       = $5;
  $startSec       = "$6.$7";
  
  $endYear      = int($8);
  $endJulianDay = int($9);
  $endHour      = $10;
  $endMin       = $11;
  $endSec       = "$12.$13";
  
  ($startMonth,$startDay) = julian2date($startYear,$startJulianDay);
  
  ($endMonth,$endDay) = julian2date($endYear,$endJulianDay);

} elsif ($ARGV[0] =~ /.*VIIRS_LSA_(\d{4})(\d{3})_h\d{2}v\d{2}.*/ or $ARGV[0] =~ /.*h\d{2}v\d{2}_(\d{4})(\d{3})_FLT.*/ or $ARGV[0] =~ /.*ims(\d{4})(\d{3})/) {
# JPSSRR Primary LSA tile file, LSA Filtered tile file, or IMS Snow Mask (DEV pattern)

  $startYear      = int($1);
  $startJulianDay = int($2);
  $startHour      = "00";
  $startMin       = "00";
  $startSec       = "00.0";

  $endYear      = $startYear;
  $endJulianDay = $startJulianDay;
  $endHour      = "23";
  $endMin       = "59";
  $endSec       = "59.9";

  ($startMonth, $startDay) = julian2date($startYear,$startJulianDay);
  ($endMonth,$endDay) = julian2date($endYear,$endJulianDay);
} elsif ($ARGV[0] =~ /.*NIC\.IMS.*(\d{4})(\d{3})(\d{2}).*\.asc\.gz/) {
# IMS Snow Mask (OPS pattern - as of 10/30/2018)
# Last two digits represent the hour. Two files are delivered for each day - labeled 00 hour, and the other labeled 18 hour.  
# Therefore, the 00 hour file is given an 18-hour duration, and the 18 hour file a 6-hour duration.
#
# ALSO... the data inside the .asc.gz file is one day behind the date in the filename.
# This is not true for the unzipped version used in DEV.

  # Get the date in the file; convert from day-of-year to (month, day-of-month).
  my $fileYear = int($1);
  my $fileJulianDay = int($2);
  my ($fileMonth, $fileDay) = julian2date($fileYear, $fileJulianDay);
  my $fileDate = timelocal(0, 0, 0, $fileDay, $fileMonth - 1, $fileYear);

  # Substract one day from the date in the file.
  my $startDate = $fileDate - 24*3600;
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($startDate);

  $startYear      = $year + 1900;
  $startMonth     = $mon + 1;
  $startDay       = $mday;
  $startHour      = int($3);
  $startMin       = "00";
  $startSec       = "00";

  $endYear       = $startYear;
  $endMonth      = $startMonth;
  $endDay        = $startDay;

  if ($startHour == 0) {
    $endHour = "17";
  } elsif($startHour == 18) {
    $endHour = "23";
  } else {
    print "ERROR: IMS Snow Mask input file had invalid hour: $startHour \n";
    exit(1);
  }

  $endMin        = "59";
  $endSec        = "59.9";

} else {
  
  # +++ who/what would see this?  should we have more error checking?
  print "ERROR: Input filename did not match pattern for VPW, GOES ABI, LSA, or IMS Snow Mask \n"; 
  exit(1);

}


# make sure everything is 2 digit that came from julian2date
if ($startMonth < 10) { $startMonth = "0$startMonth"; }
else                  { $startMonth = "$startMonth";  }

if ($startDay < 10)   { $startDay = "0$startDay"; } 
else                  { $startDay = "$startDay";  }

if ($endMonth < 10)   { $endMonth = "0$endMonth"; }
else                  { $endMonth = "$endMonth";  }

if ($endDay < 10)     { $endDay = "0$endDay"; }
else                  { $endDay = "$endDay";  }


# create the start and end strings
my $start = "$startYear-$startMonth-$startDay $startHour:$startMin:$startSec";
my $end   = "$endYear-$endMonth-$endDay $endHour:$endMin:$endSec";


# print the final string
my $result = "$start&$end";
$result .= $poly;   # this is only filled for VPW
print $result;



# convert a date from yyyy and jjj to an array containing month and day
sub julian2date {
  
  my ($year, $julianDay) = @_;
  my @julianMonth;
  my $month;
  my $day;
  my @date;
  
  # Determine if leap year
  if ( 0 == $year % 4 and 0 != $year % 100 or 0 == $year % 400 ) {
    # Is a leap year
    @julianMonth = (0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366);
  } else {
    # Not a leap year
    @julianMonth = (0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365);
  }
  
  # Determine month and day from julian day
  for my $ii (0 .. $#julianMonth) {
    if ($julianMonth[$ii] >= $julianDay) {
      $month = $ii;
      $day = $julianDay - $julianMonth[$ii-1];
      last;
    }
  }
  
  @date = ($month, $day);
  return @date;
  
} # end-sub julian2date

