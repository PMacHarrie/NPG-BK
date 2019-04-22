#!/usr/bin/env perl

# revised: 09232013 angela created
# for filename like jtwc_aep992013.dat.201309041415 or
#                   nhc_aep992013.dat.201309041415
# this finds last CARQ line and puts it in the form:
#     yyyy-mm-dd hh:mm:ss&yyyy-mmdd hh:mm:ss
#     ^^--begin time---^^ ^^---end time---^^
# begin time comes from last CARQ name
# end time is begin time +5 hours 59 minute
#
# revised: 20150826 dcp, file name pattern will change with PDA pre-pending the date as:
#                                       YYYYMMDDhhmmss-jtwc_ash952015.dat
#                                       YYYYMMDDhhmmss-nhc_ash952015.dat.gz

use Time::Local;
use File::Copy;

my $filename = $ARGV[0];
my $savename = $filename;

# may need to gunzip filename
# if so we are assuming name is similar to 'nhc_aep032014.dat.gz.201406122100'
# UPDATE: we now assume file is 20140612210000-nhc_aep032014.dat.gz
if ($filename =~ m/gz/) {
   @namepos=split(/\./,$filename);
#   my $tmpgzfile=join ".",$namepos[0],$namepos[1],$namepos[3],$namepos[2];

   my $indir = ".";
   my $orgfile = $indir . "/" . $filename;
#   my $gzfile = $indir. "/" . $tmpgzfile;
#   copy($orgfile,$gzfile) or die "File cannont be copied\n";;
#   system("gunzip","-f","$gzfile");
   system("gunzip","-f","$orgfile");
   if ($?==-1) {die $? >> 8};

#  make filename be the unzipped file
   $filename=join ".",$namepos[0],$namepos[1];
}


my $dir = $ENV{'incomingdir'};
#print "directory: $dir\n";
my $path = $dir . "/" . $filename;
#print "$path \n";
open FILE, $path or die $!;

###############################
# FIND LAST CARQ LINE
# CALCULATE START AND END TIMES
###############################

my $line = "";
my $lastLine = "";

while($line=<FILE>){
        my @values = split(', ', $line);
        #print $line;
        if ($values[4] eq "CARQ") {
                #print "...$line\n";
                $lastcarqline=$line;
                }
}

#print "$lastcarqline\n";

my @linevals = split(', ', $lastcarqline);
$datestring = "";
$datestring = $linevals[2];

#print "$datestring\n";

$inyear = substr($datestring, 0,4);
$inmonth = substr($datestring, 4,2);
$inday = substr($datestring, 6,2);
$inhour = substr($datestring, 8,2);

$year = int($inyear)-1900;
$month = int($inmonth)-1;
$day = int($inday);
$hour = int($inhour);
$min=0;
$sec=0;

#print "month=$month\n";
$start=timegm($sec,$min,$hour,$day,$month,$year);

# add 6 hours - 1 min (in secs)

$end=$start+(6*60*60)-60;

($sec,$min,$hour,$day,$month,$year,$wday,$yday,$isdst) = gmtime($end);

$year+=1900;
$month+=1;

#print "$year\n";
#print "$month\n";
#print "$day\n";
#print "$hour\n";
#print "$min\n";
#print "$sec\n";


# format

$endyear=$year;
if (int($month) < 10){ $endmonth = "0$month"; }
   else{            $endmonth = "$month";  }
if (int($day) < 10){ $endday = "0$day"; }
   else{            $endday = "$day";  }
if (int($hour) < 10){ $endhour = "0$hour"; }
   else{            $endhour = "$hour";  }
if (int($min) < 10){ $endmin = "0$min"; }
   else{            $endmin = "$min";  }

$yearstring="$inyear-$inmonth-$inday $inhour:00:00&$endyear-$endmonth-$endday $endhour:$endmin:59";
#print $yearstring;
#print "\n";
#print "$inyear-$inmonth-$inday $inhour:00:00&$endyear-$endmonth-$endday $endhour:$endmin:59";
#print "\n";

####################
# BASIN SDO GEOMETRY
####################

# figure out basin from filename

if ( ($filename =~ /.*-a(.+)\.dat/i) )
   {
   $basin=substr($1,0,2);
   }

if    ($basin eq "al") { $poly="-110.000,70.000,-110.000,-10.000,20.000,-10.000,20.000,70.000,-110.000,70.000"; }
elsif ($basin eq "ep") { $poly="160.000,50.000,-80.000,50.000,-80.000,-10.000,160.000,-10.000,160.000,50.000" }
elsif ($basin eq "cp") { $poly="160.000,50.000,-80.000,50.000,-80.000,-10.000,160.000,-10.000,160.000,50.000" }
elsif ($basin eq "wp") { $poly="85.000,70.000,85.000,-10.000,-160.000,-10.000,-160.000,70.000,85.000,70.000" }
elsif ($basin eq "io") { $poly="45.000,40.000,45.000,-10.000,95.000,-10.000,95.000,40.000,45.000,40.000" }
elsif ($basin eq "sh") { $poly="35.000,10.000,-70.000,10.000,-70.000,-50.000,35.000,-50.000,35.000,10.000" }
else {;} 


   

print "$yearstring&$poly";
print "\n";

close FILE or die $!;

# if file was gunzipped we need to zip it back up
if ($filename ne $savename) {
   system("gzip","$path");
   if ($?==-1) {die $? >> 8};
   }



