#!/usr/bin/env perl

#
# reynoldsExtractor.pl 
#
# this extracts metadata out of filenames like:
# avhrr-only-v2.20090522.nc OR YYYYMMDD.%
#This extractor should be used for data that spans 24 hours (once a day).

# puts it in the form:
#     yyyy-mm-dd 00:00:00&yyyy-mmdd 23:59:59&Global
#       ^^begin time^^        ^^end time^^    ^^data is global in spatial coverage^^
#                                           (this part is not included for some products)

use Time::Local;

if ($ARGV[0] =~ /(\D*)(\d{4})(\d{2})(\d{2}).*/)
{
    $year = int($2) - 1900;
    $month = int($3) - 1;
    $day = int($4);
    $time=timelocal(0,0,0,$day,$month,$year);
    
    if ( $ARGV[0] =~ m/avhrr/ or $ARGV[0] =~ m/CMC-L4/ ) {
    	$start=$time+86400; #SST climatologies are for day of ingest date but have file stamp for previous day
    } else {
    	$start=$time; 
    }

    if ( $ARGV[0] =~ m/VIIRS\.LSE/ or $ARGV[0] =~ m/VIIRS\.Daily\.Rolling\.SnowFraction/ ) {
        # Geographic metadata should not be inserted into the database for the outputs of JRR LSE Offline.
     	# Although the products do have global coverage, PDA cannot accept the 4-polygon representation that "Global" is converted into.
        $geo = "";
    } else {
        $geo = "&Global";
    }

    ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($start);

    $mon = $mon + 1;

    if ($mday < 10){ $startDay = "0$mday" }
    else{            $startDay = "$mday"; }

    if ($mon < 10){ $startMonth = "0$mon"; }
    else{           $startMonth = "$mon";  }

    $startYear = $year+1900;

    print "$startYear-$startMonth-$startDay 00:00:00&$startYear-$startMonth-$startDay 23:59:59$geo";

    exit(0);
}
exit(1);
