#!/usr/bin/env perl

#
# protoExtractor.pl 
#
# this extracts metadata out of filenames like:
#     NUCAPS_ALL_20090226_1753271_1759271.nc
# puts it in the form:
#     yyyy-mm-dd hh:mm:ss&yyyy-mmdd hh:mm:ss
#       ^^begin time^^        ^^end time^^

use Time::Local;

if ($ARGV[0] =~ /.*_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})(\d{1})_(\d{2})(\d{2})(\d{2})(\d{1}).*/)
{
    $start = "$1-$2-$3 $4:$5:$6";

    $fileStartSec = $4*3600+$5*60+$6;
    $fileEndSec = $8*3600+$9*60+$10;
    
    if ($fileEndSec < $fileStartSec)
    {
        $duration = $fileEndSec-$fileStartSec+86400;
    }else{
        $duration = $fileEndSec-$fileStartSec;
    }



    $year = int($1)-1900;
    $month = int($2)-1;
    $time = timelocal($6,$5,$4,$3,$month,$year);
    $time = $time + $duration;
    ($ltsec,$ltmin,$lthour,$ltmday,$ltmon,$ltyear,$ltwday,$ltyday,$ltisdst) = localtime($time);

    if ($ltsec < 10){
        $endSec = "0$ltsec";
    }else{
        $endSec = "$ltsec";
    }
 
    if ($ltmin < 10){
        $endMin = "0$ltmin";
    }else{
        $endMin = "$ltmin";
    }

    if ($lthour < 10){
        $endHr = "0$lthour";
    }else{
        $endHr = "$lthour";
    }
    
    if ($ltmday < 10){
        $endDay = "0$ltmday";
    }else{
        $endDay = "$ltmday";
    }
   
    $ltmon = $ltmon+1;
    if ($ltmon < 10){
        $endMon = "0$ltmon";
    }else{
        $endMon = "$ltmon";
    }

    $endYear = $ltyear+1900;

    $end = "$endYear-$endMon-$endDay $endHr:$endMin:$endSec";

    print "$start&$end";
    exit(0);
}
exit(1);
