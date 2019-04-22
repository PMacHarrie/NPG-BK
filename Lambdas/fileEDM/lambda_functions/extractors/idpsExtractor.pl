#!/usr/bin/env perl

#
# protoExtractor.pl
# 20160707 - teh - ENTR-2989 adding an actual idps file type - Mission-Notice-AUX to idpsExtractor.pl
#
# this extracts metadata out of filenames like:
#     GATMO_npp_d20090226_t1753270_e1759270_b00000_c20090226000000000000_noaa_ops.nc
# or like:
#     Mission-Notice-AUX_cmn_20160426150258Z_20160426150258Z_ee00000000000000Z_-_c3su_ops_all-_all.xml
# puts it in the form:
#     yyyy-mm-dd hh:mm:ss&yyyy-mmdd hh:mm:ss
#       ^^begin time^^        ^^end time^^

use Time::Local;

if ($ARGV[0] =~ /.*d(\d{4})(\d{2})(\d{2})_t(\d{2})(\d{2})(\d{2})(\d{1})_e(\d{2})(\d{2})(\d{2})(\d{1})*/)
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
} elsif ($ARGV[0] =~ /.*_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})Z_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})Z_.*/)
{
    $start="$1-$2-$3 $4:$5:$6";
    $end="$7-$8-$9 $10:$11:$12";
    print "$start&$end";
    exit(0);
}
exit(1);

