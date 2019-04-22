#!/usr/bin/env perl

#
# vpwExtractor.pl 
#
# this extracts metadata out of filenames like:
# nps_VIIRS_2013122_0126_49_ALL_3305_3305.nc
#
# puts it in the form:
#     yyyy-mm-dd 00:00:00&yyyy-mmdd 23:59:59&Global
#       ^^begin time^^        ^^end time^^    ^^data is global in spatial coverage^^

use Time::Local;

if ($ARGV[0] =~ /(.*)(\d{4})(\d{3})_(\d{2})(\d{2}).*/)
{   
    
    my $year = int($2);
    my $julianDay = int($3);
	
	my @date = julian2date($year,$julianDay);
	my $month = $date[0];
	my $day = $date[1];
	
    if ($day < 10){ $startDay = "0$day" }
    else{            $startDay = "$day"; }

    if ($month < 10){ $startMonth = "0$month"; }
    else{           $startMonth = "$month";  }
    
    $start = "$year-$startMonth-$startDay $4:$5:00";
    
    $month -= 1;
    $year -= 1900;
    $time = timelocal(0,$5,$4,$day,$month,$year);
    $time += 600; #Make 10 minutes long (time is start time for granule directly over the pole)
    ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($time);
    $mon = $mon + 1;

    if ($hour < 10){ $endHr = "0$hour"; }
    else{            $endHr = "$hour";  }

    if ($mday < 10){ $endDay = "0$mday" }
    else{            $endDay = "$mday"; }

    if ($mon < 10){ $endMonth = "0$mon"; }
    else{           $endMonth = "$mon";  }
    
    if ($hour < 10){ $endHour = "0$hour"; }
    else{           $endHour = "$hour";  }
    
    if ($min < 10){ $endMin = "0$min"; }
    else{           $endMin = "$min";  }
    
    if ($sec < 10){ $endSec = "0$sec"; }
    else{           $endSec = "$sec";  }

    $year += 1900;
    $end = "$year-$endMonth-$endDay $endHour:$endMin:$endSec";
    
    if ($1 =~ m/nps/) { #Northern hemisphere
    	$poly="0,70,90,70,90,90,0,90,0,70";
    } else {
    	$poly="0,-70,90,-70,90,-90,0,-90,0,-70";
    }
    
    print "$start&$end&$poly";

    exit(0);
}
exit(1);

sub julian2date {
	my ($year, $julianDay)=@_;
	
	#Determine if leap year
	if ( 0 == $year % 4 and 0 != $year % 100 or 0 == $year % 400 ) {
    	#then it is a leap year
    	#print "Leap year\n";
    	@julMonth = (0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366);
    } else {
    	#Not a leap year
    	#print "Not Leap year\n";
    	@julMonth = (0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365);
    }
    
    #Determine month and day from julian day
    for ($i=0;$i < 13; $i++) {
    	if ($julMonth[$i] >= $julianDay) {
    		$month = $i;
    		$day = $julianDay - $julMonth[$i-1];
    		last;
    	}
    }
    @date = ($month, $day);
    return @date;
}
