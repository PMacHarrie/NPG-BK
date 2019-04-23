 #!/usr/bin/env perl

#
# gfsExtractor.pl
#
# revised: 20110606 hp, -/+ 1.5 hrs range (removal of prisNeed=REQUIREDPOINT
# this extracts metadata out of filenames like:
#     gfs.t18z.pgrbf06.20080831
# puts it in the form:
#     yyyy-mm-dd hh:mm:ss&yyyy-mmdd hh:mm:ss
#     ^^--begin time---^^ ^^---end time---^^

use Time::Local;

if ($ARGV[0] =~ /.*t(\d{2})z.pgrb2f(\d{2}).*(\d{4})(\d{2})(\d{2})/ || $ARGV[0] =~ /.*t(\d{2})z.pgrb2.*f(\d{3}).*(\d{4})(\d{2})(\d{2})/)
{
    $year = int($3)-1900;
    $month = int($4)-1;
    $time=timelocal(0,0,$1,$5,$month,$year);

    $duration = $2*3600;
    $start=$time+$duration-10800;
    $end  =$time+$duration+10800;

    ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($start);

    $startMin = $min;

    if ($hour < 10){ $startHr = "0$hour"; }
    else{            $startHr = "$hour";  }

        if ($min < 10){ $startMin = "0$min"; }
    else{            $startMin = "$min";  }

    if ($mday < 10){ $startDay = "0$mday" }
    else{            $startDay = "$mday"; }

    $mon = $mon+1;
    if ($mon < 10){ $startMonth = "0$mon"; }
    else{           $startMonth = "$mon";  }

    $startYear = $year+1900;


    ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($end);

    $endMin = $min;

    if ($hour < 10){ $endHr = "0$hour"; }
    else{            $endHr = "$hour";  }

        if ($min < 10){ $endMin = "0$min"; }
    else{            $endMin = "$min";  }

    if ($mday < 10){ $endDay = "0$mday" }
    else{            $endDay = "$mday"; }

    $mon = $mon+1;
    if ($mon < 10){ $endMonth = "0$mon"; }
    else{           $endMonth = "$mon";  }

    $endYear = $year+1900;

    print "$startYear-$startMonth-$startDay $startHr:$startMin:00&$endYear-$endMonth-$endDay $endHr:$endMin:00&Global";

}
exit(0);
