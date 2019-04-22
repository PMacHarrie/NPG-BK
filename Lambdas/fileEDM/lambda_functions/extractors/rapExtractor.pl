#!/usr/bin/env perl

#
# rapExtractor.pl
#
# revised: 20180726 jrh, creation
#
# this extracts metadata out of filenames like:
#     rap.t20z.awp242f02.grib2.20180726
# puts it in the form:
#     yyyy-mm-dd hh:mm:ss&yyyy-mmdd hh:mm:ss
#     ^^--begin time---^^ ^^---end time---^^

use Time::Local;

sub time_to_string {
	my $time = shift;

	(my $sec, my $min, my $hour, my $mday, my $mon, my $year, my $wday, my $yday, my $isdst) = localtime($time);
	
	$year = $year + 1900;
	$mon = $mon + 1;
	return sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year, $mon, $mday, $hour, $min, $sec);
}

# main:
if ($ARGV[0] =~ /.*t(\d{2})z.*f(\d{2}).*(\d{4})(\d{2})(\d{2})/)
{
	my $model_init_hour = $1;
	my $forecast_hour = int($2);
    my $year = int($3);
    my $month = int($4) - 1;
	my $day = int($5);

	my $model_init_time = timelocal(0, 0, $model_init_hour, $day, $month, $year);

    my $forecast_second = $2 * 3600;
	my $start_time = $model_init_time + $forecast_second - 1800;
	my $end_time = $model_init_time + $forecast_second + 1800;

	print time_to_string($start_time) . "&" . time_to_string($end_time);
}
else 
{
	print "ERROR: Given filename did not match the expected pattern of a RAP filename.\n";
	exit(1);
}

