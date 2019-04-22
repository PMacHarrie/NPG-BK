#!/usr/bin/env perl

#
# yymmddExtractor.pl
#
# revised: 20181003 jhansford, creation
#
# this extracts metadata out of filenames containing a single date of the form:
#     YYYMMDD
# such as:
#     snow_map_4km_180514.nc	
# puts it in the form:
#     yyyy-mm-dd hh:mm:ss&yyyy-mmdd hh:mm:ss
#     ^^--begin time---^^ ^^---end time---^^
#
# This should be used only for data that spans 24 hours (once per day)

use Time::Local;

sub time_to_string {
	my $time = shift;

	(my $sec, my $min, my $hour, my $mday, my $mon, my $year, my $wday, my $yday, my $isdst) = localtime($time);
	
	$year = $year + 1900;
	$mon = $mon + 1;
	return sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year, $mon, $mday, $hour, $min, $sec);
}

# main:
if ($ARGV[0] =~ /(\d{2})(\d{2})(\d{2})/)
{
	my $year_without_century = int($1);
	my $month = int($2) - 1;
	my $day = int($3);

	# Passing the year without the century (in the range of 0..99) is interpreted as the "current century"
	# See https://perldoc.perl.org/Time/Local.html
	
	my $start_time = timelocal(0, 0, 0, $day, $month, $year_without_century);
	my $end_time = timelocal(59, 59, 23, $day, $month, $year_without_century);

	print time_to_string($start_time) . "&" . time_to_string($end_time);
}
else 
{
	print "ERROR: Given filename did not match the expected pattern of six consecutive digits.\n";
	die;
}

