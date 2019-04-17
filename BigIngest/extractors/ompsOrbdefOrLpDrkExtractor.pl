#!/usr/bin/env perl
#
# created: 20180625 jhansford
#
# this extracts metadata out of filenames like
#      OMPS-NPP-NPP_NPP-ORBDEF_v1.0.2-2017m0305t2227-2017m0306t171127.txt
# * or *
#      OMPS-NPP_LP-DRK-p058_v2.0_2016m0810t102513_o24801_2016m0814t084027.h5
# and puts it in the form:
#      yyyy-mm-dd hh:mm:ss&yyyy-mmdd hh:mm:ss
#      ^^--begin time---^^ ^^---end time---^^^@
# The 'begin time' is the first timestamp in the filename
# The 'end time' is three weeks after the begin time.

use Time::Local;

sub time_to_string {
	my $time = shift;

	(my $sec, my $min, my $hour, my $mday, my $mon, my $year, my $wday, my $yday, my $isdst) = localtime($time);

	$year = $year + 1900;
	$mon = $mon + 1;
	return sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year, $mon, $mday, $hour, $min, $sec);
}

# Main:

my $year;
my $month;
my $day;
my $hour;
my $minute;
my $second;

if ($ARGV[0] =~ /.*-NPP.NPP-ORBDEF_v.*-(\d{4})m(\d{2})(\d{2})t(\d{2})(\d{2})-\d{4}m\d{4}t\d{6}\.txt/ ) {
	$year = int($1);
	$month = int($2) - 1;
	$day = int($3);
	$hour = int($4);
	$minute = int($5);
	$second = 0;
} 
elsif ($ARGV[0] =~ /OMPS-NPP_LP-DRK-p.*_v.*_(\d{4})m(\d{2})(\d{2})t(\d{2})(\d{2})(\d{2})_o\d{5}_\d{4}m\d{4}t\d{6}\.h5/) {
	$year = int($1);
	$month = int($2) - 1;
	$day = int($3);
	$hour = int($4);
	$minute = int($5);
	$second = int($6);
} 
else {
	print "ERROR: Given filename did not match one of the expected patterns.\n";
	exit(1);
}

my $start_time=timelocal($second, $minute, $hour, $day, $month, $year);

my $end_time = $start_time + 1814400; # 181440 = 21 days * 24 hours / day * 3600 seconds / hour

print time_to_string($start_time) . "&" . time_to_string($end_time);

