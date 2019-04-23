#!/usr/bin/env perl
#
# originally created: 20180625 jhansford
#      created again: 20180801 jhansford -- the original file was lost and could not be recovered...
#
# this extracts metadata out of filenames like:
#      NCEP-NPP-OMPS-LP_v001_2018m0304t18z_2018m0304t220321-s0823884df5d2ad20bb751ee8ba5d81ae.bin
# and puts it in the form:
#      yyyy-mm-dd hh:mm:ss&yyyy-mmdd hh:mm:ss
#      ^^--begin time---^^ ^^---end time---^^
#
# The 'begin time' is the forecast time in the filename
# The 'end time' is six hours after the forecast time.

use Time::Local;

sub time_to_string {
	my $time = shift;

	(my $sec, my $min, my $hour, my $mday, my $mon, my $year, my $wday, my $yday, my $isdst) = localtime($time);

	$year = $year + 1900;
	$mon = $mon + 1;
	return sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year, $mon, $mday, $hour, $min, $sec);
}

# Main:


if ($ARGV[0] =~ /NCEP-NPP-OMPS-LP_v.*_(\d{4})m(\d{2})(\d{2})t(\d{2})z_\d{4}m\d{4}t\d{6}-s.*\.bin/ ) {
	my $year = int($1);
	my $month = int ($2) - 1;
	my $day = int($3);
	my $hour = int($4);

	my $start_time = timelocal(0, 0, $hour, $day, $month, $year);

	my $end_time = $start_time + 21600; # 21600 = 6 hours * 3600 seconds / hour

	print time_to_string($start_time) . "&" . time_to_string($end_time);

} else {
	print "ERROR: Given filename did not match the expected pattern.\n";
	exit(1);
}

