#!/usr/bin/env perl

#
# leapsecOrUtcpoleExtractor.pl
#
# created: 20180625 jhansford
# revised: 20181025 jhansford - prepended "$incomingdir/" to the command line argument when opening the file.
#
# this extracts metadata out of files like:
#     utcpole.dat.2012022406905 * or * leapsec.dat.2012022406905
# which contain a time somewhere in the first line of the file, formatted like:
#     2010-12-31T19:50:09Z
#
# puts it in the form:
#     yyyy-mm-dd hh:mm:ss&yyyy-mmdd hh:mm:ss
#     ^^--begin time---^^ ^^---end time---^^
#
# The 'begin time' is extracted from the first line of the file.
# The 'end time' is three weeks after the begin time.
# The file is assumed to be in the '$incomingdir' directory.

use Time::Local;

sub time_to_string {
	my $time = shift;

	(my $sec, my $min, my $hour, my $mday, my $mon, my $year, my $wday, my $yday, my $isdst) = localtime($time);
	
	$year = $year + 1900;
	$mon = $mon + 1;
	return sprintf("%04d-%02d-%02d %02d:%02d:%02d", $year, $mon, $mday, $hour, $min, $sec);
}

# Main:

my $full_path_to_file = $ENV{'incomingdir'} . '/' . $ARGV[0];

open(my $file_handle, "<", $full_path_to_file) or die "ERROR: Could not open file: $full_path_to_file\n";
my $first_line = readline($file_handle);
close($file_handle);

if ($first_line =~ /(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z/) {
	my $year = int($1);
	my $month = int($2) - 1;
	my $day = int($3);
	my $hour = int($4);
	my $minute = int($5);
	my $second = int($6);

	my $start_time=timelocal(0, 0, 0, $day, $month, $year);
	my $end_time = $start_time + 1814400; # 181440 = 21 days * 24 hours / day * 3600 seconds / hour
	print time_to_string($start_time) . "&" . time_to_string($end_time);
} else {
	print "ERROR: The time could not be found in the first line of the file.\n";
	exit(1);
}
