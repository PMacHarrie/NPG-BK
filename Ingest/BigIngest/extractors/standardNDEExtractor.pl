#!/usr/bin/perl
#
# 20110310 dcp, revised to comply with NDE file naming conventions
# 20131028 angela, revised to handle filenames with no hh,mm,ss.
# 20131125 dcp, made more flexible by eliminating required seconds after ss (no millisecs needed)
# 20150331 dcp, greatly simplified by eliminating any calculations or time modules. Just grab the time from the file.
#
# this extracts metadata out of filenames like:
#    NUCAPS_EDR_v06-08-10_npp_s201102171141019_e201102171141317_c20110217135103.nc
#    GVF-WKL-GLB_v1r0_npp_s20121107_e20121113_c201310250235510.nc
# and puts it in the form:
#     yyyy-mm-dd hh:mm:ss&yyyy-mm-dd hh:mm:ss
#       ^^begin time^^        ^^end time^^
#

if ($ARGV[0] =~ /.*s(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2}).*_e(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2}).*_c(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})*/)
{
    $start = "$1-$2-$3 $4:$5:$6";
    $end = "$7-$8-$9 $10:$11:$12";
    print "$start&$end";
    exit(0);
}

# using this if there are no hours, mins, secs
elsif ($ARGV[0] =~ /.*s(\d{4})(\d{2})(\d{2})_e(\d{4})(\d{2})(\d{2}).*_c(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})*/)
{
	$start = "$1-$2-$3 00:00:00";
	$end= "$4-$5-$6 23:59:59";

	print "$start&$end";
	exit(0);
}

exit(1);
