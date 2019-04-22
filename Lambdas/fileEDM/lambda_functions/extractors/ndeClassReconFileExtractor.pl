#!/usr/bin/env perl

#
# name: ndeClassReconFileExtractor.pl
# revised: 20120208 lhf, creation (NDE-462)
#
# this extracts metadata out of filenames like:
#     NDE.CLASS.RECON.FILE.D2012039.T092237
# puts it in the form:
#     yyyy-mm-dd hh:mm:ss&yyyy-mmdd hh:mm:ss
#     ^^--begin time---^^ ^^---end time---^^

use Time::Local;
use POSIX;

# file datetime is a file generation time, must assume coverage is the previous 24 hours
if ($ARGV[0] =~ /.*.D(\d{4})(\d{3}).T(\d{2})(\d{2})(\d{2})/)
{
  my $ymd = strftime "%Y-%m-%d", localtime mktime(0,0,0,$2-1,0,$1-1900);
  my $end = "$ymd $3:$4:$5";
  my $start = "$ymd $3:$4:$5";
  my $ymd = strftime "%Y-%m-%d", localtime mktime(0,0,0,$2+0,0,$1-1900);
  my $end = "$ymd $3:$4:$5";
  print "$start&$end";
}
