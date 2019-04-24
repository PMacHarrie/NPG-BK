#!/usr/bin/env perl

#
# name: showJob.pl
# purpose: Used by Test Engineers to view job database
# revised: 20090511 lhf creation
#          20090514 lhf, additional updates from Thursday...
#          20090522 lhf, add some stuff
#          20090902 lhf, fix for Proto4
#          20100219 lhf, cleanup output
#

use DBI;
use Getopt::Std;
getopt('m:');

if ( ! $opt_m ) {
	print "Usage $0 -m <mode>\n";
	exit(1);
}
if (-r "../common/ndeUtilities.pl") {
  require "../common/ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}
my $pw = promptForPassword("\n  Enter $opt_m password: ");
my $dbh = DBI->connect("dbi:Oracle:$ENV{ORACLE_SID}", "$opt_m", $pw) ;
my $count = 0;
my %statuses = ( 'Queued' => 0,
                 'Assigned' => 0,
                 'CopyInput' => 0,
                 'Running' => 0,
                 'CopyOutput' => 0,
                 'Complete' => 0, 
               );
my %nodes = ( 'n10rscitool' => 0,
              'n01asystest' => 0,
              'n04asadie'   => 0,
              'n03ait'      => 0,
            );

print "\n$0 Execution started: ", `date`;
print "\n\nListing the ProductionJob table...\n\n";

my $sth = $dbh->prepare("select pj.prjobid,pjs.prodpartialjobid,prrulename,
            productfilenameprefix,prjobstatus,prjobenqueuetime,processingnodename,pjs.prid
             from productionjob pj,productionjobspec pjs,productionrule pr,productdescription pd,
              prinputspec pis, processingnode pn, prinputproduct pip
               where pj.prodpartialjobid=pjs.prodpartialjobid
                and pjs.prid=pr.prid
                 and pr.prid=pis.prid and pis.PRISID=pip.PRISID
                  and pip.productid=pd.productid
                   and pis.prisneed='TRIGGER'
                    and pj.processingnodeid  = pn.processingnodeid (+)
                     order by pj.prjobid");
$sth->execute;
printf "%8s  %8s  %-46s %-14s %-13s  %-10s\n","JOBID","Partial","PRID/RuleName","Status","NodeName","Enqueue Time";
printf "%8s  %8s  %-46s %-14s %-13s  %-10s\n","-----","-------","-------------","------","--------","------------";
while ( my ($pji,$ppji,$prn,$pfp,$pjs,$pjet,$pnn,$prid) = $sth->fetchrow_array()) {
  my ( $node, $null, $null ) = split(/\./,$pnn);
  printf "%7s  %7s    %-46s %-14s %-14s %-13s\n",$pji,$ppji,$prid . '/' . $prn,$pjs,$node,$pjet;
  $count += 1;
  $statuses{$pjs} += 1;
  $nodes{$node} += 1;
}
##print "\n\nJob Status Breakdown:\n";
##print "---------------------\n";
##while ( my ( $key, $value ) = each (%statuses) ) {
##  printf "%-22s %8d\n",$key,$value;
##}
##print "\n\nNode Assignments:\n";
##print "-----------------\n";
##while ( my ( $key, $value ) = each (%nodes) ) {
##  my $nodename = $key; 
##  $nodename = 'Unassigned', if ( ! $key ); 
##  printf "%-22s %8d\n",$nodename,$value;
##}
##print "\nNumber of Jobs Shown: $count\n";
#print "\n  Enter JOBID to view: ";
#$jobid_picked = <STDIN>;
#chomp($jobid_picked);
#print "Picked: $jobid_picked\n";
