#!/usr/bin/env perl
#
# Name: removeProductionRule.pl
#
# Author: Tom Feroli
#
# Input:
#       Database Schema
#       Production Rule ID
#
# Output:
#       Updated Oracle Database
#
# Process:
#       Connect
#       Make sure no more job specs will occur(set PRACTIVEFLAG=0)
#       Delete PRODUCTIONJOB
#	Delete JOBSPEC[INPUT, OUTPUT, PARAMETERS]
#       Delete PRODUCTIONJOBSPEC
#	Delete PR[INPUTSPEC, OUTPUTSPEC, PARAMETER]
#	Delete PRODUCTIONRULE
#
# revised: 20091006 lhf, add delete of rows in PRODUCTIONJOBLOGMESSAGES
#          20100318 lhf, reformat output
#          20110419 lhf, -s --> -m
#          20190122 jrh, changed database connection string and SQL queries to work in NDE in the Cloud (with Postgres)
#

use Getopt::Std;
getopt('m:i:');

##############################
# Check for comand line args #
##############################

if ( ! $opt_m || ! $opt_i ) {
        print "Usage $0 -m <mode> -i <Production Rule ID>\n";
        exit(1);
}

#####################
# Connect to Oracle #
#####################

use DBI;
if (-r "../common/ndeUtilities.pl") {
  require "../common/ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}
my $pw = promptForPassword("\n  Enter $opt_m password: ");
my $dbh = DBI->connect("dbi:Pg:dbname=nde_dev1;host=nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com", lc($opt_m), $pw) 
	or die "\nDatabase Connection Error: $DBI::errstr\n";

$dbh->{LongReadLen}=64000;

##############################
# Fetch Production Rule Name #
##############################

my $sql=qq{select PRRULENAME from PRODUCTIONRULE where PRID=$opt_i};
my $sth=$dbh->prepare($sql)
	or die "\nCan't prepare SQL statement: $DBI::errstr\n";
$sth->execute
	or die "\nCan't execute SQL statement: $DBI::errstr\n";
#my ($ruleName) = eval { $sth->fetchrow_array()};
my ($ruleName) = $sth->fetchrow_array();
$sth->finish;
if($ruleName eq ""){
	print "\n Invalid Prodution Rule!!!\n\n";
	exit(1);
}

###############################
# Prompt User Before Removing #
###############################

print "\n  This is the rule you've selected for removal:\n\t\t$ruleName\n\n\n";
print "  Type 'y' to verify: ";

chomp($bool=<>);

if ($bool eq "Y" || $bool eq "y" ) {

	########################
	# Remove Everything!!! #
	########################
	
	# Set PRACTIVEFLAG = 0

	my $sql=qq{update PRODUCTIONRULE set PRACTIVEFLAG_NSOF=0, PRACTIVEFLAG_CBU=0 where PRID=$opt_i};
        my $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

	# Delete PRODUCTIONJOBLOGMESSAGES

	$sql=qq{delete from PRODUCTIONJOBLOGMESSAGES where PRJOBID in (
     		select PRJOBID from PRODUCTIONJOB where PRODPARTIALJOBID in (
                 select PRODPARTIALJOBID from PRODUCTIONJOBSPEC where PRID=$opt_i))};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

        # Delete PRODUCTIONJOBOUTPUTFILES (added with ENTR-2574)

        $sql=qq{delete from PRODUCTIONJOBOUTPUTFILES where PRJOBID in (
     		select PRJOBID from PRODUCTIONJOB where PRODPARTIALJOBID in (
                 select PRODPARTIALJOBID from PRODUCTIONJOBSPEC where PRID=$opt_i))};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

        # Delete PRODUCTIONJOB

        $sql=qq{delete from PRODUCTIONJOB where PRODPARTIALJOBID in (
		select PRODPARTIALJOBID from PRODUCTIONJOBSPEC where PRID=$opt_i)};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

        # Delete JOBSPECINPUT 

        $sql=qq{delete from JOBSPECINPUT where PRODPARTIALJOBID in (
                select PRODPARTIALJOBID from PRODUCTIONJOBSPEC where PRID=$opt_i)};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

       # Delete JOBSPECOUTPUT

        $sql=qq{delete from JOBSPECOUTPUT where PRODPARTIALJOBID in (
                select PRODPARTIALJOBID from PRODUCTIONJOBSPEC where PRID=$opt_i)};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

       # Delete JOBSPECPARAMETERS

        $sql=qq{delete from JOBSPECPARAMETERS where PRODPARTIALJOBID in (
                select PRODPARTIALJOBID from PRODUCTIONJOBSPEC where PRID=$opt_i)};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

        # Delete PRODUCTIONJOBSPEC

        $sql=qq{delete from PRODUCTIONJOBSPEC where PRID=$opt_i};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

        # Delete PRINPUTPRODUCT

        $sql=qq{delete from PRINPUTPRODUCT where PRISID in (select PRISID from PRINPUTSPEC where PRID=$opt_i)};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

	# Delete PRINPUTSPEC

	$sql=qq{delete from PRINPUTSPEC where PRID=$opt_i};
	$sth=$dbh->prepare($sql)
	        or die "\nCan't prepare SQL statement: $DBI::errstr\n";
	$sth->execute
	        or die "\nCan't execute SQL statement: $DBI::errstr\n";

	# Delete PRPARAMETER

	$sql=qq{delete from PRPARAMETER where PRID=$opt_i};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

        # Delete PROUTPUTSPEC

	$sql=qq{delete from PROUTPUTSPEC where PRID=$opt_i};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

        # Delete PRODUCTIONRULE

        $sql=qq{delete from PRODUCTIONRULE where PRID=$opt_i};
        $sth=$dbh->prepare($sql)
                or die "\nCan't prepare SQL statement: $DBI::errstr\n";
        $sth->execute
                or die "\nCan't execute SQL statement: $DBI::errstr\n";

        print"\n  Rule removed, Execution complete.\n\n";
	$sth->finish;
        $dbh->disconnect();
}
else{
	print"\n\nProduction Rule removal aborted.\n\n";
        $dbh->disconnect();
}




exit(0);
