#!/usr/bin/env perl

#
# name: registerGazetteer.pl
# revised: 20111114 dcp, created 
#          20130131 teh, require ndeUtiltiles.pl for pwd prompt
#		   20150218 dcp, added update capability for updating geo-spatial coordinates
#
# Input:
#	Database Name
#       Gazetteer Definition XML File
# Output:
#       Updated Oracle Database
#

use XML::Simple;
use Data::Dumper;
use Getopt::Std;
getopt('m:f:');


# Check for comand line args
if ( ! $opt_m || ! $opt_f ) {
	print "\nUsage $0 -m <mode> -f <XML Filename>\n\n";
	exit(1);
}

chomp( $YMDHMS = `date +%Y%m%d_%H%M%S` );
my $rfn = reverse( $opt_f );
if ( index(reverse($opt_f),"/") > 0) {
  $rfn = substr(reverse($opt_f),0,index(reverse($opt_f),"/"));
}
my $logfile = "/opt/data/nde/" . $opt_m . "/logs/common/GAZETTEER_" . reverse($rfn) . "_" . $YMDHMS . ".log";

open ( LOG, ">$logfile" ) or
  die "ERROR Error opening logfile $logfile, exiting...\n";

logger(  "INFO Execution started..." );

# Connect to Oracle
if (-r "./ndeUtilities.pl") {
  require "./ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}
my $pw = promptForPassword("\n  Enter $opt_m password: ");
use DBI;
my $dbh = DBI->connect("dbi:Pg:dbname=nde_dev1;host=nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com",lc($opt_m),$pw)
        or die "\nDatabase Connection Error: $DBI::errstr\n";
$dbh->{LongReadLen}=64000;
$dbh->begin_work;


my %ref;
#my $ref = XMLin($opt_f, forcearray => [ 'Gazetteer' ]);

my $ref = XMLin($opt_f);

# Gazatteer 
my $gazName = $ref->{gzFeatureName};
logger( "INFO Checking existing gz features for duplicate...");
my $sth = runSQL( $dbh, "select GZID from GAZETTEER where GZFEATURENAME = '$gazName'");
my $gazID = $sth->fetchrow_array();

if ( !defined($gazID) ) {
	if( !defined($gazName) ){
		logger( "ERROR Gazetteer feature name must be defined");
		$dbh->rollback(); exit 1;
	}
	$sth = runSQL( $dbh, "select nextval('s_gazetteer')" );
	$gazID = $sth->fetchrow_array();
	
	logger( "INFO Adding gazetteer... ");
	# Insert GAZETTEER
	logger( "$ref->{gzFeatureName}");
	$sql = " insert into GAZETTEER (GZID, GZFEATURENAME, GZLOCATIONSPATIAL, GZSOURCETYPE, GZLOCATIONELEVATIONMETERS, GZDESIGNATION)
	                  values($gazID, '$ref->{gzFeatureName}', $ref->{gzLocationSpatial}, 
	                   '$ref->{gzSourceType}', $ref->{gzLocationElevationMeters}, 
	                    '$ref->{gzDesignation}' ) ";
	print "$sql\n";
	my $success += $dbh->do( $sql );
	if( $success == 1 ){
		logger( "INFO Complete, ID: $gazID");
	}
	else{
		logger( "ERROR There was a problem, please check gazetteer definition.");
		$dbh->rollback(); exit 1;
	}
}
else {
	my $update="n";
	logger( "WARN Gazatteer \"$gazName\" already exists (ID: $gazID), continue with update (y/n)?");
	chomp($update = <STDIN>);
	if (($update ne 'y') && ($update ne 'n')) {
		logger( "ERROR  You did not enter a \"y\" or a \"n\". Exiting...");
		$dbh->rollback();
		exit 1;
	} elsif ($update eq "n") {
		logger( "INFO  Not updating duplicate rule $ref->{prRuleName} and exiting...");
		$dbh->rollback();
		exit 1;
	}
	logger("INFO Updating gazetter geo-spatial coordinates");
	$sql = "update GAZETTEER set GZLOCATIONSPATIAL = $ref->{gzLocationSpatial} where GZID = $gazID";
	my $success += $dbh->do($sql);
	if( $success == 1 ){
		logger( "INFO Update complete, ID: $gazID");
	}
	else{
		logger( "ERROR There was a problem, please check gazetteer definition.");
		$dbh->rollback(); exit 1;
	}
}


$dbh->commit();

logger( "INFO Gazetteer \"$gazName \" has been successfully added (ID: $gazID)");
logger( "INFO Execution completed.");

#in ndeUtilties.pl
#sub runSQL {
# my ($dbh,$sql) = @_;
# my $sth = $dbh->prepare($sql);
# $sth->execute;
# return $sth;
#}

sub logger {
  my ( $msg ) = @_;
  print "$msg\n";
  chomp( $HMS = `date +%H:%M:%S,%N` );
  print LOG substr($HMS,0,12) . " $msg\n";
}

exit 0;
