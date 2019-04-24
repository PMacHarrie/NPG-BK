#!/usr/bin/env perl

#
# name: removeEnterpriseMeasures.pl
#
# revised:  20110809 htp, creation
#			20120726 dcp, removed check on whether h5 or nc4 for deleting xref table. not needed and causes errors
#							when measurenames are the same for nc4 and hdf5 arrays
#			20140409 dcp, refactored to log properly
#

use DBI;
use Data::Dumper;

use Getopt::Std;
getopt('m:');


# Check for comand line args
if ( ! $opt_m ) {
        print "Usage $0 -m <mode> <[productID 1] [productID 2] ... [productID n] OR [all]>\n";
        exit(1);
}

open SOURCE, "bash -c '. ~/.set_NDE_ENV_vars $opt_m >& /dev/null; env'|" or die "Can't fork: $!";
while(<SOURCE>){
  if (/^(SAN_HOME)=(.*)/){
    $ENV{$1} = ${2};
  }
}

chomp( $YMDHMS = `date +%Y%m%d_%H%M%S` );
my $rfn = reverse( $opt_f );
if ( index(reverse($opt_f),"/") > 0) {
  $rfn = substr(reverse($opt_f),0,index(reverse($opt_f),"/"));
}
my $logfile = "/opt/data/nde/" . $opt_m . "/logs/pds/removeEnterpriseMeasures_" . reverse($rfn) . "_" . $YMDHMS . ".log";

open ( LOG, ">$logfile" ) or
  die "ERROR Error opening logfile $logfile, exiting...\n";

logger(  "INFO Execution started..." );

my @prodIDs = @ARGV;

if (-r "../common/ndeUtilities.pl") {
  require "../common/ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}
my $pw = promptForPassword("\n  Enter $opt_m password: ");
my $dbh = DBI->connect("dbi:Pg:dbname=nde_dev1;host=nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com", lc($opt_m), $pw) ;
$dbh->{LongReadLen}=64000;
# start a transaction
$dbh->begin_work;

#Process all loaded profiles for HDF5 and NC4
if ($prodIDs[0] =~ m/all/){
	my $sth = $dbh->prepare("select unique PRODUCTID from HDF5_GROUP");
	$sth->execute;
	my $i=0;
	while (my $id = $sth->fetchrow_array()) {
		$prodIDs[$i] = $id;
		$i++;		
	}
	$sth = $dbh->prepare("select unique PRODUCTID from NC4_GROUP");
	$sth->execute;
	while (my $id = $sth->fetchrow_array()) {
		$prodIDs[$i] = $id;
		$i++;		
	}
}

my %products = ();

#Lookup type and product short name
logger( "INFO  Looking up product shortnames and extensions." );
if( @prodIDs ){
	foreach my $productID ( @prodIDs ){
		#print $productID . "\n";
		my $sth = $dbh->prepare("select PRODUCTSHORTNAME, PRODUCTFILENAMEEXTENSION 
									from PRODUCTDESCRIPTION where PRODUCTID = $productID");
		$sth->execute;
		my ($psn, $pfne) = $sth->fetchrow_array();
		if ( $pfne eq "h5" || $pfne eq "nc" ){
			$products{$psn} = $pfne;
			logger( "INFO    $psn (Product ID: $productID, $pfne file)" );
		}
		elsif ( $pfne eq "" ){
			logger( "ERROR  Product filename extension undefined or invalid product ID ($productID) entered. Exiting."); exit 1;
		}
		else {
			logger( "ERROR  Product filename extension \"$pfne\" for product ID: $productID is not supported (nc or h5 only). Exiting." ); exit 1;
			
		}
	}
}


#Remove measure/dimensions for product by product
#print Dumper(%products);
for my $psn ( keys %products ){
	my $sth_main = runSQL($dbh, "select MEASUREID, MEASURENAME, em.E_DIMENSIONLISTID, E_DIMENSIONLISTNAME 
									from ENTERPRISEMEASURE em, ENTERPRISEDIMENSIONLIST edl 
										where em.E_DIMENSIONLISTID = edl.E_DIMENSIONLISTID and MEASURENAME like '\%\@$psn'");
	
	while ( my ($measureID, $measureName, $dimListID, $dimListName) = $sth_main->fetchrow_array() ){
		
		logger( "INFO    	Removing measure: $measureName" );
		my $rowsAffected = 0;
		
		
		#Delete from XREF table
#		if( $products{$psn} eq "h5" ){
			$rowsAffected = $dbh->do("delete from MEASURE_H_ARRAY_XREF where MEASUREID = $measureID");
			if ($rowsAffected >= 1) { logger( "INFO    	  Deleting $rowsAffected row from 'MEASURE_H_ARRAY_XREF' table" );}
			
#		}
#		elsif( $products{$psn} eq "nc" ){
			$rowsAffected = $dbh->do("delete from MEASURE_N_ARRAY_XREF where MEASUREID = $measureID");
			if ($rowsAffected >= 1) {logger( "INFO    	  Deleting $rowsAffected row from 'MEASURE_N_ARRAY_XREF' table" );}
#		}
		
		#Delete from Enterprise measure
		$rowsAffected = $dbh->do("delete from ENTERPRISEMEASURE where MEASUREID = $measureID");
		logger( "INFO    	  Deleting $rowsAffected row from 'ENTERPRISE_MEASURE' table" );
		
		
		#Check DimensionList/Group for relation to other measures, remove if not
		logger( "INFO    	Removing dimension group: $dimListName" );
		
		my $sth_dimListCount = runSQL($dbh, "select count(*) from ENTERPRISEMEASURE where E_DIMENSIONLISTID = $dimListID");
		my $dimListCount = $sth_dimListCount->fetchrow_array();
		
		if( $dimListCount == 0 ){
			
			#Select dimension IDs to check later, delete from ordered dimensions and dimension list
			my $sth_dim = runSQL($dbh, "select E_DIMENSIONID from ENTERPRISEORDEREDDIMENSION where E_DIMENSIONLISTID = $dimListID");
		
			$rowsAffected = $dbh->do("delete from ENTERPRISEORDEREDDIMENSION where E_DIMENSIONLISTID = $dimListID");
			logger( "INFO    	  Deleting $rowsAffected rows from 'ENTERPRISEORDEREDDIMENSION' table" );
						
			$rowsAffected = $dbh->do("delete from ENTERPRISEDIMENSIONLIST where E_DIMENSIONLISTID = $dimListID");
			logger( "INFO    	  Deleting $rowsAffected rows from 'ENTERPRISEDIMENSIONLIST' table" );	
			
			#Check dimension to see if used in another list/group, remove if not 
			my $totalDimDelCount = 0;
			while ( my $dimID = $sth_dim->fetchrow_array() ){
				my $sth_dimCount = runSQL($dbh, "select count(*) from ENTERPRISEORDEREDDIMENSION where E_DIMENSIONID = $dimID");
				my $dimCount = $sth_dimCount->fetchrow_array();
				if( $dimCount == 0 ){
					$rowsAffected = $dbh->do("delete from ENTERPRISEDIMENSION where E_DIMENSIONID = $dimID");
					logger( "INFO    	  Deleting $rowsAffected rows from 'ENTERPRISEDIMENSION' table" );	
				}
				else{
					logger( "INFO    	  Not deleting from ENTERPRISEDIMENSION, $dimCount other dimension groups are still linked" );
				}
			}
		}
		else{
			logger( "INFO    	  Not deleting from ENTERPRISEDIMENSIONLIST, $dimListCount other measures are still linked" );
		}
		
	}
	
}


$dbh->commit();

logger ("INFO Removal of enterprise measures has been successful." );
logger( "Execution Completed.");

sub logger {
  my ( $msg ) = @_;
  print "$msg\n";
  chomp( $HMS = `date +%H:%M:%S,%N` );
  print LOG substr($HMS,0,12) . " $msg\n";
}

exit 0;



