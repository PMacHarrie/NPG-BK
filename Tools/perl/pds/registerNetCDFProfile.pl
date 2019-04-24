#!/usr/bin/env perl

#
# name: registerNetCDFProfile.pl
#
# revised: 20110729 htp, creation
#		   20120521 dcp, updated to reflect NDE standard file convention (DAP V1.3) and some refactoring
#		   20130321 dcp, updated nc4_dimensionlist insert to uniquely insert dimensionid (CR NDE-852)
#		   20130425 dcp, added registration of dimension max size into nc4 tables
#		   20140401 dcp, added capability to update profiles and improved error handling and logging
#		   20140721 dcp, added capability to insert bit mask information into NC4_ARRAYATTRIBUTE table
#		   20141002 dcp, updates to populate nc4 attribute table data types
#		   20141013 dcp, added platform mapping to product IDs
#

use XML::Simple;
use Data::Dumper;

use Getopt::Std;
getopt('m:f:');


# Check for comand line args

if ( ! $opt_m || ! $opt_f ) {
        print "Usage $0 -m <mode> -f <XML Filename>\n";
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
my $logfile = "/opt/data/nde/" . $opt_m . "/logs/pds/PRODUCTPROFILE_NDE_" . reverse($rfn) . "_" . $YMDHMS . ".log";

open ( LOG, ">$logfile" ) or
  die "ERROR Error opening logfile $logfile, exiting...\n";

logger("------");
logger(  "INFO Execution started..." );

#Connect to Oracle
use DBI;
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

my $sth;
my $sql;

my %ref;
my $ref = XMLin($opt_f, ForceArray => 1);

#print Dumper($ref);


# look up productid 
logger( "INFO  Checking for existing product." );
$st=runSQL($dbh,"select PRODUCTID from PRODUCTDESCRIPTION where PRODUCTSHORTNAME='$ref->{attribute}->{title}->{value}'");

#Loop through productIDs with same shortname (multiple platforms)
my $i=0;
while ( my $productID = $st->fetchrow_array() ) {
	
	logger("INFO  Profile for product \"$ref->{attribute}->{title}->{value}\" (ID: $productID)");
	
	# Check if update or new product profile
	$sth=runSQL($dbh,"select N_GROUPID from NC4_GROUP where PRODUCTID='$productID' and N_GROUPNAME='/'");
	my $rootGroupID = $sth->fetchrow_array();
	my $update="n"; #initialize the update flag
	if ( !defined($rootGroupID) ){
		logger( "INFO  No existing profile. Adding now." );
	} else {
		logger( "WARN  Profile exists, continue with update (y/n)?" );
		chomp($update = <STDIN>);
		if (($update ne 'y') && ($update ne 'n')) {
			logger( "ERROR  You did not enter a \"y\" or a \"n\". Exiting...");
			$dbh->rollback();
			exit 1;
		} elsif ($update eq "n") {
			logger( "INFO  Not updating existing profile and exiting.");
			$dbh->rollback();
			exit 1;
		}
	}
	
	
	if ($update eq 'n') {  #Add new profile
	#Insert into root group if new profile
		my $success = $dbh->do( "insert into NC4_GROUP (N_GROUPID, NC4_N_GROUPID, PRODUCTID, N_GROUPNAME) values (nextval('S_NC4_GROUP'), null, $productID, '/')");
		if ($success != 1) {
			logger( "ERROR  Insert in NC4_GROUP table failed. Exiting..." );
			$dbh->rollback();
			exit 1;
		}
		$sth=runSQL($dbh, "select N_GROUPID from NC4_GROUP where PRODUCTID=$productID and N_GROUPNAME='/'");
		$rootGroupID = $sth->fetchrow_array();
		if (!defined($rootGroupID) ){
			logger( "ERROR  No N_GROUPID retrieved. Insert failed? Exiting..." );
			$dbh->rollback();
			exit 1;
		}
		logger( "INFO   NC4 GROUP ID: $rootGroupID");
		
	#do group attributes (NC4_GROUPATTRIBUTE)	
		logger ("INFO   Adding group attributes.");
		for my $groupAttributeName ( keys %{$ref->{attribute}} ) {
			my $groupAttributeValue = $ref->{'attribute'}{$groupAttributeName}{'value'};
			my $groupAttributeType = $ref->{'attribute'}{$groupAttributeName}{'type'};
			if (!defined($groupAttributeType)) {$groupAttributeType='string';}
			my $groupNumAttrValues = 1;
			if( $groupAttributeType ne "string" ){
					my @attributeValues = split(" ",$groupAttributeValue);
					$groupNumAttrValues = @attributeValues;
			}
			
			logger ("INFO    	$groupAttributeName: $groupAttributeValue");
	
			$success=$dbh->do("insert into NC4_GROUPATTRIBUTE (N_GROUPATTRIBUTEID, N_GROUPID, N_GROUPATTRIBUTENAME, N_GROUPATTRIBUTEDATATYPE, 
								   N_GROUPATTRIBUTEEXTERNALFLAG, N_GROUPATTRIBUTESTRINGVALUE, N_GROUPNUMATTRVALUES) 
									values (nextval('S_NC4_DIMENSION'), $rootGroupID, '$groupAttributeName', '$groupAttributeType', 0, '$groupAttributeValue', '$groupNumAttrValues')");
			if ($success != 1) {
				logger( "ERROR  Insert in NC4_GROUPATTRIBUTE table failed. Check Product Profile. Exiting..." );
				$dbh->rollback();
				exit 1;
			}
		}
		
	#Add dimensions (NC4_DIMENSION)
		logger ("INFO   Adding dimensions for arrays.");
		my @dimNames;
	#	my $i=0;
	#	my $i_dimChange; #if there is a change to the dimension name then this will capture which dim
		for $dimName ( sort keys %{$ref->{dimension}} ) {
			my $dimSize = $ref->{'dimension'}{$dimName}{'length'};
			my $dimMaxSize = $ref->{'dimension'}{$dimName}{'maxLength'};
			
			if (!defined($dimMaxSize) ){$dimMaxSize=$dimSize;}
			
			logger( "INFO    	$dimName: $dimSize | Max size: $dimMaxSize" );
			$success = $dbh->do("insert into NC4_DIMENSION (N_DIMENSIONID, N_GROUPID, N_DIMENSIONNAME, N_DIMENSIONSIZE, N_DIMENSIONMAXIMUMSIZE) 
						values (nextval('S_NC4_DIMENSION'), $rootGroupID, '$dimName', $dimSize, $dimMaxSize)");
			if ($success != 1) {
				logger( "ERROR  Insert in NC4_DIMENSION table failed. Check Product Profile. Exiting..." );
				$dbh->rollback();
				exit 1;
			}
			
	#	    $dimNames[$i] = $dimName;
	#	    $i++;
		}
		
	#Add arrays and dimensionlists
	 #do arrays
		logger( "INFO   Adding arrays, dimension lists, and array attributes.");
		for my $arrayName ( sort keys %{$ref->{variable}} ) {
			
			my $separator = " ";
			
			my $arrayType = $ref->{'variable'}{$arrayName}{'type'};
			
			logger( "INFO    	$arrayName: $arrayType");
			$success = $dbh->do( "insert into NC4_ARRAY (N_ARRAYID, N_GROUPID, N_ARRAYNAME, N_DATATYPE) 
						values (nextval('S_NC4_DIMENSION'), $rootGroupID, '$arrayName', '$arrayType')");
			if ($success != 1) {
				logger( "ERROR  Insert in NC4_ARRAY table failed. Check Product Profile. Exiting..." );
				$dbh->rollback();
				exit 1;
			}
			
			$sth=runSQL($dbh, "select N_ARRAYID from NC4_ARRAY where N_ARRAYNAME='$arrayName' and N_GROUPID=$rootGroupID");
			my $arrayID = $sth->fetchrow_array();
			
	 #do dimension list
			my $dimList = $ref->{'variable'}{$arrayName}{'shape'};
			logger( "INFO    	  Dimension list: $dimList" );
			my @dims = split($separator,$dimList);
			my $dimOrder = 1;
		        my $i=0;
			for my $dim (@dims){
		#		if ( $dim !~ @dimNames[$i]) {        
		#                	$dim = @dimNames[$i];   #dcp - not sure why this is here??
		#        }
		        my $dimDataPartition = 1; #The granule boundary is always the first dimension (along track) unless GranuleB is defined (to force concatenation along this dimension)
				
				my $checkDimGranuleB = join("|",@dims);
				if ($checkDimGranuleB =~ m/GranuleB/ and $dim !~ m/GranuleB/) {$dimDataPartition=0;}
				if ($i > 0 and $dim !~ m/GranuleB/) {$dimDataPartition=0;}
				
				$sth=runSQL($dbh, "select N_DIMENSIONID from NC4_DIMENSION nd, NC4_GROUP ng 
									where N_DIMENSIONNAME='$dim' and ng.n_groupid=$rootGroupID and nd.n_groupid=ng.n_groupid and ng.productid=$productID");
				my $dimID = $sth->fetchrow_array();
				if ( !defined($dimID) ){ 
					logger( "ERROR  Could not find dimension: \"$dim\". Exiting." ); 
					$dbh->rollback();
					exit 1; 
				}
				$success = $dbh->do("insert into NC4_DIMENSIONLIST (N_ARRAYID, N_DIMENSIONORDER, N_DIMENSIONID, N_DATAPARTITION) 
						values ($arrayID, $dimOrder, $dimID, $dimDataPartition)");
				if ($success != 1) {
					logger( "ERROR  Insert in NC4_DIMENSIONLIST table failed. Check Product Profile. Exiting..." );
					$dbh->rollback();
					exit 1;
				}
				$dimOrder ++;
		        $i++;
			}
		        #print "\t DimList: @dimNames\n" ;
			
	 #do array attributes
		    for my $arrayAttributeName ( sort keys %{$ref->{variable}{$arrayName}{'attribute'}} ) {
		
				my $arrayAttributeType = $ref->{'variable'}{$arrayName}{'attribute'}{$arrayAttributeName}{'type'};
				my $arrayAttributeValue = $ref->{'variable'}{$arrayName}{'attribute'}{$arrayAttributeName}{'value'};
				my $arrayAttributeDelimiter = "";
				my $arrayNumAttrValues = 0;
	
				if( !defined($arrayAttributeValue) || $arrayAttributeValue eq "" ){ logger( "WARN:  No attribute value found for attribute \"$arrayAttributeName\"" );}
				else{ $arrayNumAttrValues = 1;}
				
				#set type as string if type is missing
				if( !defined($arrayAttributeType) ){
					$arrayAttributeType = "string";
				}
				
				if( $arrayAttributeType ne "string" && $arrayNumAttrValues == 1 ){
					$arrayAttributeDelimiter = " ";
					my @attributeValues = split($separator,$arrayAttributeValue);
					$arrayNumAttrValues = @attributeValues;
				}
				
				logger("INFO    	  $arrayAttributeName (Att ID: $arrayID): \"$arrayAttributeValue\"");
				$success = $dbh->do( "insert into NC4_ARRAYATTRIBUTE
						values (nextval('S_NC4_ARRAYATTRIBUTE'), $arrayID, '$arrayAttributeName', '$arrayAttributeType', 0,
								'$arrayAttributeDelimiter', '$arrayAttributeValue', $arrayNumAttrValues)" );
				if ($success != 1) {
					logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Check Product Profile. Exiting." );
					$dbh->rollback();
					exit 1;
				}
				if ($arrayAttributeName eq 'transform' && $arrayAttributeValue eq 'bit') { #bit mask array
					my $i=1;
					for my $datumName ( sort keys %{$ref->{variable}{$arrayName}{'datum'}} ) {
						my $bit_longName=$datumName;
						my $bit_offset=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'offset'};
						my $bit_length=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'length'};
						my $bit_values=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'values'};
						my $bit_meanings=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'meanings'};
						my $maskName=$arrayName . "_flag" . $i;
						logger("INFO    	  $maskName:");
						logger("INFO    	    Bit offset: $bit_offset");
			          	$success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
			                                  '$maskName\_bit_offset','short',1,null,'$bit_offset',1)");
			          	if ($success != 1) {
							logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  	} 
				  	    logger("INFO    	    Bit length: $bit_length");
				  	    $success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
			                                  '$maskName\_bit_length','short',1,null,'$bit_length',1)");
			          	if ($success != 1) {
							logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  	}
				  	    logger("INFO    	    Bit long name: $bit_longName");
				  	    $success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
			                                  '$maskName\_bit_long_name','string',1,null,'$bit_longName',1)");
			          	if ($success != 1) {
							logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  	}
				  	  	my @numAttr = split(/,/,$bit_values);
				  	  	my $numAttr = @numAttr;
				  	    logger("INFO    	    Bit values: $bit_values");
				  	    $success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
			                                  '$maskName\_bit_values','ubyte',1,',','$bit_values',$numAttr)");
			          	if ($success != 1) {
							logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  	}
				  	    logger("INFO    	    Bit meanings: $bit_meanings");
				  	  	$success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
			                                  '$maskName\_bit_meanings','string',1,',','$bit_meanings',$numAttr)");
			          	if ($success != 1) {
							logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  	}
						$i=$i+1;
					}
				}
		    }
		
		}	
	
	} else { 			#Update the profile
	#Update group attributes (NC4_GROUPATTRIBUTE)
	    logger( "INFO   NC4 GROUP ID: $rootGroupID");	
		logger ("INFO   Updating NC4 group attributes.");
		for my $groupAttributeName ( keys %{$ref->{attribute}} ) {
			my $groupAttributeValue = $ref->{'attribute'}{$groupAttributeName}{'value'};
			my $sth=runSQL($dbh,"select N_GROUPATTRIBUTEID from NC4_GROUPATTRIBUTE where N_GROUPID=$rootGroupID and N_GROUPATTRIBUTENAME='$groupAttributeName'");
			my $n_groupattid=$sth->fetchrow_array();
			
			my $groupAttributeType = $ref->{'attribute'}{$groupAttributeName}{'type'};
			if (!defined($groupAttributeType)) {$groupAttributeType='string';}
			my $groupNumAttrValues = 1;
			if( $groupAttributeType ne "string" ){
					my @attributeValues = split(" ",$groupAttributeValue);
					$groupNumAttrValues = @attributeValues;
			}
			
			if(!defined($n_groupattid) ) { #add new attribute
				my $success=$dbh->do("insert into NC4_GROUPATTRIBUTE (N_GROUPATTRIBUTEID, N_GROUPID, N_GROUPATTRIBUTENAME, N_GROUPATTRIBUTEDATATYPE, 
								   N_GROUPATTRIBUTEEXTERNALFLAG, N_GROUPATTRIBUTESTRINGVALUE, N_GROUPNUMATTRVALUES) 
									values (nextval('S_NC4_DIMENSION'), $rootGroupID, '$groupAttributeName', 'string', 0, '$groupAttributeValue', '$groupNumAttrValues')");
				if ($success != 1) {
					logger( "ERROR  Insert in NC4_GROUPATTRIBUTE table failed. Check Product Profile. Exiting." );
					$dbh->rollback();
					exit 1;
				}
				logger("INFO    	Adding new attribute: $groupAttributeName: $groupAttributeValue");
			} else { #update existing attributes values (content)
				my $success=$dbh->do("update NC4_GROUPATTRIBUTE set N_GROUPATTRIBUTESTRINGVALUE='$groupAttributeValue', N_GROUPNUMATTRVALUES='$groupNumAttrValues' 
										where N_GROUPATTRIBUTEID=$n_groupattid");
				if ($success != 1) {
					logger( "ERROR  Update of NC4_GROUPATTRIBUTE table failed for N_GROUPATTRIBUTEID: $n_groupattid. Exiting..." );
					$dbh->rollback();
					exit 1;
				}
				logger ("INFO    	$groupAttributeName: $groupAttributeValue");
			}
		}
	
	#Update dimensions (NC4_DIMENSION)
		logger ("INFO   Updating dimensions for arrays.");
		my @dimNames;
	#	my $i=0;
	#	my $i_dimChange; #if there is a change to the dimension name then this will capture which dim
		for $dimName ( sort keys %{$ref->{dimension}} ) {
			my $dimSize = $ref->{'dimension'}{$dimName}{'length'};
			my $dimMaxSize = $ref->{'dimension'}{$dimName}{'maxLength'};
			
			if (!defined($dimMaxSize) ){$dimMaxSize=$dimSize;}
	
			$sth=runSQL($dbh,"select N_DIMENSIONID from NC4_DIMENSION ND, NC4_GROUP NG where NG.N_GROUPID=$rootGroupID and NG.PRODUCTID=$productID
								and NG.N_GROUPID=ND.N_GROUPID and N_DIMENSIONNAME='$dimName'");
			my $n_dimensionID=$sth->fetchrow_array();
			
			if (!defined($n_dimensionID) ){ #Add a new dimension to the nc4 group
			    logger( "INFO    	Adding new dimension $dimName: $dimSize" );
				$success = $dbh->do("insert into NC4_DIMENSION (N_DIMENSIONID, N_GROUPID, N_DIMENSIONNAME, N_DIMENSIONSIZE, N_DIMENSIONMAXIMUMSIZE) 
						values (nextval('S_NC4_DIMENSION'), $rootGroupID, '$dimName', $dimSize, $dimMaxSize)");
				if ($success != 1) {
					logger( "ERROR  Insert in NC4_DIMENSION table failed. Check Product Profile. Exiting..." );
					$dbh->rollback();
					exit 1;
				}
			} else { #update dimension sizes
				logger( "INFO    	$dimName: $dimSize (max size: $dimMaxSize)" );
				my $success=$dbh->do("update NC4_DIMENSION set N_DIMENSIONSIZE=$dimSize, N_DIMENSIONMAXIMUMSIZE=$dimMaxSize
										where N_DIMENSIONID=$n_dimensionID and N_GROUPID=$rootGroupID");
				if ($success != 1) {
					logger( "ERROR  Update in NC4_DIMENSION table failed for N_DIMENSIONID: $n_dimensionID. Exiting..." );
					$dbh->rollback();
					exit 1;
				}
			}		
	#	    $dimNames[$i] = $dimName;
	#	    $i++;
		}
	#Update arrays and dimensionlists
	 #do arrays
		logger( "INFO   Updating arrays and dimension lists:");
		for my $arrayName ( sort keys %{$ref->{variable}} ) {
			
			my $separator = " ";
			
			my $arrayType = $ref->{'variable'}{$arrayName}{'type'};
			
			$sth=runSQL($dbh,"select N_ARRAYID from NC4_ARRAY where N_ARRAYNAME='$arrayName' and N_GROUPID=$rootGroupID");
			my $arrayID = $sth->fetchrow_array();
			
			if (!defined($arrayID) ){ #new array
			    logger( "INFO    	Adding $arrayName: $arrayType");
				$success = $dbh->do( "insert into NC4_ARRAY (N_ARRAYID, N_GROUPID, N_ARRAYNAME, N_DATATYPE) 
							values (nextval('S_NC4_DIMENSION'), $rootGroupID, '$arrayName', '$arrayType')");
				if ($success != 1) {
					logger( "ERROR  Insert in NC4_ARRAY table failed. Check Product Profile. Exiting..." );
					$dbh->rollback();
					exit 1;
				}
				$sth=runSQL($dbh,"select N_ARRAYID from NC4_ARRAY where N_ARRAYNAME='$arrayName' and N_GROUPID=$rootGroupID");
			    $arrayID = $sth->fetchrow_array();
			} else { #update array type
				logger( "INFO    	$arrayName (Array ID: $arrayID): $arrayType");
				$success = $dbh->do( "update NC4_ARRAY set N_DATATYPE='$arrayType' where N_ARRAYID=$arrayID");
				if ($success != 1) {
					logger( "ERROR  Update to NC4_ARRAY table failed. Check Product Profile. Exiting..." );
					$dbh->rollback();
					exit 1;
				}
			}
			
	 #do dimension list
			my $dimList = $ref->{'variable'}{$arrayName}{'shape'};
			logger( "INFO    	  Dimension List: $dimList" );
			my @dims = split($separator,$dimList);
			my $dimOrder = 1;
		        my $i=0;
			for my $dim (@dims){
		#		if ( $dim !~ @dimNames[$i]) {        
		#                	$dim = @dimNames[$i];   #dcp - not sure why this is here??
		#        }
		        my $dimDataPartition = 1; #The granule boundary is always the first dimension (along track) unless GranuleB is defined (to force concatenation along this dimension)
				
				my $checkDimGranuleB = join("|",@dims);
				if ($checkDimGranuleB =~ m/GranuleB/ and $dim !~ m/GranuleB/) {$dimDataPartition=0;}
				if ($i > 0 and $dim !~ m/GranuleB/) {$dimDataPartition=0;}
				
				$sth=runSQL($dbh, "select N_DIMENSIONID from NC4_DIMENSION nd, NC4_GROUP ng 
									where N_DIMENSIONNAME='$dim' and ng.n_groupid=$rootGroupID and nd.n_groupid=ng.n_groupid and ng.productid=$productID");
				my $dimID = $sth->fetchrow_array();
				
				if ( !defined($dimID) ){ 
					logger( "ERROR  Could not find dimension: \"$dim\". Exiting." ); 
					$dbh->rollback();
					exit 1; 
				}
				$sth=runSQL($dbh,"select count(*) from NC4_DIMENSIONLIST where N_ARRAYID=$arrayID and N_DIMENSIONORDER=$dimOrder");
				my $count=$sth->fetchrow_array();
				if ($count < 1) { #new array
					$success = $dbh->do("insert into NC4_DIMENSIONLIST (N_ARRAYID, N_DIMENSIONORDER, N_DIMENSIONID, N_DATAPARTITION) 
							values ($arrayID, $dimOrder, $dimID, $dimDataPartition)");
					if ($success != 1) {
						logger( "ERROR  Insert in NC4_DIMENSIONLIST table failed. Check Product Profile. Exiting..." );
						$dbh->rollback();
						exit 1;
					}
				} else { #update dimension order and partition dimension (aka aggregation dimension)				
					$success = $dbh->do("update NC4_DIMENSIONLIST set N_DIMENSIONID=$dimID, N_DATAPARTITION=$dimDataPartition
										where N_ARRAYID=$arrayID and N_DIMENSIONORDER=$dimOrder");
					if ($success != 1) {
						logger( "ERROR  Update to NC4_DIMENSIONLIST table failed. Check Product Profile. Exiting..." );
						$dbh->rollback();
						exit 1;
					}
				}
				$dimOrder ++;
		        $i++;
			}
		        #print "\t DimList: @dimNames\n" ;
			
	 #do array attributes
		    for my $arrayAttributeName ( sort keys %{$ref->{variable}{$arrayName}{'attribute'}} ) {
		
				my $arrayAttributeType = $ref->{'variable'}{$arrayName}{'attribute'}{$arrayAttributeName}{'type'};
				my $arrayAttributeValue = $ref->{'variable'}{$arrayName}{'attribute'}{$arrayAttributeName}{'value'};
				my $arrayAttributeDelimiter = "";
				my $arrayNumAttrValues = 0;
	
				if( !defined($arrayAttributeValue) || $arrayAttributeValue eq "" ){ logger( "WARN:  No attribute value found for attribute \"$arrayAttributeName\"" );}
				else{ $arrayNumAttrValues = 1;}
				
				#set type as string if type is missing
				if( !defined($arrayAttributeType) ){
					$arrayAttributeType = "string";
				}
				
				if( $arrayAttributeType ne "string" && $arrayNumAttrValues == 1 ){
					$arrayAttributeDelimiter = " ";
					my @attributeValues = split($separator,$arrayAttributeValue);
					$arrayNumAttrValues = @attributeValues;
				}
				
				if (!defined($arrayID) ){ #new array
					logger("INFO    	  Adding $arrayAttributeName ($arrayID): \"$arrayAttributeValue\"");
					$success = $dbh->do( "insert into NC4_ARRAYATTRIBUTE (N_ARRAYATTRIBUTEID, N_ARRAYID, N_ARRAYATTRIBUTENAME, N_ARRAYATTRIBUTEDATATYPE, N_ARRAYATTRIBUTEEXTERNALFLAG,
										  N_ARRAYATTRIBUTEDELIMITER, N_ARRAYATTRIBUTESTRINGVALUE, N_ARRAYNUMATTRVALUES)	
											values (nextval('S_NC4_ARRAYATTRIBUTE'), $arrayID, '$arrayAttributeName', '$arrayAttributeType', 0,
												'$arrayAttributeDelimiter', '$arrayAttributeValue', $arrayNumAttrValues)" );
					if ($success != 1) {
						logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Check Product Profile. Exiting..." );
						$dbh->rollback();
						exit 1;
					}
				} else { #update attributes
					$sth=runSQL($dbh,"select count(*) from NC4_ARRAYATTRIBUTE where N_ARRAYID=$arrayID and N_ARRAYATTRIBUTENAME='$arrayAttributeName'");
					my $attCount = $sth->fetchrow_array();
					if ($attCount > 1) {
						logger("ERROR  Duplicate attribute ID for this attribute. Check the Product Profile. Exiting.");
						$dbh->rollback();
						exit 1;
					}
					$sth=runSQL($dbh,"select N_ARRAYATTRIBUTEID from NC4_ARRAYATTRIBUTE where N_ARRAYID=$arrayID and N_ARRAYATTRIBUTENAME='$arrayAttributeName'");
					my $n_arrayAttID=$sth->fetchrow_array();
					
					if ( !defined($n_arrayAttID) ) {
						logger("INFO    	  Adding $arrayAttributeName: \"$arrayAttributeValue\"");
						$success = $dbh->do( "insert into NC4_ARRAYATTRIBUTE (N_ARRAYATTRIBUTEID, N_ARRAYID, N_ARRAYATTRIBUTENAME, N_ARRAYATTRIBUTEDATATYPE, N_ARRAYATTRIBUTEEXTERNALFLAG,
										  N_ARRAYATTRIBUTEDELIMITER, N_ARRAYATTRIBUTESTRINGVALUE, N_ARRAYNUMATTRVALUES)	
											values (nextval('S_NC4_ARRAYATTRIBUTE'), $arrayID, '$arrayAttributeName', '$arrayAttributeType', 0,
												'$arrayAttributeDelimiter', '$arrayAttributeValue', $arrayNumAttrValues)" );
						if ($success != 1) {
							logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Check Product Profile. Exiting..." );
							$dbh->rollback();
							exit 1;
						}
						if ($arrayAttributeName eq 'transform' && $arrayAttributeValue eq 'bit') { #bit mask array
							my $i=1;
							for my $datumName ( sort keys %{$ref->{variable}{$arrayName}{'datum'}} ) {
								my $bit_longName=$datumName;
								my $bit_offset=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'offset'};
								my $bit_length=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'length'};
								my $bit_values=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'values'};
								my $bit_meanings=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'meanings'};
								my $maskName=$arrayName . "_flag" . $i;
								logger("INFO    	  Adding $maskName:");
								logger("INFO    	    Bit offset: $bit_offset");
					          	$success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
					                                  '$maskName\_bit_offset','short',1,null,'$bit_offset',1)");
					          	if ($success != 1) {
									logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
									$dbh->rollback();
									exit 1;
						  	  	} 
						  	    logger("INFO    	    Bit length: $bit_length");
						  	    $success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
					                                  '$maskName\_bit_length','short',1,null,'$bit_length',1)");
					          	if ($success != 1) {
									logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
									$dbh->rollback();
									exit 1;
						  	  	}
						  	    logger("INFO    	    Bit long name: $bit_longName");
						  	    $success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
					                                  '$maskName\_bit_long_name','string',1,null,'$bit_longName',1)");
					          	if ($success != 1) {
									logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
									$dbh->rollback();
									exit 1;
						  	  	}
						  	  	my @numAttr = split(/,/,$bit_values);
						  	  	my $numAttr = @numAttr;
						  	    logger("INFO    	    Bit values: $bit_values");
						  	    $success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
					                                  '$maskName\_bit_values','ubyte',1,',','$bit_values',$numAttr)");
					          	if ($success != 1) {
									logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
									$dbh->rollback();
									exit 1;
						  	  	}
						  	    logger("INFO    	    Bit meanings: $bit_meanings");
						  	  	$success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
					                                  '$maskName\_bit_meanings','string',1,',','$bit_meanings',$numAttr)");
					          	if ($success != 1) {
									logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
									$dbh->rollback();
									exit 1;
						  	  	}
								$i=$i+1;
							}
						}
					} else {
						logger("INFO    	  $arrayAttributeName: \"$arrayAttributeValue\"");
						$success = $dbh->do("update NC4_ARRAYATTRIBUTE set N_ARRAYATTRIBUTEDATATYPE='$arrayAttributeType',N_ARRAYATTRIBUTESTRINGVALUE='$arrayAttributeValue',
												N_ARRAYNUMATTRVALUES=$arrayNumAttrValues where N_ARRAYATTRIBUTEID=$n_arrayAttID");
						if ($success != 1) {
							logger( "ERROR  Update to NC4_ARRAYATTRIBUTE table failed. Check Product Profile. Exiting..." );
							$dbh->rollback();
							exit 1;
						}
						if ($arrayAttributeName eq 'transform' && $arrayAttributeValue eq 'bit') {
							my $i=1;
							for my $datumName ( sort keys %{$ref->{variable}{$arrayName}{'datum'}} ) {
								my $bit_longName=$datumName;
								my $bit_offset=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'offset'};
								my $bit_length=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'length'};
								my $bit_values=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'values'};
								my $bit_meanings=$ref->{'variable'}{$arrayName}{'datum'}{$datumName}{'meanings'};
								my $maskName=$arrayName . "_flag" . $i;
								logger("INFO    	  Updating $maskName:");
								
								logger("INFO    	    Bit long name: $bit_longName");
								$sth=runSQL($dbh,"select N_ARRAYATTRIBUTEID from NC4_ARRAYATTRIBUTE where
										N_ARRAYATTRIBUTENAME='$maskName\_bit_long_name' and N_ARRAYID=$arrayID");
								my $attID=$sth->fetchrow_array();
								if (defined($attID)) {
									$success=$dbh->do("update NC4_ARRAYATTRIBUTE set N_ARRAYATTRIBUTESTRINGVALUE='$bit_longName', N_ARRAYATTRIBUTEDATATYPE='string'
										where N_ARRAYATTRIBUTEID=$attID");
						          	if ($success != 1) {
										logger( "ERROR  Update in NC4_ARRAYATTRIBUTE table failed. Exiting." );
										$dbh->rollback();
										exit 1;
							  	  	}
							  	else {
							  		$success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
					                                  '$maskName\_bit_long_name','string',1,null,'$bit_longName',1)");
						          	if ($success != 1) {
										logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
										$dbh->rollback();
										exit 1;
							  	  	}
							  	}
							  	}
							  	
						  	  	logger("INFO    	    Bit offset: $bit_offset");
								$sth=runSQL($dbh,"select N_ARRAYATTRIBUTEID from NC4_ARRAYATTRIBUTE where
										N_ARRAYATTRIBUTENAME='$maskName\_bit_offset' and N_ARRAYID=$arrayID");
								$attID=$sth->fetchrow_array();
								if (defined($attID)) {
									$success=$dbh->do("update NC4_ARRAYATTRIBUTE set N_ARRAYATTRIBUTESTRINGVALUE='$bit_offset', N_ARRAYATTRIBUTEDATATYPE='short'
										where N_ARRAYATTRIBUTEID=$attID");
						          	if ($success != 1) {
										logger( "ERROR  Update in NC4_ARRAYATTRIBUTE table failed. Exiting." );
										$dbh->rollback();
										exit 1;
							  	  	}
								} else {
									$success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
					                                  '$maskName\_bit_offset','short',1,null,'$bit_offset',1)");
						          	if ($success != 1) {
										logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
										$dbh->rollback();
										exit 1;
							  	  	}
								}
						  	  	
						  	  	logger("INFO    	    Bit length: $bit_length");
								$sth=runSQL($dbh,"select N_ARRAYATTRIBUTEID from NC4_ARRAYATTRIBUTE where
										N_ARRAYATTRIBUTENAME='$maskName\_bit_length' and N_ARRAYID=$arrayID");
								$attID=$sth->fetchrow_array();
								if (defined($attID)) {
									$success=$dbh->do("update NC4_ARRAYATTRIBUTE set N_ARRAYATTRIBUTESTRINGVALUE='$bit_length', N_ARRAYATTRIBUTEDATATYPE='short'
										where N_ARRAYATTRIBUTEID=$attID");
						          	if ($success != 1) {
										logger( "ERROR  Update in NC4_ARRAYATTRIBUTE table failed. Exiting." );
										$dbh->rollback();
										exit 1;
							  	  	}
								} else {
							  		$success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
					                                  '$maskName\_bit_length','short',1,null,'$bit_length',1)");
						          	if ($success != 1) {
										logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
										$dbh->rollback();
										exit 1;
							  	  	}
							  	}
						  	  	my @numAttr = split(/,/,$bit_values);
						  	  	my $numAttr = @numAttr;
						  	  	logger("INFO    	    Bit values: $bit_values");
						  	  	$sth=runSQL($dbh,"select N_ARRAYATTRIBUTEID from NC4_ARRAYATTRIBUTE where
										N_ARRAYATTRIBUTENAME='$maskName\_bit_values' and N_ARRAYID=$arrayID");
								$attID=$sth->fetchrow_array();
								if (defined($attID)) {
									$success=$dbh->do("update NC4_ARRAYATTRIBUTE set N_ARRAYATTRIBUTESTRINGVALUE='$bit_values', N_ARRAYATTRIBUTEDATATYPE='ubyte',
										N_ARRAYNUMATTRVALUES=$numAttr
										where N_ARRAYATTRIBUTEID=$attID");
						          	if ($success != 1) {
										logger( "ERROR  Update in NC4_ARRAYATTRIBUTE table failed. Exiting." );
										$dbh->rollback();
										exit 1;
							  	  	}
								} else {
									$success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
					                                  '$maskName\_bit_values','ubyte',1,',','$bit_values',$numAttr)");
						          	if ($success != 1) {
										logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
										$dbh->rollback();
										exit 1;
							  	  	}
								}
						  	  	
						  	  	logger("INFO    	    Bit meanings: $bit_meanings");
						  	  	$sth=runSQL($dbh,"select N_ARRAYATTRIBUTEID from NC4_ARRAYATTRIBUTE where
										N_ARRAYATTRIBUTENAME='$maskName\_bit_meanings' and N_ARRAYID=$arrayID");
								$attID=$sth->fetchrow_array();
								if (defined($attID)) {
									$success=$dbh->do("update NC4_ARRAYATTRIBUTE set N_ARRAYATTRIBUTESTRINGVALUE='$bit_meanings', N_ARRAYATTRIBUTEDATATYPE='string',
										N_ARRAYNUMATTRVALUES=$numAttr
										where N_ARRAYATTRIBUTEID=$attID");
						          	if ($success != 1) {
										logger( "ERROR  Update in NC4_ARRAYATTRIBUTE table failed. Exiting." );
										$dbh->rollback();
										exit 1;
							  	  	}
								} else {
									$success=$dbh->do("insert into NC4_ARRAYATTRIBUTE values (nextval('S_NC4_ARRAYATTRIBUTE'),$arrayID,
					                                  '$maskName\_bit_meanings','string',1,',','$bit_meanings',$numAttr)");
						          	if ($success != 1) {
										logger( "ERROR  Insert in NC4_ARRAYATTRIBUTE table failed. Exiting." );
										$dbh->rollback();
										exit 1;
							  	  	}
								}
						  	  	$i=$i+1;
							}
						}
					}
				}
		    }
		
		}
	}
$i++;
}

if ($i == 0) {
	logger ("ERROR  Product \"$ref->{CollectionShortName}\" does not exist. Exiting.");
	$dbh->rollback();
	exit 1;
}


$dbh->commit;

logger ("INFO Addition of Product Profile for \"$ref->{attribute}->{title}->{value}\" has been successful." );
my $rc = system("cp $opt_f /opt/data/nde/$opt_m\/xmls");
if ( $rc ) {
  logger( "ERROR Failed to copy xml file: $opt_f" );
} else {
  logger( "INFO $opt_f successfully copied");
}
logger( "Execution Completed.");
logger("----");

sub runDupeCheck {
  my ( $dbh, $sql ) = @_;
  my $sth = $dbh->prepare( $sql );
  $sth->execute;
  return $sth->fetchrow_array();
}

sub logger {
  my ( $msg ) = @_;
  print "$msg\n";
  chomp( $HMS = `date +%H:%M:%S,%N` );
  print LOG substr($HMS,0,12) . " $msg\n";
}

exit(0);
