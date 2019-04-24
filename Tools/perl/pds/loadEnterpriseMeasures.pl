#!/usr/bin/env perl

#
# name: loadEnterpriseMeasures.pl
#
# revised:  20110719 lhf, creation
#           20110721 lhf, correct to use corrected XML, add separator
#			20110729 htp, added prints, Measure_N_Array_xref support, and general code clean-up
#			20120607 dcp, added code for dealing with tailoring tailored products (measure names, product id, etc.)
#			20130326 dcp, added code to register quality flag bit information into measure_h_array_xref
#			20130402 dcp, added registration of array scale references to measure_h_array_xref
#		    20140403 dcp, added capability to update enterprise measures and refactoring (error handling)
#		    20140721 dcp, added capability to insert bit mask information
#			20141013 dcp, added platform mapping to product IDs
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
my $logfile = "/opt/data/nde/" . $opt_m . "/logs/pds/ENTERPRISEMEASURES_" . reverse($rfn) . "_" . $YMDHMS . ".log";

open ( LOG, ">$logfile" ) or
  die "ERROR Error opening logfile $logfile, exiting...\n";

logger(  "INFO Execution started..." );

use DBI;
print "\nExecution started...\n";
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


my $separator = "@";

my %ref;
my $ref = XMLin($opt_f, forcearray => [ 'OrderedDimension', 'Measure', 'EnterpriseDimensionGroup', 'EnterpriseDimension' ]);

#print Dumper($ref);



# 1st: insert into the ENTERPRISEDIMENSION table
logger( "INFO  Adding Enterprise Dimensions.");

for my $eDimName ( sort keys %{$ref->{EnterpriseDimensions}->{EnterpriseDimension}} ) {
	logger( "INFO  	 $eDimName" );
	logger("INFO   	 	Start: \"$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{start}\"");
	logger("INFO   		Interval: \"$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{interval}\"");
	logger("INFO   		End: \"$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{end}\"");
	logger("INFO   		Storage size: \"$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{storagesize}\"");
	logger("INFO   		Max storagesize: \"$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{storageMaxSize}\"");
	
	if ( ! runDupeCheck( $dbh, "select count(*) from ENTERPRISEDIMENSION where E_DIMENSIONNAME='$eDimName'" ) ) {
		$success =$dbh->do( "insert into ENTERPRISEDIMENSION (E_DIMENSIONID, E_DIMENSIONNAME, E_DIMENSIONSTART, E_DIMENSIONINTERVAL, E_DIMENSIONEND, 
													E_DIMENSIONSTORAGESIZE, E_DIMENSIONSTORAGEMAXSIZE) 
					values (nextval('S_ENTERPRISEDIMENSION'), '$eDimName', 
							'$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{start}', 
							'$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{interval}',
							'$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{end}',  
							  $ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{storagesize},
							  $ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{storageMaxSize})");
		if ($success != 1) {
			logger( "ERROR  Insert into ENTERPRISEDIMENSION table failed. Check Product Profile. Exiting..." );
			$dbh->rollback();
			exit 1;
		}
	} else {
		$sth=runSQL($dbh,"select E_DIMENSIONID from ENTERPRISEDIMENSION where E_DIMENSIONNAME='$eDimName'");
		my $e_dimID=$sth->fetchrow_array();
		$success=$dbh->do("update ENTERPRISEDIMENSION set E_DIMENSIONSTART='$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{start}',
							E_DIMENSIONINTERVAL='$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{interval}',
							E_DIMENSIONEND='$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{end}',
							E_DIMENSIONSTORAGESIZE=$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{storagesize},
							E_DIMENSIONSTORAGEMAXSIZE=$ref->{EnterpriseDimensions}->{EnterpriseDimension}{$eDimName}{storageMaxSize}
							 where E_DIMENSIONID=$e_dimID");
		if ($success != 1) {
			logger( "ERROR  Update to ENTERPRISEDIMENSION table failed. Check Product Profile. Exiting..." );
			$dbh->rollback();
			exit 1;
		}			
	}
}



# 2nd: insert the EnterpriseDimensionList rows (now they're EnterpriseDimensionGroups)
logger("INFO  Adding to Enterprise Dimension Lists, Ordered Dimensions, and Measures.");
for my $eDimListName ( sort keys %{$ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}} ) {
	logger( "INFO    $eDimListName");
	if ( ! runDupeCheck( $dbh, "select count(*) from ENTERPRISEDIMENSIONLIST where E_DIMENSIONLISTNAME = '$eDimListName'" ) ) {
		$success =$dbh->do( "insert into ENTERPRISEDIMENSIONLIST (E_DIMENSIONLISTID, E_DIMENSIONLISTNAME)
					values (nextval('S_ENTERPRISEDIMENSIONLIST'), '$eDimListName')");
		if ($success != 1) {
			logger( "ERROR  Insert into ENTERPRISEDIMENSIONLIST table failed. Check Product Profile. Exiting..." );
			$dbh->rollback();
			exit 1;
		}
	}
	
	$sth = runSQL( $dbh, "select E_DIMENSIONLISTID from ENTERPRISEDIMENSIONLIST where E_DIMENSIONLISTNAME = '$eDimListName'" );
	my $e_DimListID = $sth->fetchrow_array();
	

  	# 3rd: create the Ordered Dimensions (within EnterpriseDimensionGroup)
	for my $eOrderDimName ( sort keys %{$ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}{$eDimListName}{OrderedDimensions}{OrderedDimension}} ) {
		
		$sth = runSQL( $dbh, "select E_DIMENSIONID from ENTERPRISEDIMENSION where E_DIMENSIONNAME = '$eOrderDimName'" );
		my $eDimID = $sth->fetchrow_array();
		
		my $dimOrder = $ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}{$eDimListName}{OrderedDimensions}{OrderedDimension}{$eOrderDimName}{'e_DimensionOrder'};
		my $dimGB = $ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}{$eDimListName}{OrderedDimensions}{OrderedDimension}{$eOrderDimName}{'e_dimensionDataPartition'};
		
		logger( "INFO    	$eOrderDimName (Ent Dimension ID: $eDimID) | Order: $dimOrder | Gran Boundary (partition): $dimGB" );
		
		if ( ! runDupeCheck ($dbh,"select count(*) from ENTERPRISEORDEREDDIMENSION where 
			E_DIMENSIONID=$eDimID and E_DIMENSIONLISTID=$e_DimListID and 
			E_DIMENSIONORDER=$dimOrder and E_DIMENSIONDATAPARTITION=$dimGB") ) {
			$success =$dbh->do("insert into ENTERPRISEORDEREDDIMENSION (E_DIMENSIONID, E_DIMENSIONLISTID, E_DIMENSIONORDER, E_DIMENSIONDATAPARTITION) 
				values ($eDimID, $e_DimListID, $dimOrder, $dimGB)");
	  		if ($success != 1) {
				logger( "ERROR  Insert into ENTERPRISEDIMENSIONLIST table failed. Check Product Profile. Exiting..." );
				$dbh->rollback();
				exit 1;
			}
		} else {
			$success =$dbh->do("update ENTERPRISEORDEREDDIMENSION set E_DIMENSIONORDER=$dimOrder, E_DIMENSIONDATAPARTITION=$dimGB
								 where E_DIMENSIONID=$eDimID and E_DIMENSIONLISTID=$e_DimListID"); 
	  		if ($success != 1) {
				logger( "ERROR  Update into ENTERPRISEORDEREDDIMENSION table failed. Check Product Profile. Exiting..." );
				$dbh->rollback();
				exit 1;
			}
		}
	}

	# 4th: add enterprise measures
	for my $eMeasureName ( sort keys %{$ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}{$eDimListName}{Measures}{Measure}} ) {
		#Get the product file extension (nc or h5)
		my $pfne = $ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}{$eDimListName}{Measures}{Measure}{$eMeasureName}{'productExt'};
		my $eMeasureDataType = $ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}{$eDimListName}{Measures}{Measure}{$eMeasureName}{'dataType'};
		my ( $arrayName, $productShortname, $productShortnameNC4 ) = split($separator, $eMeasureName);
		if ($arrayName =~ m/(^.*)_flag\d+$/) {$arrayName = $1;}
		
		#Get the quality array's bit information
		my $transform = $ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}{$eDimListName}{Measures}{Measure}{$eMeasureName}{'transform'};
		my $bitOffset = $ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}{$eDimListName}{Measures}{Measure}{$eMeasureName}{'bitOffset'};
		my $bitLength = $ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}{$eDimListName}{Measures}{Measure}{$eMeasureName}{'bitLength'};
		if (! defined($transform)) {
			$transform="null";
			$bitOffset="null";
			$bitLength="null";
		}
		#Get the array scale reference
		my $scale_reference = $ref->{EnterpriseDimensionGroups}->{EnterpriseDimensionGroup}{$eDimListName}{Measures}{Measure}{$eMeasureName}{'scaleRef'};
		if ($scale_reference eq "") {$scale_reference="null";}
		
		#Get the product id
		if (! defined($productShortnameNC4)) {
			$sql = "select productid from productdescription where productshortname = '$productShortname'";
		} else { #it's an already tailored product
			$sql = "select productid from productdescription where productshortname = '$productShortnameNC4'";
			$eMeasureName =~ s/@.*/@/;
 			$eMeasureName .= $productShortname;
		}
		$st=$dbh->prepare($sql);
		$st->execute;
	while ( $productID=$st->fetchrow_array() ) {
		
		if ( ! runDupeCheck( $dbh, "select count(*) from ENTERPRISEMEASURE where E_DIMENSIONLISTID = $e_DimListID and MEASURENAME = '$eMeasureName'" ) ) {
			logger( "INFO    	Added $eMeasureName: $eMeasureDataType" );
			$success =$dbh->do("insert into ENTERPRISEMEASURE (MEASUREID, E_DIMENSIONLISTID, MEASURENAME, MEASUREDATATYPE) 
					values ( nextval('S_ENTERPRISEMEASURE'), $e_DimListID, '$eMeasureName', '$eMeasureDataType')");
			
			if ($success != 1) {
				logger( "ERROR  Insert into ENTERPRISEMEASURE table failed. Check Product Profile. Exiting..." );
				$dbh->rollback();
				exit 1;
			}
			$sth = runSQL( $dbh, "select MEASUREID from ENTERPRISEMEASURE EM, ENTERPRISEDIMENSIONLIST ED where MEASURENAME = '$eMeasureName'
								and EM.E_DIMENSIONLISTID=ED.E_DIMENSIONLISTID and ED.E_DIMENSIONLISTNAME = '$eDimListName'" );
			$eMeasureID = $sth->fetchrow_array();
		} else {
			$sth = runSQL( $dbh, "select MEASUREID from ENTERPRISEMEASURE EM, ENTERPRISEDIMENSIONLIST ED where MEASURENAME = '$eMeasureName'
								and EM.E_DIMENSIONLISTID=ED.E_DIMENSIONLISTID and ED.E_DIMENSIONLISTNAME = '$eDimListName'" );
			$eMeasureID = $sth->fetchrow_array();
			
			$success =$dbh->do("update ENTERPRISEMEASURE set E_DIMENSIONLISTID=$e_DimListID, MEASUREDATATYPE='$eMeasureDataType' 
									where MEASUREID=$eMeasureID");
			if ($success != 1) {
				logger( "ERROR  Update into ENTERPRISEMEASURE table failed. Check Product Profile. Exiting..." );
				$dbh->rollback();
				exit 1;
			}
			logger( "INFO    Updated $eMeasureName (Ent Measure ID: $eMeasureID): $eMeasureDataType" );
		}
		
		
		
		if ( $pfne eq "h5" ) {
			logger( "INFO      Adding to MEASURE_H_ARRAY_XREF." );
			$sth = runSQL($dbh,"select ha.H_ARRAYID from HDF5_ARRAY ha, HDF5_GROUP hg
					where hg.PRODUCTID = $productID and ha.H_GROUPID = hg.H_GROUPID and H_ARRAYNAME = '$arrayName'");
			my $hArrayID  = $sth->fetchrow_array();
			
			logger("INFO    	$arrayName (Product ID: $productID): $eMeasureName (Enterprise Measure ID: $eMeasureID)");
			logger("INFO    	  Transform: $tranform");
			logger("INFO    	  Bit offset: $bitOffset");
			logger("INFO    	  Bit length: $bitLength");
			logger("INFO    	  Scale/offset reference: $scale_reference");
			
			if ( ! runDupeCheck( $dbh, "select count(*) from MEASURE_H_ARRAY_XREF where MEASUREID = $eMeasureID and H_ARRAYID = $hArrayID" ) ) {
				$success =$dbh->do( "insert into MEASURE_H_ARRAY_XREF ( MEASUREID,H_ARRAYID,H_TRANSFORMTYPE,H_BITOFFSET,
										H_BITLENGTH,H_SCALEFACTORREFERENCE,H_ADDOFFSETREFERENCE) 
											values ( $eMeasureID, $hArrayID, '$transform', $bitOffset, $bitLength, '$scale_reference', '$scale_reference' )" );
				if ($success != 1) {
					logger( "ERROR  Insert into MEASURE_H_ARRAY_XREF table failed. Check Product Profile. Exiting..." );
					$dbh->rollback();
					exit 1;
				}
			} else {
				$success=$dbh->do("update MEASURE_H_ARRAY_XREF set H_TRANSFORMTYPE='$transform',H_BITOFFSET=$bitOffset,H_BITLENGTH=$bitLength,
									H_SCALEFACTORREFERENCE='$scale_reference',H_ADDOFFSETREFERENCE='scale_reference'
									  where MEASUREID = $eMeasureID and H_ARRAYID = $hArrayID");
				if ($success != 1) {
					logger( "ERROR  Update into MEASURE_H_ARRAY_XREF table failed. Check Product Profile. Exiting..." );
					$dbh->rollback();
					exit 1;
				}
			}
		} 
		elsif ( $pfne eq "nc" ) {
			logger( "INFO       Adding to MEASURE_N_ARRAY_XREF." );
			logger( "DEBUG   \$productID = $productID   \$arrayName = '$arrayName'");
			$sth = runSQL($dbh,"select na.N_ARRAYID from NC4_ARRAY na, NC4_GROUP ng
					where ng.PRODUCTID = $productID and na.N_GROUPID = ng.N_GROUPID and N_ARRAYNAME = '$arrayName'");
			my $nArrayID = $sth->fetchrow_array();
			
			logger("INFO     	$arrayName: $eMeasureName (Enterprise Measure ID: $eMeasureID)");
			logger("INFO    	  Transform: $tranform");
			logger("INFO    	  Bit offset: $bitOffset");
			logger("INFO    	  Bit length: $bitLength");
			if (!defined($nArrayID)) {    #Added to account for tailoring of tailored products
			
				$sth = runSQL($dbh, "select na.N_ARRAYID from NC4_ARRAY na, NC4_GROUP ng
					where ng.PRODUCTID = $productID and na.N_GROUPID = ng.N_GROUPID and N_ARRAYNAME = '$eMeasureName'" );
				$nArrayID = $sth->fetchrow_array();
			}
			if ( ! runDupeCheck( $dbh, "select count(*) from MEASURE_N_ARRAY_XREF where MEASUREID= $eMeasureID and N_ARRAYID = $nArrayID") ) {
				$success = $dbh->do(  "insert into MEASURE_N_ARRAY_XREF ( MEASUREID,N_ARRAYID,N_TRANSFORMTYPE,N_BITOFFSET,N_BITLENGTH) 
						values ( $eMeasureID, $nArrayID, '$transform', $bitOffset, $bitLength )" );
				if ($success != 1) {
					logger( "ERROR  Insert into MEASURE_N_ARRAY_XREF table failed. Check Product Profile. Exiting..." );
					$dbh->rollback();
					exit 1;
				}
			} else {
				$success=$dbh->do("update MEASURE_N_ARRAY_XREF set N_TRANSFORMTYPE='$transform', N_BITOFFSET=$bitOffset, N_BITLENGTH=$bitLength
							where MEASUREID = $eMeasureID and N_ARRAYID = $nArrayID");
			}
		}
		else {
			logger( "ERROR  Product extension ($pfne) must be \"nc\" or \"h5\"" );
			exit 1;
		}
	}
  } #while loop over productid
}

$dbh->commit;

logger ("INFO Addition of enterprise measures has been successful." );
my $rc = system("cp $opt_f /opt/data/nde/$opt_m\/xmls");
if ( $rc ) {
  logger( "ERROR Failed to copy xml file: $opt_f" );
} else {
  logger( "INFO $opt_f successfully copied");
}

logger( "INFO  Execution Completed.");


# assumed that queries will return a count(*)
sub runDupeCheck {
	my ($dbh,$sql) = @_;
	logger ( "DEBUG   About to execute this query: $sql");
	my $sth = $dbh->prepare($sql);
	$sth->execute;
	my ($count) = $sth->fetchrow_array();

	if ( $count ) {
		logger ("WARN  Duplicate found. Continuing with update.");
	}

	return $count;
}

sub logger {
  my ( $msg ) = @_;
  print "$msg\n";
  chomp( $HMS = `date +%H:%M:%S,%N` );
  print LOG substr($HMS,0,12) . " $msg\n";
}

exit(0);
