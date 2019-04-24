#!/usr/bin/env perl

#
# name: registerDataProductProfile.pl
# purpose: Scan an NPOESS Data Product Profile and load into 'Tailoring Tables', this procedure
#          is the optional second step of DPD for products to be made available for tailoring.
#
# authors: Lou Fenichel, Tom Feroli
#
# revised: 20091023 lhf/tjf, creation
#          20091026 lhf, update to include forcearrays for Datum and LegendEntry
#          20091027 lhf, rename, document, and place under CM control in the NDE_Tools project
#          20091117 lhf, correct Out of Memory error w/VIIRS profiles
#          20091120 lhf, update groups to include full path
#          20091210 lhf, remove structure (now handled by the product itself), use ->{RangeMin/Max}
#          20100304 lhf, correct issue with add_offset
#          20110422 lhf, -s --> -m
#          20110706 lhf, prevent loading the same product profile more than once (NDE-53)
#		   20140403 dcp, major refactoring (error handling) and added capability to update profiles
#		   20141002 dcp, updates to populate hdf5 attribute table data types
#		   20141013 dcp, added platform mapping to product IDs
#
# note: this program has been modified to forcearray for ProductData, this is because it appears 
#       the the quality flags are handled differently depending on the NPOESS product profile
#       For example, in the ATMS-SDR-GEO there is only one ProductData with
#             <DataName>ATMS SDR Geolocation Product Profile</DataName>, this contains all <Field>
#       tags (arrays), which include StartTime, MidTime, Longitude, etc. plus QF1_ etc. 
#
#       But in the VIIRS-MOD-GEO-TC there are two ProductData tags with 
#             <DataName>VIIRS M-Band SDR Terrain Corrected Geolocation Product Profile</DataName>
#             <DataName>VIIRS M-Band SDR Terrain Corrected Geolocation Product Profile - Quality Flags</DataName>
#
#       Ok for now (20091117) since the DataName tag is not significant.
#
# assumptions: 
#	-QF arrays are identified by their Datum[0] DataType of m/bit\(s\)/
#	-FillValues and QF arrays are mutually exclusive
#	-Scaling and QF arrays are mutually exclusive
#

use XML::Simple;
use Data::Dumper;

use Getopt::Std;
getopt('m:f:');

# 1=true, 2=false
#my $debug = 1;
my $debug = 0;

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
my $logfile = "/opt/data/nde/" . $opt_m . "/logs/pds/PRODUCTPROFILE_IDPS_" . reverse($rfn) . "_" . $YMDHMS . ".log";

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
$dbh->begin_work;

#Convert HDF5 IDPS data type to CDL (NC4) data type for attributes
my %dataTypeH = ();
$dataTypeH{'signed 8-bit character'}='char';
$dataTypeH{'signed 8-bit char'}='char';
$dataTypeH{'unsigned 8-bit character'}='char';
$dataTypeH{'unsigned 8-bit char'}='char';
$dataTypeH{'unsigned 16-bit integer'}='ushort';
$dataTypeH{'16-bit integer'}='short';
$dataTypeH{'unsigned 32-bit integer'}='uint';
$dataTypeH{'32-bit integer'}='int';
$dataTypeH{'32-bit floating point'}='float';
$dataTypeH{'64-bit floating point'}='double';
$dataTypeH{'64-bit integer'}='int64';
$dataTypeH{'unsigned 64-bit integer'}='uint64';

my %ref;
my $ref = XMLin($opt_f, forcearray => [ 'ProductData', 'Field', 'Datum', 'Dimension', 'FillValue', 'LegendEntry' ]);

print Dumper($ref), if ( $debug );

# get CollectionShortname look up productid 
logger( "INFO  Checking for existing product." );
my $st=runSQL($dbh,"select PRODUCTID from PRODUCTDESCRIPTION where PRODUCTSHORTNAME='$ref->{CollectionShortName}'");

#Loop through productIDs with same shortname (multiple platforms)
my $i=0;

while ( my $productid = $st->fetchrow_array() ) {
	
	logger ("INFO  Profile for product \"$ref->{CollectionShortName}\" (Product ID: $productid)");
	
	# Check if update or new product profile
	$sth=runSQL($dbh,"select H_GROUPID from HDF5_GROUP where PRODUCTID=$productid and H_GROUPNAME='/'");
	my $h_groupid = $sth->fetchrow_array();
	my $update="n"; #initialize the update flag
	if (!defined $h_groupid) {
		logger( "INFO  No existing profile. Adding new profile." );
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
	
	if ($update eq 'n') { #Add new profile
	
	#Insert HDF5 groups
	#Insert into root group
		print "Debug1\n";
		my $success=$dbh->do("insert into HDF5_Group (H_GROUPID,HDF_H_GROUPID,PRODUCTID,H_GROUPNAME) 
	                        values (nextval('s_hdf5_group'),null,$productid,'/')");
		print "Debug2\n";
	    if ($success != 1) {
			logger( "ERROR  Insert in HDF5_GROUP table failed (root group). Exiting..." );
			$dbh->rollback();
			exit 1;
		}
		my $sth=runSQL($dbh,"select h_groupid from HDF5_Group where productid=$productid and h_groupname='/'");
		my $h_groupid = $sth->fetchrow_array();
		if (!defined $h_groupid) {
			logger( "ERROR  No H_GROUPID retrieved. Insert failed? Exiting..." );
			$dbh->rollback();
			exit 1;
		}
		logger( "INFO   HDF5 ROOT GROUP ID: $h_groupid");
		
	# Insert into All_Data
		$success=$dbh->do("insert into HDF5_Group (H_GROUPID,HDF_H_GROUPID,PRODUCTID,H_GROUPNAME) 
		                        values (nextval('S_HDF5_GROUP'),$h_groupid,$productid,'/All_Data/')");
		if ($success != 1) {
			logger( "ERROR  Insert in HDF5_GROUP table failed (All_Data group). Exiting..." );
			$dbh->rollback();
			exit 1;
		}
		$sth=runSQL($dbh,"select h_groupid from HDF5_Group where productid=$productid and h_groupname='/All_Data/'");
		my $h_groupid_All_Data = $sth->fetchrow_array();
	
	# Insert into All_Data/Collection_All	
		$success=$dbh->do("insert into HDF5_Group (H_GROUPID,HDF_H_GROUPID,PRODUCTID,H_GROUPNAME) 
		                        values (nextval('S_HDF5_GROUP'),$h_groupid_All_Data,$productid,'/All_Data/$ref->{CollectionShortName}_All/')");
		if ($success != 1) {
			logger( "ERROR  Insert in HDF5_GROUP table failed (All_Data/$ref->{CollectionShortName}_All group). Exiting..." );
			$dbh->rollback();
			exit 1;
		}                      
		$sth=runSQL($dbh,"select h_groupid from HDF5_Group where productid=$productid and h_groupname='/All_Data/$ref->{CollectionShortName}_All/'");
		my $h_groupid_CollectionName_All_Data = $sth->fetchrow_array();
	
	# Insert into Data_Products	
		$success=$dbh->do("insert into HDF5_Group (H_GROUPID,HDF_H_GROUPID,PRODUCTID,H_GROUPNAME) 
		                        values (nextval('S_HDF5_GROUP'),$h_groupid,$productid,'/Data_Products/')");
		if ($success != 1) {
			logger( "ERROR  Insert in HDF5_GROUP table failed (Data_Products group). Exiting..." );
			$dbh->rollback();
			exit 1;
		}                        
		$sth=runSQL($dbh,"select h_groupid from HDF5_Group where productid=$productid and h_groupname='/Data_Products/'");
		my $h_groupid_Data_Products = $sth->fetchrow_array();
	
	# Insert into Data_Products/Collection_All	
		$success=$dbh->do("insert into HDF5_Group (H_GROUPID,HDF_H_GROUPID,PRODUCTID,H_GROUPNAME) 
		                        values (nextval('S_HDF5_GROUP'),$h_groupid_Data_Products,$productid,'/Data_Products/$ref->{CollectionShortName}/')");
		if ($success != 1) {
			logger( "ERROR  Insert in HDF5_GROUP table failed (Data_Products/$ref->{CollectionShortName} group). Exiting..." );
			$dbh->rollback();
			exit 1;
		}                        
		$sth=runSQL($dbh,"select h_groupid from HDF5_Group where productid=$productid and h_groupname='/Data_Products/$ref->{CollectionShortName}/'");
		my $h_groupid_CollectionName_Data_Products = $sth->fetchrow_array();	
	
	# Insert HDF5_ARRAY information
		print "Prior to \$ref->{ProductData} for loop\n", if ( $debug );	
		for my $ProductData_parm ( @{$ref->{ProductData}} ) {
			  print "$ProductData_parm: $ProductData_parm\n", if ( $debug );
			  
	   # for each array (<Field>)
			  my $h_datatype = "";
			  print "Prior to \$ProductData_parm->{Field} for loop\n", if ( $debug );
			  
			  logger( "INFO   Adding arrays and dimension lists.");
			  for my $parm (@{$ProductData_parm->{Field}}) {		    
			    if (  $parm->{Name} =~ m/PadByte/ ) {  
			      logger( "WARN  Ignoring $parm->{Name} and continuing (this is normal).");
			    } else {
			      # Store array name (default datatype for update later (datatype value is in Datum[0]))
			      $success=$dbh->do("insert into HDF5_ARRAY 
			                              values (nextval('S_HDF5_ARRAY'),$h_groupid_CollectionName_All_Data,'$parm->{Name}',
			                               'DATATYPEHOLDVALUE')");
			      if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAY table failed for \"$parm->{Name}\". Exiting..." );
						$dbh->rollback();
						exit 1;
				  }                         
			    
			      $sth=runSQL($dbh,"select H_ARRAYID from HDF5_ARRAY where H_GROUPID=$h_groupid_CollectionName_All_Data and H_ARRAYNAME='$parm->{Name}'");
			      my $h_arrayid = $sth->fetchrow_array();
			      logger("INFO    $parm->{Name} (HDF5 Array ID: $h_arrayid)");
			      
			      print "Prior to process dimensions\n", if ( $debug );
			      # process dimensions
			      my $dimensionOrder = 1; 
			      my $dim_name = "";
			      my $dim_gb = "";
			      my $dim_dynamic = "";
			      my $dim_minindex = "";
			      my $dim_maxindex = "";
			      foreach my $dimparm (@{$parm->{Dimension}}) {
			        print "In dimparm loop \$dimparm: $dimparm\n", if ( $debug );
			        $dim_name = $dim_name . "$dimparm->{Name}" . ",";
			        $dim_gb = $dim_gb . "$dimparm->{GranuleBoundary}" . ",";
			        $dim_dynamic = $dim_dynamic . "$dimparm->{Dynamic}" . ",";
			        $dim_minindex = $dim_minindex . "$dimparm->{MinIndex}" . ",";
			        $dim_maxindex = $dim_maxindex . "$dimparm->{MaxIndex}" . ",";
			        print "before insert to HDF5_DIMENSIONLIST\n", if ( $debug );
			        
			        $success=$dbh->do("insert into HDF5_DIMENSIONLIST
			                    values($h_arrayid,$dimensionOrder,'$dimparm->{MinIndex}',
			                     '$dimparm->{MaxIndex}',0,$dimparm->{GranuleBoundary})");
			        if ($success != 1) {
						logger( "ERROR  Insert in HDF5_DIMENSIONLIST table failed for \"$dim_name\". Exiting..." );
						$dbh->rollback();
						exit 1;
				    }
			        $dimensionOrder += 1;
			      }
			      $dimensionOrder = $dimensionOrder - 1;
			    
			      # store HDF5_ArrayAttribute for the group of dimensions
			      $dim_name = substr($dim_name,0,length($dim_name)-1);
			      $dim_dynamic = substr($dim_dynamic,0,length($dim_dynamic)-1);
			      $dim_minindex = substr($dim_minindex,0,length($dim_minindex)-1);
			      $dim_maxindex  = substr($dim_maxindex,0,length($dim_maxindex)-1);
			      $dim_gb = substr($dim_gb,0,length($dim_gb)-1);
			      		      
			      print "before insert to HDF5_ARRAYATTRIBUTE\n", if ( $debug );
			      
			      my @attCount=split(",",$dim_name);
			      my $count=scalar(@attCount);
			      logger("INFO    	dim_name: $dim_name");
			      $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                  'dim_name',1,'string',$count,'$dim_name',',')");
			      if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
						$dbh->rollback();
						exit 1;
				  }
				  
				  @attCount=split(",",$dim_dynamic);
			      $count=scalar(@attCount);
			      logger("INFO    	dim_dynamic: $dim_dynamic");
			      $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                  'dim_dynamic',1,'byte',$count,'$dim_dynamic',',')");
			      if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
						$dbh->rollback();
						exit 1;
				  }
			      
			      @attCount=split(",",$dim_minindex);
			      $count=scalar(@attCount);
			      logger("INFO    	dim_minindex: $dim_minindex");
			      $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                 'dim_minindex',1,'short',$count,'$dim_minindex',',')");
			      if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
						$dbh->rollback();
						exit 1;
				  }
			      
			      @attCount=split(",",$dim_maxindex);
			      $count=scalar(@attCount);
			      logger("INFO    	dim_maxindex: $dim_maxindex");
			      $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                  'dim_maxindex',1,'short',$count,'$dim_maxindex',',')");
			      if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
						$dbh->rollback();
						exit 1;
				  }
			      
			      @attCount=split(",",$dim_gb);
			      $count=scalar(@attCount);
			      logger("INFO    	dim_gb (partition): $dim_gb");
			      $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                  'dim_gb',1,'byte',$count,'$dim_gb',',')");
			      if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
						$dbh->rollback();
						exit 1;
				  }
			      
			      print "after all inserts to HDF5_ARRAYATTRIBUTE\n", if ( $debug );
			    
			      # For each occurrence of datum, sometimes there's only one
			      my $long_name = "";
			      my $scale_factor = "";
			      my $add_offset = "";
			      my $units = "";
			      my $_FillValueName = "";
			      my $_FillValue = "";
			      my $datumLoopCount = 0;
			      my $qfAttributeNumber = 0;
			      for my $datumParm (@{$parm->{Datum}}) {
			    
			        $datumLoopCount += 1;
			        print "\tDatumLoop...\$datumLoopCount: $datumLoopCount\n", if ( $debug );
			    
			        # In the very first Datum of QF arrays (supposedly), the data type will be '*bit(s)' ...always...this identifies it as a Quality Flag array
			        my $qfFlag = 0;
			        $qfFlag = 1, if ($datumParm->{DataType} =~ m/bit\(s\)/);
			    	
			        # If not a QF array, store scale and offset (if necessary)
			        # 20091209 translate RangeMin/Max into valid_min/max
			        if (! $qfFlag) {
			          logger( "INFO    	Data Type: $datumParm->{DataType}" );
			          $success=$dbh->do("update HDF5_ARRAY set H_DATATYPE='$datumParm->{DataType}' where H_ARRAYID=$h_arrayid");
			          if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
						$dbh->rollback();
						exit 1;
				  	  }
			          logger( "INFO    	Long name: $datumParm->{Description}" );
			          my $description = $datumParm->{Description};
			          $description =~ s/'/''/g;
			          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                  'long_name',1,'string',1,'$description',null)");
			          if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed." );
						$dbh->rollback();
						exit 1;
				  	  }
				  	              
			          if ($datumParm->{Scaled} > 0) {
			          	logger("INFO    	Scale factor vector name: $datumParm->{ScaleFactorName}");
			            $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                    'scale_factor',1,'string',1,'$datumParm->{ScaleFactorName}', null)");
			            if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	    }
				  	    
				  	    logger("INFO    	Add offset vector name: $datumParm->{ScaleFactorName}");                        
			            $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                    'add_offset',1,'string',1,'$datumParm->{ScaleFactorName}', null)");
			            if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	    }                        
			          }
			          
			          logger("INFO    	Units: $datumParm->{MeasurementUnits}");
			          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                    'units',1,'string',1,'$datumParm->{MeasurementUnits}',null)");
			          if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
						$dbh->rollback();
						exit 1;
				  	  }                         
			
			          # make sure empty values don't get in
			          if ( $datumParm->{RangeMin} =~ /HASH/ <= 0 ) {
			          	logger("INFO    	Valid min: $datumParm->{RangeMin}");
			            $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                    'valid_min',1,'float',1,'$datumParm->{RangeMin}',null)");
			            if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	    }                         
			          } 
			          if ( $datumParm->{RangeMax} =~ /HASH/ <= 0 ) {
			          	logger("INFO    	Valid max: $datumParm->{RangeMax}");
			            $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                    'valid_max',1,'float',1,'$datumParm->{RangeMax}',null)");
			            if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	    }                         
			          } 
			
			          # Accumulate and store FillValueNames and FillValues
			          my $fillValueName = "";
			          my $fillValue = "";
			          my $fillCount = 0;
			          foreach my $fillparm (@{$datumParm->{FillValue}}) {
			            $fillValueName = $fillValueName . $fillparm->{Name} . ",";
			            $fillValue = $fillValue . $fillparm->{Value} . ",";
			            $fillCount += 1;
			          }
			          $fillValueName = substr($fillValueName,0,length($fillValueName)-1);
			          $fillValue = substr($fillValue,0,length($fillValue)-1);
			          
			          logger("INFO    	Fill value name: $fillValueName");
			          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                  '_FillValueName',1,'string',$fillCount,'$fillValueName',',')");
			          if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  }
				  	  
				  	  logger("INFO    	Fill value: $fillValue");                         
			          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                  '_FillValue',1,'$dataTypeH{$datumParm->{DataType}}',$fillCount,'$fillValue',',')");
			          if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  }                        
			    
			        # Working with a Quality Flag array
			        } else {
			          if ($datumLoopCount == 1) {
			          	logger("INFO    	Data Type: unsigned 8-bit character");
			            $success=$dbh->do("update HDF5_ARRAY set H_DATATYPE='unsigned 8-bit character' where H_ARRAYID=$h_arrayid");
			            if ($success != 1) {
							logger( "ERROR  Update in HDF5_ARRAY table failed for quality flag data type. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  	}
			          }
			          $qfAttributeNumber += 1;
			          my $h_ArrayAttributeName = $parm->{Name} . "_flag" . $qfAttributeNumber;
			          my $bit_length = substr($datumParm->{DataType},0,index($datumParm->{DataType},"\ "));
			    
			          logger("INFO    	Bit offset: $datumParm->{DatumOffset}");
			          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                  '$h_ArrayAttributeName\_bit_offset',1,'short',1,'$datumParm->{DatumOffset}',null)");
			          if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  } 
				  	  
				  	  logger("INFO    	Bit length: $bit_length");                       
			          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                  '$h_ArrayAttributeName\_bit_length',1,'short',1,'$bit_length',null)");
			          if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  }
				  	  
				  	  logger("INFO    	Long name: $datumParm->{Description}");
				  	  my $description = $datumParm->{Description};
				  	  $description =~ s/'/''/g;    #Allows insertion of single quotes in SQL string                   
			          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                  '$h_ArrayAttributeName\_long_name',1,'string',1,'$description',null)");
			          if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  }                        
			    
			          # Process LegendEntry (if applicable)
			          my $qfMeanings = "";
			          my $qfValues = "";
			          my $qfVCount = 0;
			          foreach my $qfparm (@{$datumParm->{LegendEntry}}) {
			            $qfMeanings = $qfMeanings . $qfparm->{Name} . ",";
			            $qfValues = $qfValues . $qfparm->{Value} . ",";
			            $qfVCount += 1;
			          }
			          $qfValues = substr($qfValues,0,length($qfValues)-1);
			          $qfMeanings = substr($qfMeanings,0,length($qfMeanings)-1);
			          
			          logger("INFO    	Quality flag values: $qfValues");
			          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                  '$h_ArrayAttributeName\_values',1,'ubyte',$qfVCount,'$qfValues',',')");
			          if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  }
				  	  
				  	  logger("INFO    	Quality flag meanings: $qfMeanings");                        
			          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
			                                  '$h_ArrayAttributeName\_meanings',1,'string',$qfVCount,'$qfMeanings',',')");
			          if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  }                        
			    
			        }
			      }
			    }
			  }
			}
	} else { #### Update profile	                     
		$sth=runSQL($dbh,"select h_groupid from HDF5_Group where productid=$productid and h_groupname='/All_Data/$ref->{CollectionShortName}_All/'");
		my $h_groupid_CollectionName_All_Data = $sth->fetchrow_array();	
		if (!defined $h_groupid_CollectionName_All_Data) {
			logger("ERROR  No H_GROUPID exists for /All_Data/$ref->{CollectionShortName}_All/");
			$dbh->rollback();
			exit 1;
		}
		
	# Insert HDF5_ARRAY information
		print "Prior to \$ref->{ProductData} for loop\n", if ( $debug );	
		for my $ProductData_parm ( @{$ref->{ProductData}} ) {
			  print "$ProductData_parm: $ProductData_parm\n", if ( $debug );
			  
	   # for each array (<Field>)
			  my $h_datatype = "";
			  print "Prior to \$ProductData_parm->{Field} for loop\n", if ( $debug );
			  
			  logger( "INFO   Updating arrays and dimension lists.");
			  for my $parm (@{$ProductData_parm->{Field}}) {		    
			    if (  $parm->{Name} =~ m/PadByte/ ) {  
			      logger( "WARN  Ignoring $parm->{Name} and continuing (this is normal).");
			    } else {
			      $sth=runSQL($dbh,"select H_ARRAYID from HDF5_ARRAY where H_GROUPID=$h_groupid_CollectionName_All_Data and H_ARRAYNAME='$parm->{Name}'");
			      $h_arrayid = $sth->fetchrow_array();
			      
			      if (!defined $h_arrayid) {  #Add new array  
				    $success=$dbh->do("insert into HDF5_ARRAY 
				                              values (nextval('S_HDF5_ARRAY'),$h_groupid_CollectionName_All_Data,'$parm->{Name}',
				                               'DATATYPEHOLDVALUE')");
				    if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAY table failed for \"$parm->{Name}\". Exiting..." );
						$dbh->rollback();
						exit 1;
					}
					$sth=runSQL($dbh,"select H_ARRAYID from HDF5_ARRAY where H_GROUPID=$h_groupid_CollectionName_All_Data and H_ARRAYNAME='$parm->{Name}'");
			      	$h_arrayid = $sth->fetchrow_array();
					logger("INFO    Adding $parm->{Name} (HDF5 Array ID: $h_arrayid)");
			      } else {
			      	logger("INFO    Updating $parm->{Name} (HDF5 Array ID: $h_arrayid)");
			      }
	
			      print "Prior to process dimensions\n", if ( $debug );
			      # process dimensions
			      my $dimensionOrder = 1; 
			      my $dim_name = "";
			      my $dim_gb = "";
			      my $dim_dynamic = "";
			      my $dim_minindex = "";
			      my $dim_maxindex = "";
			      $sth=runSQL($dbh,"select count(*) from HDF5_DIMENSIONLIST where H_ARRAYID=$h_arrayid");
			      my $arrayNew=$sth->fetchrow_array();
	
			      foreach my $dimparm (@{$parm->{Dimension}}) {
			        print "In dimparm loop \$dimparm: $dimparm\n", if ( $debug );
			        $dim_name = $dim_name . "$dimparm->{Name}" . ",";
			        $dim_gb = $dim_gb . "$dimparm->{GranuleBoundary}" . ",";
			        $dim_dynamic = $dim_dynamic . "$dimparm->{Dynamic}" . ",";
			        $dim_minindex = $dim_minindex . "$dimparm->{MinIndex}" . ",";
			        $dim_maxindex = $dim_maxindex . "$dimparm->{MaxIndex}" . ",";
			        
			        print "before insert to HDF5_DIMENSIONLIST\n", if ( $debug );
	
			        if ($arrayNew < 1)	{ #new array
			        	$success=$dbh->do("insert into HDF5_DIMENSIONLIST
			                    values($h_arrayid,$dimensionOrder,'$dimparm->{MinIndex}',
			                     '$dimparm->{MaxIndex}',0,$dimparm->{GranuleBoundary})");
				        if ($success != 1) {
							logger( "ERROR  Insert in HDF5_DIMENSIONLIST table failed for \"$dim_name\". Exiting..." );
							$dbh->rollback();
							exit 1;
					    }
			        } else {
			        	$success=$dbh->do("update HDF5_DIMENSIONLIST set H_DIMENSIONSIZE='$dimparm->{MinIndex}',
			                     			H_DIMENSIONMAXIMUMSIZE='$dimparm->{MaxIndex}', H_DATAPARTITION=$dimparm->{GranuleBoundary} 
			                     				where H_ARRAYID=$h_arrayid and H_DIMENSIONORDER=$dimensionOrder");
				        if ($success != 1) {
							logger( "ERROR  Update to HDF5_DIMENSIONLIST table failed for \"$dim_name\". Exiting..." );
							$dbh->rollback();
							exit 1;
					    }
			        }	        
			        
			        $dimensionOrder += 1;
			      }
			      $dimensionOrder = $dimensionOrder - 1;
			    
			      # store HDF5_ArrayAttribute for the group of dimensions
			      $dim_name = substr($dim_name,0,length($dim_name)-1);
			      $dim_dynamic = substr($dim_dynamic,0,length($dim_dynamic)-1);
			      $dim_minindex = substr($dim_minindex,0,length($dim_minindex)-1);
			      $dim_maxindex  = substr($dim_maxindex,0,length($dim_maxindex)-1);
			      $dim_gb = substr($dim_gb,0,length($dim_gb)-1);
			      
			      print "before insert to HDF5_ARRAYATTRIBUTE\n", if ( $debug );
			      
			      my @attCount=split(",",$dim_name);
			      my $count=scalar(@attCount);
			      logger("INFO    	dim_name: $dim_name");
			      $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='dim_name'");
			      my $h_arrayattid=$sth->fetchrow_array();
			      if (!defined $h_arrayattid) {
				      $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
				                  'dim_name',1,'string',$count,'$dim_name',',')");
				      if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  }
			      	
			      } else {
				      $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYNUMATTRVALUES=$count,H_ARRAYATTRIBUTESTRINGVALUE='$dim_name'
				      						, H_ARRAYATTRIBUTEDATATYPE='string'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
				      if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  }
			      }
			      
			      @attCount=split(",",$dim_dynamic);
			      $count=scalar(@attCount);
			      logger("INFO    	dim_dynamic: $dim_dynamic");
			      $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='dim_dynamic'");
			      $h_arrayattid=$sth->fetchrow_array();
			      if (!defined $h_arrayattid) {
				      $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
				                  'dim_dynamic',1,'byte',$count,'$dim_dynamic',',')");
				      if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  }
			      } else {
			      	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYNUMATTRVALUES=$count,H_ARRAYATTRIBUTESTRINGVALUE='$dim_dynamic'
			      	  						, H_ARRAYATTRIBUTEDATATYPE='byte'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
				      if ($success != 1) {
							logger( "ERROR  Update HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  }
			      }
			    
			      @attCount=split(",",$dim_minindex);
			      $count=scalar(@attCount);
			      logger("INFO    	dim_minindex: $dim_minindex");
			      $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='dim_minindex'");
			      $h_arrayattid=$sth->fetchrow_array();
			      if (!defined $h_arrayattid) {
				      $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
				                 'dim_minindex',1,'short',$count,'$dim_minindex',',')");
				      if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  }
			      } else {
			      	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYNUMATTRVALUES=$count,H_ARRAYATTRIBUTESTRINGVALUE='$dim_minindex'
			      	  						, H_ARRAYATTRIBUTEDATATYPE='short'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
				      if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  }
			      }
			    
			      @attCount=split(",",$dim_maxindex);
			      $count=scalar(@attCount);
			      logger("INFO    	dim_maxindex: $dim_maxindex");
			      $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='dim_maxindex'");
			      $h_arrayattid=$sth->fetchrow_array();
			      if (!defined $h_arrayattid) {
				      $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
				                  'dim_maxindex',1,'short',$count,'$dim_maxindex',',')");
				      if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  }
			      } else {
			      	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYNUMATTRVALUES=$count,H_ARRAYATTRIBUTESTRINGVALUE='$dim_maxindex'
			      	  						, H_ARRAYATTRIBUTEDATATYPE='short'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
				      if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  }
			      }
			    
			      @attCount=split(",",$dim_gb);
			      $count=scalar(@attCount);
			      logger("INFO    	dim_gb (partition): $dim_gb");
			      $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='dim_gb'");
			      $h_arrayattid=$sth->fetchrow_array();
			      if (!defined $h_arrayattid) {
				      $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval('S_HDF5_ARRAYATTRIBUTE'),$h_arrayid,
				                  'dim_gb',1,'byte',$count,'$dim_gb',',')");
				      if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  }
			      } else {
			      	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYNUMATTRVALUES=$count,H_ARRAYATTRIBUTESTRINGVALUE='$dim_gb'
			      	  						, H_ARRAYATTRIBUTEDATATYPE='byte'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
				      if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  }
			      }
			      
			      print "after all inserts to HDF5_ARRAYATTRIBUTE\n", if ( $debug );
			    
			      # For each occurrence of datum, sometimes there's only one
			      my $long_name = "";
			      my $scale_factor = "";
			      my $add_offset = "";
			      my $units = "";
			      my $_FillValueName = "";
			      my $_FillValue = "";
			      my $datumLoopCount = 0;
			      my $qfAttributeNumber = 0;
			      for my $datumParm (@{$parm->{Datum}}) {
			    
			        $datumLoopCount += 1;
			        print "\tDatumLoop...\$datumLoopCount: $datumLoopCount\n", if ( $debug );
			    
			        # In the very first Datum of QF arrays (supposedly), the data type will be '*bit(s)' ...always...this identifies it as a Quality Flag array
			        my $qfFlag = 0;
			        $qfFlag = 1, if ($datumParm->{DataType} =~ m/bit\(s\)/);
			    	
			        # If not a QF array, store scale and offset (if necessary)
			        # 20091209 translate RangeMin/Max into valid_min/max
			        if (! $qfFlag) {
			          logger( "INFO    	Data Type: $datumParm->{DataType}" );
			          $success=$dbh->do("update HDF5_ARRAY set H_DATATYPE='$datumParm->{DataType}' where H_ARRAYID=$h_arrayid");
			          if ($success != 1) {
						logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
						$dbh->rollback();
						exit 1;
				  	  }
				  	  
			          logger( "INFO    	Long name: $datumParm->{Description}" );
			          my $description = $datumParm->{Description};
			          $description =~ s/'/''/g;
			          $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='long_name'");
			      	  $h_arrayattid=$sth->fetchrow_array();
			      	  if (!defined $h_arrayattid) {
				          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                  'long_name',1,'string',1,'$description',null)");
				          if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed." );
							$dbh->rollback();
							exit 1;
					  	  }
			      	  } else {
			      	  	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYATTRIBUTESTRINGVALUE='$description'
			      	  	  					, H_ARRAYATTRIBUTEDATATYPE='string'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
					      if ($success != 1) {
								logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
						  }
			      	  }
				  	              
			          if ($datumParm->{Scaled} > 0) {
			          	logger("INFO    	Scale factor vector name: $datumParm->{ScaleFactorName}");
			          	$sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='scale_factor'");
			      	  	$h_arrayattid=$sth->fetchrow_array();
			      	  	if (!defined $h_arrayattid) {
				            $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                    'scale_factor',1,'string',1,'$datumParm->{ScaleFactorName}', null)");
				            if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	    }
			      	  	} else {
			      	  		$success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYATTRIBUTESTRINGVALUE='$datumParm->{ScaleFactorName}'
			      	  						, H_ARRAYATTRIBUTEDATATYPE='string'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						    if ($success != 1) {
								logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
							}
			      	  	}
				  	    logger("INFO    	Add offset vector name: $datumParm->{ScaleFactorName}");   
				  	    $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='add_offset'");
			      	  	$h_arrayattid=$sth->fetchrow_array();
			      	  	if (!defined $h_arrayattid) {                     
				            $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                    'add_offset',1,'string',1,'$datumParm->{ScaleFactorName}', null)");
				            if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	    }    
			      	  	} else {
			      	  		$success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYATTRIBUTESTRINGVALUE='$datumParm->{ScaleFactorName}'
			      	  						, H_ARRAYATTRIBUTEDATATYPE='string'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						    if ($success != 1) {
								logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
							}
			      	  	}                    
			          }
			          
			          logger("INFO    	Units: $datumParm->{MeasurementUnits}");
			          $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='units'");
			      	  $h_arrayattid=$sth->fetchrow_array();
			      	  if (!defined $h_arrayattid) {
				           $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                   'units',1,'string',1,'$datumParm->{MeasurementUnits}',null)");
				           if ($success != 1) {
							logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
					  	   }    
			      	  } else {
			      	  	$success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYATTRIBUTESTRINGVALUE='$datumParm->{MeasurementUnits}'
			      	  					, H_ARRAYATTRIBUTEDATATYPE='string'
				      					where H_ARRAYATTRIBUTEID=$h_arrayattid");
						   if ($success != 1) {
								logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
							}
			      		}                     
			
			          # make sure empty values don't get in
			          if ( $datumParm->{RangeMin} =~ /HASH/ <= 0 ) {
			          	logger("INFO    	Valid min: $datumParm->{RangeMin}");
			          	$sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='valid_min'");
			      	  	$h_arrayattid=$sth->fetchrow_array();
			      	  	if (!defined $h_arrayattid) {
				            $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                    'valid_min',1,'float',1,'$datumParm->{RangeMin}',null)");
				            if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	    }     
			      	  	} else {
			      	  		$success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYATTRIBUTESTRINGVALUE='$datumParm->{RangeMin}'
			      	  						, H_ARRAYATTRIBUTEDATATYPE='float'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						    if ($success != 1) {
								logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
							}
			      	  	}                    
			          } 
			          if ( $datumParm->{RangeMax} =~ /HASH/ <= 0 ) {
			          	logger("INFO    	Valid max: $datumParm->{RangeMax}");
			          	$sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='valid_max'");
			      	  	$h_arrayattid=$sth->fetchrow_array();
			      	  	if (!defined $h_arrayattid) {
				            $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                    'valid_max',1,'float',1,'$datumParm->{RangeMax}',null)");
				            if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	    }  
			      	  	} else {
			      	  		$success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYATTRIBUTESTRINGVALUE='$datumParm->{RangeMax}'
			      	  						, H_ARRAYATTRIBUTEDATATYPE='float'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						    if ($success != 1) {
								logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
							}
			      	  	}                       
			          } 
			
			          # Accumulate and store FillValueNames and FillValues
			          my $fillValueName = "";
			          my $fillValue = "";
			          my $fillCount = 0;
			          foreach my $fillparm (@{$datumParm->{FillValue}}) {
			            $fillValueName = $fillValueName . $fillparm->{Name} . ",";
			            $fillValue = $fillValue . $fillparm->{Value} . ",";
			            $fillCount += 1;
			          }
			          $fillValueName = substr($fillValueName,0,length($fillValueName)-1);
			          $fillValue = substr($fillValue,0,length($fillValue)-1);
			          
			          logger("INFO    	Fill value name: $fillValueName");
			          $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='_FillValueName'");
			      	  $h_arrayattid=$sth->fetchrow_array();
			      	  if (!defined $h_arrayattid) {
				          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                  '_FillValueName',1,'string',$fillCount,'$fillValueName',',')");
				          if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	  }
			      	  } else{
			      	  	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYNUMATTRVALUES=$fillCount, H_ARRAYATTRIBUTESTRINGVALUE='$fillValueName'
			      	  	  					, H_ARRAYATTRIBUTEDATATYPE='string'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						  if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
						  } 
			      	  }
				  	  
				  	  logger("INFO    	Fill value: $fillValue");
				  	  $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and H_ARRAYATTRIBUTENAME='_FillValue'");
			      	  $h_arrayattid=$sth->fetchrow_array();
			      	  if (!defined $h_arrayattid) {                         
				          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                  '_FillValue',1,'$dataTypeH{$datumParm->{DataType}}',$fillCount,'$fillValue',',')");
				          if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	  }     
			      	  } else {
			      	  	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYNUMATTRVALUES=$fillCount, H_ARRAYATTRIBUTESTRINGVALUE='$fillValue'
			      	  	  					, H_ARRAYATTRIBUTEDATATYPE='$dataTypeH{$datumParm->{DataType}}'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						  if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
						  }
			      	  }                   
			    
			        # Working with a Quality Flag array
			        } else {
			          if ($datumLoopCount == 1) {
			          	logger("INFO    	Data Type: unsigned 8-bit character");
			            $success=$dbh->do("update HDF5_ARRAY set H_DATATYPE='unsigned 8-bit character' where H_ARRAYID=$h_arrayid");
			            if ($success != 1) {
							logger( "ERROR  Update in HDF5_ARRAY table failed for quality flag data type. Exiting." );
							$dbh->rollback();
							exit 1;
				  	  	}
			          }
			          $qfAttributeNumber += 1;
			          my $h_ArrayAttributeName = $parm->{Name} . "_flag" . $qfAttributeNumber;
			          my $bit_length = substr($datumParm->{DataType},0,index($datumParm->{DataType},"\ "));
			    
			          logger("INFO    	Bit offset: $datumParm->{DatumOffset}");
			          $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and 
			          						H_ARRAYATTRIBUTENAME='$h_ArrayAttributeName\_bit_offset'");
			      	  $h_arrayattid=$sth->fetchrow_array();
			      	  if (!defined $h_arrayattid) {
				          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                  '$h_ArrayAttributeName\_bit_offset',1,'short',1,'$datumParm->{DatumOffset}',null)");
				          if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	  } 
			      	  } else {
			      	  	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYATTRIBUTESTRINGVALUE='$datumParm->{DatumOffset}'
			      	  	  					, H_ARRAYATTRIBUTEDATATYPE='short'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						  if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
						  }
			      	  }
				  	  
				  	  logger("INFO    	Bit length: $bit_length");
				  	  $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and 
			          						H_ARRAYATTRIBUTENAME='$h_ArrayAttributeName\_bit_length'");
			      	  $h_arrayattid=$sth->fetchrow_array();
			      	  if (!defined $h_arrayattid) {                       
				          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                  '$h_ArrayAttributeName\_bit_length',1,'short',1,'$bit_length',null)");
				          if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	  }
			      	  } else {
			      	  	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYATTRIBUTESTRINGVALUE='$bit_length'
			      	  	  					, H_ARRAYATTRIBUTEDATATYPE='short'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						  if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
						  }
			      	  }
				  	  
				  	  logger("INFO    	Long name: $datumParm->{Description}");
				  	  my $description = $datumParm->{Description};
			          $description =~ s/'/''/g;
				  	  $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and 
			          						H_ARRAYATTRIBUTENAME='$h_ArrayAttributeName\_long_name'");
			      	  $h_arrayattid=$sth->fetchrow_array();
			      	  if (!defined $h_arrayattid) {                        
				          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                  '$h_ArrayAttributeName\_long_name',1,'string',1,'$description',null)");
				          if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	  }     
			      	  } else {
			      	  	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYATTRIBUTESTRINGVALUE='$description'
			      	  	  					, H_ARRAYATTRIBUTEDATATYPE='string'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						  if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
						  }
			      	  }                   
			    
			          # Process LegendEntry (if applicable)
			          my $qfMeanings = "";
			          my $qfValues = "";
			          my $qfVCount = 0;
			          foreach my $qfparm (@{$datumParm->{LegendEntry}}) {
			            $qfMeanings = $qfMeanings . $qfparm->{Name} . ",";
			            $qfValues = $qfValues . $qfparm->{Value} . ",";
			            $qfVCount += 1;
			          }
			          $qfValues = substr($qfValues,0,length($qfValues)-1);
			          $qfMeanings = substr($qfMeanings,0,length($qfMeanings)-1);
			          
			          logger("INFO    	Quality flag values: $qfValues");
			          $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and 
			          						H_ARRAYATTRIBUTENAME='$h_ArrayAttributeName\_values'");
			      	  $h_arrayattid=$sth->fetchrow_array();
			      	  if (!defined $h_arrayattid) {
				          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                  '$h_ArrayAttributeName\_values',1,'ubyte',$qfVCount,'$qfValues',',')");
				          if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	  }
			      	  } else {
			      	  	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYNUMATTRVALUES=$qfVCount, H_ARRAYATTRIBUTESTRINGVALUE='$qfValues'
			      	  	  					, H_ARRAYATTRIBUTEDATATYPE='ubyte'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						  if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
						  }
			      	  }
				  	  
				  	  logger("INFO    	Quality flag meanings: $qfMeanings");
				  	  $sth=runSQL($dbh,"select H_ARRAYATTRIBUTEID from HDF5_ARRAYATTRIBUTE where H_ARRAYID=$h_arrayid and 
			          						H_ARRAYATTRIBUTENAME='$h_ArrayAttributeName\_meanings'");
			      	  $h_arrayattid=$sth->fetchrow_array();
			      	  if (!defined $h_arrayattid) {                        
				          $success=$dbh->do("insert into HDF5_ARRAYATTRIBUTE values (nextval(S_HDF5_ARRAYATTRIBUTE),$h_arrayid,
				                                  '$h_ArrayAttributeName\_meanings',1,'string',$qfVCount,'$qfMeanings',',')");
				          if ($success != 1) {
								logger( "ERROR  Insert in HDF5_ARRAYATTRIBUTE table failed. Exiting." );
								$dbh->rollback();
								exit 1;
					  	  }
			      	  } else {
			      	  	  $success=$dbh->do("update HDF5_ARRAYATTRIBUTE set H_ARRAYNUMATTRVALUES=$qfVCount, H_ARRAYATTRIBUTESTRINGVALUE='$qfMeanings'
			      	  	  					, H_ARRAYATTRIBUTEDATATYPE='string'
				      						where H_ARRAYATTRIBUTEID=$h_arrayattid");
						  if ($success != 1) {
							logger( "ERROR  Update to HDF5_ARRAYATTRIBUTE table failed. Exiting." );
							$dbh->rollback();
							exit 1;
						  }
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

logger ("INFO Addition of Product Profile for \"$ref->{CollectionShortName}\" has been successful." );
my $rc = system("cp $opt_f /opt/data/nde/$opt_m\/xmls");
if ( $rc ) {
  logger( "ERROR Failed to copy xml file: $opt_f" );
} else {
  logger( "INFO $opt_f successfully copied");
}
logger( "Execution Completed.");
logger("----");                     

# assumed that queries will return a count(*)
sub runDupeCheck() {
 my ($dbh,$sql) = @_;
 my $sth = $dbh->prepare($sql);
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
