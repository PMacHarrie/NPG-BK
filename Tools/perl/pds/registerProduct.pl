#!/usr/bin/env perl

#
# name: registerProduct.pl
# revised: 20111220 lhf, add xsd validation, update capability
#          20090225 tjf make work w/ingestdirectoryname string
#          20090615 tjf changed productmetadataxml to productprofilelink, no xml type
#                     columns are handled by this script
#          20091028 lhf added PRODUCTGROUPNAME to xml and section for PRODUCTGROUPID
#          20091127 lhf update to include PRODUCTDATASOURCE information
#          20091208 lhf add structure row
#          20100511 lhf added PRODUCTCOVERAGEGAPINTERVAL_DS
#          2010l203 lhf allows null in PRODUCTCOVERAGEGAPINTERVAL_DS
#          20110111 htp update to include Ingest Process Step information
#          20110111 htp update to rollback DB inserts if one fails
#          20110408 lhf RENTETIONPERIODHR -> RETENTIONPERIODHR
#          20110422 htp removed hardcoded IPS flags and optional params
#          20110811 htp general refactoring, more prints, use $dbh->begin_work
#                        added warning for prod home dir
#          20120522 lhf build 4/5 updates
#          20120530 lhf PRODUCTPLATFORM_XREF info added
#          20121102 lhf correct for PRODUCTOBSTIMEPATTERN and PRODUCTIRMESSAGEEXTENSION
#          20121212 lhf add logs/pds
#          20130123 lhf validate PRODUCTSTATUS, default to NOP
#		   20131205 dcp modified to allow update of product quality summary table
#		   20141013 dcp, added platform mapping to product IDs
#	       20141118 teh remove product homedir=shortname test.  OBE with GVF_IP products especially.
#          20171120 teh add handling for new optional PRODUCTDISTFLAG field. ENTR-4319
#	20180308 pgm postgres

use XML::Simple;
use XML::LibXML;
use Data::Dumper;
use DBI;

use Getopt::Std;
getopt('m:f:');

# Check for comand line args
if ( ! $opt_m || ! $opt_f) {
        print "\nUsage $0 -m <mode> -f <XML Filename>\n\n";
        exit(1);
}

chomp( $YMDHMS = `date +%Y%m%d_%H%M%S` );
my $rfn = reverse( $opt_f );
if ( index(reverse($opt_f),"/") > 0) {
  $rfn = substr(reverse($opt_f),0,index(reverse($opt_f),"/"));
}

my $logfile = "/opt/data/nde/" . $opt_m . "/logs/pds/PRODUCTDESCRIPTION_" . reverse($rfn) . "_" . $YMDHMS . ".log";
open ( LOG, ">$logfile" ) or
  die "ERROR Error opening logfile, ($logfile) exiting...\n";

logger( "INFO Execution started..." );

my $schema = XML::LibXML::Schema->new(location => "/opt/data/nde/$opt_m\/xsds/PRODUCTDESCRIPTION.xsd");
my $parser = XML::LibXML->new;
my $doc = $parser->parse_file( $opt_f );

eval {
    $schema->validate($doc);
};

if ($@) {
    logger( "ERROR $opt_f file FAILED validation: $@" ) if $@;
    exit( 1 );
}
if (-r "../common/ndeUtilities.pl") {
  require "../common/ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}
my $pw = promptForPassword("\n  Enter $opt_m password: ");

# postgres is case-sensitive convert $opt_m to lowercase 

my $dbh = DBI->connect( "dbi:Pg:dbname=nde_dev1;host=nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com", lc($opt_m), 'nde' )
          or die "\nDatabase Connection Error: $DBI::errstr\n";
#$dbh->{LongReadLen}=64000;
$dbh->begin_work;
my $update = "n";

my %ref;
my $ref = XMLin( $opt_f, forcearray => ['ProductQualitySummary','IngestProcessStep','Platform'] );

#logger( Dumper( $ref ) );


### Check for existing product
my $psn = $ref->{PRODUCTSHORTNAME};

my $platformName = $ref->{Platforms}{Platform}[0]->{Platformname};
logger( "INFO $platformName" );
$sth = runSQL( $dbh, "select PLATFORMID from PLATFORM where PLATFORMNAME = '$platformName'" );
my ( $platformID ) = $sth->fetchrow_array();

if (!defined($platformID)) {
	$sth = runSQL($dbh, "select PRODUCTID from PRODUCTDESCRIPTION where PRODUCTSHORTNAME = '$psn'" );
} else {
	$sth = runSQL($dbh, "select pd.PRODUCTID from PRODUCTDESCRIPTION pd, PRODUCTPLATFORM_XREF prx where PRODUCTSHORTNAME = '$psn'
							and pd.PRODUCTID = prx.PRODUCTID and PLATFORMID = $platformID" );
}
my ($productID) = eval { $sth->fetchrow_array()};
if ($productID ne "" ) {
  print "WARNING Product $psn already exists (ID: $productID), continue with update? (y/n): ";
  chomp($update = <STDIN>);
  if (($update ne 'y') && ($update ne 'n')) {
		logger( "ERROR  You did not enter a \"y\" or a \"n\". Exiting...");
		$dbh->rollback();
		exit 1;
	} elsif ($update eq "n") {
		logger( "INFO  Not updating product $psn and exiting...");
		$dbh->rollback();
		exit 1;
	}
}

## Check product home dir
#if( $ref->{PRODUCTHOMEDIRECTORY} ne ("products/" . $psn) ){
#        print "WARNING Product home dir ($ref->{PRODUCTHOMEDIRECTORY}) does not match product short name, continue? (y/n) :";
#        if( <> eq "n\n"){       print "\nExiting\n\n"; $dbh->rollback; exit 1; }
#}

# Validate status
my $productStatus = $ref->{PRODUCTSTATUS};
$productStatus = "NOP", if( $productStatus eq "" );
if( ! grep(/$productStatus/, ('OP','TEST','NOP') ) ){
        print "WARNING Product Status value invalid ($productStatus), valid values are OP, TEST, and NOP\n";
        print "\nExiting\n\n"; $dbh->rollback; exit 1; }
        
# Validate archive
my $productArchive = $ref->{PRODUCTARCHIVE};
$productArchive = "0", if ( $productArchive eq "" );
if( ! grep(/$productArchive/, ('0','1') ) ){
        print "WARNING Product Archive value invalid ($productArchive), valid values are 0 or 1\n";
        print "\nExiting\n\n"; $dbh->rollback; exit 1; }

# Get some required values
print "  Looking up IncomingDirID and ProductGroupID ...";

my $sql= "select INGESTINCOMINGDIRECTORYID from INGESTINCOMINGDIRECTORY where INGESTDIRECTORYNAME = '$ref->{INGESTDIRECTORYNAME}'" ;
$sth = runSQL($dbh, $sql );
$sth->execute;
my ( $incomingDirID ) = $sth->fetchrow_array();
if ($incomingDirID eq "" ) {
        print "    could not find ID for Ingest Incoming Directory \"$ref->{INGESTDIRECTORYNAME}\"\n";
        $dbh->rollback; exit( 1 );
}
my $productGroupID = "null";
if ( $ref->{PRODUCTGROUPNAME} ne "null" && $ref->{PRODUCTGROUPNAME} ne "" )  {
  $sth = runSQL($dbh, "select PRODUCTGROUPID from PRODUCTGROUP where PRODUCTGROUPNAME = '$ref->{PRODUCTGROUPNAME}'");
  ( $productGroupID ) = $sth->fetchrow_array();
}
print " done\n";

my $productDistFlag = "TRUE";
if ( $ref->{PRODUCTDISTFLAG} ne "" ) {
  	$productDistFlag = uc $ref->{PRODUCTDISTFLAG};
  	#There are only 2 allowable values now, after filtering out null and uppercasing: TRUE or FALSE 
  	#Enforce:
  	if ($productDistFlag ne "FALSE" ) {$productDistFlag = "TRUE";}
}


my $pcgi_ds;
if ( $ref->{PRODUCTCOVERAGEGAPINTERVAL_DS} eq "" || $ref->{PRODUCTCOVERAGEGAPINTERVAL_DS} eq "null" ) {
  $pcgi_ds = "interval '0' second";
  }
else {
  $pcgi_ds = $ref->{PRODUCTCOVERAGEGAPINTERVAL_DS};
}


# Register product
# if insert, then store stub (psn may not be updated)
if ( $update eq "n" ) {
  logger( "INFO New product: $psn ..." );
  $sql = "insert into ProductDescription (PRODUCTID, PRODUCTSHORTNAME)
           values (nextval('S_PRODUCTDESCRIPTION'), '$psn')";
  $sth = runSQL( $dbh, $sql );

  $sth = runSQL($dbh, "select PRODUCTID from PRODUCTDESCRIPTION where PRODUCTSHORTNAME = '$psn'
								and PRODUCTFILENAMEPATTERN IS NULL" );
  $productID = eval { $sth->fetchrow_array()};
}
logger( "INFO Completing registration of product: $psn (ProductId: $productID) ..." );
$sql = "update PRODUCTDESCRIPTION set PRODUCTSHORTNAME='$psn', PRODUCTLONGNAME='$ref->{PRODUCTLONGNAME}',
         PRODUCTDESCRIPTION='$ref->{PRODUCTDESCRIPTION}', PRODUCTMETADATALINK='$ref->{PRODUCTMETADATALINK}',
          PRODUCTPROFILELINK='$ref->{PRODUCTPROFILELINK}', PRODUCTTYPE='$ref->{PRODUCTTYPE}', PRODUCTFILENAMEPREFIX='$ref->{PRODUCTFILENAMEPREFIX}',
           PRODUCTIDPSMNEMONIC='$ref->{PRODUCTIDPSMNEMONIC}', PRODUCTAVAILABILITYDATE='$ref->{PRODUCTAVAILABILITYDATE}',
            PRODUCTCIPPRIORITY='$ref->{PRODUCTCIPPRIORITY}', PRODUCTSTATUS='$productStatus',
             PRODUCTFILENAMEPATTERN='$ref->{PRODUCTFILENAMEPATTERN}', PRODUCTHOMEDIRECTORY='$ref->{PRODUCTHOMEDIRECTORY}',
              PRODUCTIDPSRETRANSMITLIMIT='$ref->{PRODUCTIDPSRETRANSMITLIMIT}', PRODUCTAREA='$ref->{PRODUCTAREA}',
               PRODUCTESTAVGFILESIZE='$ref->{PRODUCTESTAVGFILESIZE}', PRODUCTARCHIVE=$productArchive,
                 PRODUCTRETENTIONPERIODHR='$ref->{PRODUCTRETENTIONPERIODHR}', PRODUCTHORIZONTALRESOLUTION='$ref->{PRODUCTHORIZONTALRESOLUTION}',
                  PRODUCTVERTICALRESOLUTION='$ref->{PRODUCTVERTICALRESOLUTION}', PRODUCTFILEFORMAT='$ref->{PRODUCTFILEFORMAT}',
                   PRODUCT_TIME_COVERAGE='$ref->{PRODUCT_TIME_COVERAGE}', PRODUCT_MAP_PROJECTION='$ref->{PRODUCT_MAP_PROJECTION}',
                    PRODUCTFILEMETADATAEXTERNAL=$ref->{PRODUCTFILEMETADATAEXTERNAL}, PRODUCTFILENAMEEXTENSION='$ref->{PRODUCTFILENAMEEXTENSION}',
                     PRODUCTFILENAMEMETAEXTENSION='$ref->{PRODUCTFILENAMEMETAEXTENSION}', PRODUCTSUBTYPE='$ref->{PRODUCTSUBTYPE}',
                      INGESTINCOMINGDIRECTORYID=$incomingDirID, PRODUCTGROUPID=$productGroupID, PRODUCTCOVERAGEGAPINTERVAL_DS=$pcgi_ds,
                       PRODUCTOBSTIMEPATTERN='$ref->{PRODUCTOBSTIMEPATTERN}', PRODUCTIRMESSAGEEXTENSION='$ref->{PRODUCTIRMESSAGEEXTENSION}',
                        PRODUCTDISTFLAG='$productDistFlag'
         where PRODUCTID = $productID";

$sth = runSQL( $dbh, $sql );

### Insert into tailoring tables
if ( $update eq "n" ) {
  if( $ref->{PRODUCTFILENAMEEXTENSION} eq "h5"){
    $dbh->do( "insert into HDF5_STRUCTURE values ($productID) ");
  }
  elsif( $ref->{PRODUCTFILENAMEEXTENSION} eq "nc" ){
    $dbh->do( "insert into NETCDF4_STRUCTURE values ($productID) ");
  }
}

### Do Product Quality Summarys
#$dbh->do( "delete from PRODUCTQUALITYSUMMARY where productID=$productID" ), if ( $update eq "y" );
logger( "INFO Adding product quality summaries ..." );
my $pqsCount = 0;
for my $parm (@{$ref->{ProductQualitySummarys}{ProductQualitySummary}}) {
        logger( "INFO $parm->{PRODUCTQUALITYSUMMARYNAME} " );
        if ($update eq "n") {
        	my $sql = "insert into PRODUCTQUALITYSUMMARY
                                values ($productID, '$parm->{PRODUCTQUALITYSUMMARYNAME}',
                                '$parm->{PRODUCTQUALITYSUMMARYTYPE}', '$parm->{PRODUCTQUALITYDESCRIPTION}') ";
            $pqsCount += $dbh->do($sql);
            logger( "Added $parm->{PRODUCTQUALITYSUMMARYNAME} " );
        } else {
        	my $sql = "select PRODUCTQUALITYSUMMARYNAME from PRODUCTQUALITYSUMMARY where PRODUCTID=$productID
        				and PRODUCTQUALITYSUMMARYNAME='$parm->{PRODUCTQUALITYSUMMARYNAME}'";
        	$sth = runSQL( $dbh, $sql );   	
			my $summary=$sth->fetchrow_array();
			if (!defined($summary)){
				my $sql = "insert into PRODUCTQUALITYSUMMARY
                                values ($productID, '$parm->{PRODUCTQUALITYSUMMARYNAME}',
                                '$parm->{PRODUCTQUALITYSUMMARYTYPE}', '$parm->{PRODUCTQUALITYDESCRIPTION}') ";
            	$pqsCount += $dbh->do($sql);
            	logger( "Added $parm->{PRODUCTQUALITYSUMMARYNAME} " );
			}
        }
}


### Do Ingest Process Steps
$dbh->do( "delete from ingestprocessstep where productID=$productID" ), if ( $update eq "y" );
logger( "INFO Adding ingest process steps ..." );
my $ipsCount = 0;
for my $ips (@{$ref->{IngestProcessSteps}{IngestProcessStep}}) {

        # Check to see if this NSF exists
        my $nsfname = $ips->{NSFDESCRIPTION};
        $sth = runSQL( $dbh, "select NSFID from NDE_SUPPORTFUNCTION where NSFDESCRIPTION='$nsfname'") ;
        my $nsfid = eval { $sth->fetchrow_array()};
        if ($nsfid eq "" ) {
           logger( "ERROR NSFDescription $nsfname does not exist. Exiting..." );
           $dbh->rollback; exit(1);
        }

        # get ips options
        $optparam = $ips->{IPSOPTIONALPARAMETERS};
        $ipsfails = $ips->{IPSFAILSINGEST};
        $ipsenable = $ips->{IPSENABLE};
        $ipsretran = $ips->{IPSDORETRANSMIT};
        if( $ipsfails eq "" ){ $dbh->rollback; logger( "ERROR Missing \"IPSFAILSINGEST\" flag for IPS: $nsfname, please add!" ); exit(1);}
        if( $ipsenable eq "" ){ $dbh->rollback; logger( "ERROR Missing \"IPSENABLE\" flag for IPS: $nsfname, please add!" ); exit(1);}
        if( $ipsretran eq "" ){ $dbh->rollback; logger( "ERROR Missing \"IPSDORETRANSMIT\" flag for IPS: $nsfname, please add!" ); exit(1);}

        $sql="insert into INGESTPROCESSSTEP (PRODUCTID,NSFID, IPSOPTIONALPARAMETERS,IPSFAILSINGEST,IPSENABLE,IPSDORETRANSMIT)
                   values ($productID, $nsfid, '$optparam', '$ipsfails', '$ipsenable', '$ipsretran') ";
        $ipsCount += $dbh->do($sql);
        logger( "INFO $nsfname" );
                if( $optparam ne "" ){ logger( "INFO Optional Param: $optparam" ); }
}


### Do Product Data Source, if applicable
if ( $ref->{HOSTNAME} ne "" ) {
        logger( "INFO Adding external product data source ..." );
        $sth = runSQL( $dbh, "select HOSTID from EXTERNALDATAHOST where HOSTNAME = '$ref->{HOSTNAME}'" );
        my ( $hostID ) = $sth->fetchrow_array();
        if( $hostID eq ""){
                logger( "ERROR External data host \"$ref->{HOSTNAME}\" not found in the database. Exiting..." );
                $dbh->rollback; exit 1;
        }
        $dbh->do( "delete from PRODUCTDATASOURCE where productID=$productID and HOSTID=$hostID" ), if ( $update eq "y" );
        my $sql = "insert into PRODUCTDATASOURCE
                                        (PRODUCTID, HOSTID, PRODUCTPROVIDERDIRECTORY, PRODUCTPOLLINGFREQUENCY, PRODUCTPROVIDERPOLLINGRETRIES )
                                values ( $productID, $hostID, '$ref->{PRODUCTPROVIDERDIRECTORY}',
                                        $ref->{PRODUCTPOLLINGFREQUENCY}, $ref->{PRODUCTPROVIDERPOLLINGRETRIES} ) ";
        $sth = $dbh->do($sql);
        logger( "INFO host: $ref->{HOSTNAME}" );
}
else{
        logger( "INFO No external product data source found, skipping" );
}

### Do Platforms
$dbh->do( "delete from PRODUCTPLATFORM_XREF where productID=$productID" ), if ( $update eq "y" );
logger( "INFO Adding product platform cross-reference(s) ..." );
for my $parm (@{$ref->{Platforms}{Platform}}) {
        logger( "INFO $parm->{Platformname} " );
        $sth = runSQL( $dbh, "select PLATFORMID from PLATFORM where PLATFORMNAME = '$parm->{Platformname}'" );
        my ( $platformID ) = $sth->fetchrow_array();
        if ( defined($platformID) ){
          my $sql = "insert into PRODUCTPLATFORM_XREF values ( $platformID, $productID ) ";
          $sth = $dbh->do($sql);
        } else {
          logger( "WARN No Platformname: $parm->{Platformname} defined, not found in db." );
        }
}

$dbh->commit;


my $action = "added";
$action = "updated", if ( $update eq "y" );
logger( "INFO $psn $action successfully with $pqsCount Quality Flags and $ipsCount Ingest Process Step(s)" );

my $rc = system("cp $opt_f /opt/data/nde/$opt_m\/xmls");
if ( $rc ) {
  logger( "ERROR Failed to copy xml file: $opt_f" );
} else {
  logger( "INFO $opt_f successfully copied");
}

logger( "INFO Execution complete." );

sub logger() {
  my ( $msg ) = @_;
  print "$msg\n";
  chomp( $HMS = `date +%H:%M:%S,%N` );
  print LOG substr($HMS,0,12) . " $msg\n";

}

exit;
