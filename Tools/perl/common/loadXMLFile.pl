#!/usr/bin/env perl

#
# name: loadXMLFile.pl
# purpose: Loads xml file to db, options include PLATFORM, PRODUCTGROUP, JOBPRIORITYCODE, JOBCLASSCODE,
#          DISPRIORITYCODE, DISCLASSCODE, DELIVERYNOTIFICATIONTYPE, INGESTINCOMINGDIRECTORY, NDE_SUPPORTFUNCTION,
#          DATADENIALFLAG, PLATFORMSENSOR, RESOURCES
# revised: 20120501 lhf, creation
#          20120525 lhf, cp file to /opt/data/nde/<mode>/xmls
#	       20120922 nls, add support for configuration registry
#          20121228 lhf, reposition logs
#          20130131 teh, require ndeUtiltiles.pl for pwd prompt
#          20130215 lhf, commented runSQL, uncommented runDupeCheck
#          20130220 lhf, removed unnecessary comment line, uncalled subroutine
#		   20160429 dcp, added ability to load new orbit types for platforms using the PLATFORMS.xml profile
#          20181106 jrh, modified to register extractors in NDE in the Cloud (nothing besides NDE_SUPPORTFUNCTIONs is guaranteed to work)
#

use DBI;
use XML::LibXML;
use Switch;
use Getopt::Std;
getopt('m:t:f:x:');

# Check for comand line args
if ( ! $opt_m || ! $opt_t || ! $opt_f || ! $opt_x ) {
        print "\nUsage: $0 -m <mode> -t <TableName> -f <xml Filename> -x <xsd Filename> [-i]\nNote: To initialize <TableName>, use the \"-i\" option\n\n";
        exit(1);
}

if (-r "./ndeUtilities.pl") {
  require "./ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}

chomp( $YMDHMS = `date +%Y%m%d_%H%M%S` );
my $logfile = "/opt/data/nde/" . $opt_m . "/logs/common/" . $opt_t . "_" . $YMDHMS . ".log";
open ( LOG, ">$logfile" ) or
  die "ERROR Error opening logfile $logfile, exiting...\n";

logger( "INFO $0 Execution started..." );

my $pw = promptForPassword("\n  Enter $opt_m password: ");
my $dbh = DBI->connect( "dbi:Pg:dbname=nde_dev1;host=nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com", lc($opt_m), $pw )
  or die "\nDatabase Connection Error: $DBI::errstr\n";
$dbh->begin_work;

my $doc = validateAgainstXSD( $opt_f, $opt_x, $opt_t );

switch ( $opt_t ) {
  case "DATADENIALFLAG" {
    my $rc = loadDATADENIALFLAG( $dbh, $doc );
  }
  case "DELIVERYNOTIFICATIONTYPE" {
    my $rc = loadDELIVERYNOTIFICATIONTYPE( $dbh, $doc );
  }
  case "DISJOBCLASSCODE" {
    my $rc = loadDISJOBCLASSCODE( $dbh, $doc );
  }
  case "DISJOBPRIORITYCODE" {
    my $rc = loadDISJOBPRIORITYCODE( $dbh, $doc );
  }
  case "GAZETTEER" {
    my $rc = loadGAZETTEER( $dbh, $doc );
  }
  case "INGESTINCOMINGDIRECTORY" {
    my $rc = loadINGESTINCOMINGDIRECTORY( $dbh, $doc );
  }
  case "JOBCLASSCODE" {
    my $rc = loadJOBCLASSCODE( $dbh, $doc );
  }
  case "JOBPRIORITYCODE" {
    my $rc = loadJOBPRIORITYCODE( $dbh, $doc );
  }
  case "NDE_SUPPORTFUNCTION" {
    my $rc = loadNDE_SUPPORTFUNCTION( $dbh, $doc );
  }
  case "NDE_USER" {
    my $rc = loadNDE_USER( $dbh, $doc );
  }
  case "PLATFORMS" {
    my $rc = loadPLATFORMS( $dbh, $doc );
  }
  case "PRODUCTGROUP" {
    my $rc = loadPRODUCTGROUP( $dbh, $doc );
  }
  case "CONFIGURATIONREGISTRY" {
    my $rc = loadCONFIGURATIONREGISTRY( $dbh, $doc );
  }
  else {
    logger(  "ERROR $opt_t not recognized" );
  }
}
if (! $rc ) {
  my $rcFromCopy = system("cp $opt_f /opt/data/nde/$opt_m\/xmls");
  if ( $rc ) {
    logger( "ERROR Failed to copy xml file: $opt_f" );
  } else {
    logger( "INFO $opt_f successfully copied");
  }
} else {
  logger( "ERROR non-zero return from subroutine" );
}

$dbh->commit;
logger( "INFO $0 Execution complete" );
close ( LOG );
print "\nLog file: $logfile\n";

sub validateAgainstXSD {
  my ( $xmlFile, $xsdFile, $tablename ) = @_;
  my $schema = XML::LibXML::Schema->new(location => $xsdFile);
  my $parser = XML::LibXML->new;
  my $doc = $parser->parse_file($xmlFile);

  eval {
      $schema->validate($doc);
  };

  if ($@) {
      print "ERROR $filename file FAILED validation: $@" if $@;
      exit( 1 );
  }

  return $doc;

}

sub loadDATADENIALFLAG {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "DATADENIALFLAG" ), if ( $opt_i );
  foreach my $ddfs ($doc->findnodes('/DATADENIALFLAGS/DATADENIALFLAG')) {
    my $ddf = $ddfs->findnodes('./DDFLAG');
    if ( ! runDupeCheck( $dbh, "select count(*) from DATADENIALFLAG" ) ) {
       if ( runSQL( $dbh, "insert into DATADENIALFLAG values ( $ddf )" ) ) {
          logger( "INFO DATADENIALFLAG: $ddf successfully added" );
       } else {
          logger( "ERROR Problem attempting to add DATADENIALFLAG: $ddf" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe DATADENIALFLAG: $ddf ignored" );
    }
  }
  return $rc;
}

sub loadDELIVERYNOTIFICATIONTYPE {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "DELIVERYNOTIFICATIONTYPE" ), if ( $opt_i );
  foreach my $dnts ($doc->findnodes('/DELIVERYNOTIFICATIONTYPES/DELIVERYNOTIFICATIONTYPE')) {
    my $dnn = $dnts->findnodes('./DELIVERYNOTIFICATIONNAME');
    if ( ! runDupeCheck( $dbh, "select count(*) from DELIVERYNOTIFICATIONTYPE
                                    where DELIVERYNOTIFICATIONNAME = \'$dnn\'" ) ) {
       if ( runSQL( $dbh, "insert into DELIVERYNOTIFICATIONTYPE values (
                            S_DELIVERYNOTIFICATIONTYPE.nextval, \'$dnn\' )" ) ) {
          logger( "INFO DELIVERYNOTIFICATIONTYPE: $dnn successfully added" );
       } else {
          logger( "ERROR Problem attempting to add DELIVERYNOTIFICATIONTYPE: $dnn" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe DELIVERYNOTIFICATIONTYPE: $dnn ignored" );
    }
  }
  return $rc;
}

sub loadDISJOBCLASSCODE {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "DISJOBCLASSCODE" ), if ( $opt_i );
  foreach my $dcs ($doc->findnodes('/DISJOBCLASSCODES/DISJOBCLASSCODE')) {
    my $dc = $dcs->findnodes('./DICLASS');
    my $dcd = $dcs->findnodes('./DICLASSDESCRIPTION');
    if ( ! runDupeCheck( $dbh, "select count(*) from DISJOBCLASSCODE
                                    where DICLASS = $dc" ) ) {
            if ( runSQL( $dbh, "insert into DISJOBCLASSCODE values ( $dc, \'$dcd\' )" ) ) {
          logger( "INFO DICLASS: $dc successfully added" );
       } else {
          logger( "ERROR Problem attempting to add DICLASS: $dc" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe DICLASS: $dc ignored" );
    }
  }
  return $rc;
}

sub loadDISJOBPRIORITYCODE {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "DISJOBPRIORITYCODE" ), if ( $opt_i );
  foreach my $dps ($doc->findnodes('/DISJOBPRIORITYCODES/DISJOBPRIORITYCODE')) {
    my $dp = $dps->findnodes('./DIPRIORITY');
    my $dpd = $dps->findnodes('./DIPRIORITYDESCRIPTION');
    if ( ! runDupeCheck( $dbh, "select count(*) from DISJOBPRIORITYCODE
                                    where DIPRIORITY = $dp" ) ) {
            if ( runSQL( $dbh, "insert into DISJOBPRIORITYCODE values ( $dp, \'$dpd\' )" ) ) {
          logger( "INFO DIPRIORITY: $dp successfully added" );
       } else {
          logger( "ERROR Problem attempting to add DIPRIORITY: $dp" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe DIPRIORITY: $dp ignored" );
    }
  }
  return $rc;
}

sub loadGAZETTEER {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "GAZETTEER" ), if ( $opt_i );
  foreach my $gzs ($doc->findnodes('/GAZETTEERS/GAZETTEER')) {
    my $gd = $gzs->findnodes('./GZDESIGNATION');
    my $gfn = $gzs->findnodes('./GZFEATURENAME');
    my $gls = $gzs->findnodes('./GZLOCATIONSPATIAL');
    my $glem = $gzs->findnodes('./GZLOCATIONELEVATIONMETERS');
    my $gst = $gzs->findnodes('./GZSOURCETYPE');
    if ( ! runDupeCheck( $dbh, "select count(*) from GAZETTEER
                                    where GZFEATURENAME = \'$gfn\'" ) ) {
       if ( runSQL( $dbh, "insert into GAZETTEER values ( S_GAZETTEER.nextval,
                                    \'$gd\', \'$gfn\', $gls, \'$glem\', \'$gst\' )" ) ) {
          logger( "INFO GAZETTEER: $gfn successfully added" );
       } else {
          logger( "ERROR Problem attempting to add GAZETTEER: $gfn" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe GAZETTEER: $gfn ignored" );
    }
  }
  return $rc;
}

sub loadINGESTINCOMINGDIRECTORY {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "INGESTINCOMINGDIRECTORY" ), if ( $opt_i );
  foreach my $iis ($doc->findnodes('/INGESTINCOMINGDIRECTORYS/INGESTINCOMINGDIRECTORY')) {
    my $idn = $iis->findnodes('./INGESTDIRECTORYNAME');
    if ( ! runDupeCheck( $dbh, "select count(*) from INGESTINCOMINGDIRECTORY
                                    where INGESTDIRECTORYNAME = \'$idn\'" ) ) {
       if ( runSQL( $dbh, "insert into INGESTINCOMINGDIRECTORY values (
                            S_INGESTINCOMINGDIRECTORY.nextval, \'$idn\' )" ) ) {
          logger( "INFO INGESTINCOMINGDIRECTORY: $idn successfully added" );
       } else {
          logger( "ERROR Problem attempting to add INGESTINCOMINGDIRECTORY: $idn" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe INGESTINCOMINGDIRECTORY: $idn ignored" );
    }
  }
  return $rc;
}

sub loadJOBCLASSCODE {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "JOBCLASSCODE" ), if ( $opt_i );
  foreach my $jcs ($doc->findnodes('/JOBCLASSCODES/JOBCLASSCODE')) {
    my $jc = $jcs->findnodes('./JOBCLASS');
    my $jcd = $jcs->findnodes('./JOBCLASSDESCRIPTION');
    if ( ! runDupeCheck( $dbh, "select count(*) from JOBCLASSCODE
                                    where JOBCLASS = $jc" ) ) {
       if ( runSQL( $dbh, "insert into JOBCLASSCODE values ( $jc, \'$jcd\' )" ) ) {
          logger( "INFO JOBCLASSCODE: $jc successfully added" );
       } else {
          logger( "ERROR Problem attempting to add JOBCLASSCODE: $jc" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe JOBCLASSCODE: $jc ignored" );
    }
  }
  return $rc;
}

sub loadJOBPRIORITYCODE {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "JOBPRIORITYCODE" ), if ( $opt_i );
  foreach my $jps ($doc->findnodes('/JOBPRIORITYCODES/JOBPRIORITYCODE')) {
    my $jp = $jps->findnodes('./JOBPRIORITY');
    my $jpd = $jps->findnodes('./JOBPRIORITYDESCRIPTION');
    if ( ! runDupeCheck( $dbh, "select count(*) from JOBPRIORITYCODE
                                    where JOBPRIORITY = $jp" ) ) {
       if ( runSQL( $dbh, "insert into JOBPRIORITYCODE values ( $jp, \'$jpd\' )" ) ) {
          logger( "INFO JOBPRIORITYCODE: $jp successfully added" );
       } else {
          logger( "ERROR Problem attempting to add JOBPRIORITYCODE: $jp." );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe JOBPRIORITYCODE: $jp ignored" );
    }
  }
  return $rc;
}

sub loadNDE_SUPPORTFUNCTION {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "NDE_SUPPORTFUNCTION" ), if ( $opt_i );
  foreach my $nsfs ($doc->findnodes('/NDE_SUPPORTFUNCTIONS/NDE_SUPPORTFUNCTION')) {
    my $nd = $nsfs->findnodes('./NSFDESCRIPTION');
    my $nt = $nsfs->findnodes('./NSFTYPE');
    my $npocn = $nsfs->findnodes('./NSFPATHORCLASSNAME');
    my $nmoen = $nsfs->findnodes('./NSFMETHODOREXECUTABLENAME');
    if ( ! runDupeCheck( $dbh, "select count(*) from NDE_SUPPORTFUNCTION
                                    where NSFTYPE = \'$nt\' and
                                          NSFPATHORCLASSNAME = \'$npocn\' and
                                          NSFMETHODOREXECUTABLENAME = \'$nmoen\' " ) ) {
       if ( runSQL( $dbh, "insert into NDE_SUPPORTFUNCTION values (
                            nextval('s_nde_supportfunction'), \'$nd\', \'$nt\', \'$npocn\',
                             \'$nmoen\'  )" ) ) {
          logger( "INFO NDE_SUPPORTFUNCTION: $nd successfully added" );
       } else {
          logger( "ERROR Problem attempting to add NDE_SUPPORTFUNCTION: $nd" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe NDE_SUPPORTFUNCTION: $nd ignored" );
    }
  }
  return $rc;
}

sub loadNDE_USER {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "NDE_USER" ), if ( $opt_i );
  foreach my $nu ($doc->findnodes('/NDE_USERS/NDE_USER')) {
    my $nupv = $nu->findnodes('./NDEUSERPRIVILEGES');
    my $nuid = $nu->findnodes('./NDEUSERIDENTIFIER');
    my $nupw = $nu->findnodes('./NDEUSERPASSWORD');
    my $plut = $nu->findnodes('./NDEUSER_PW_LASTUPDATETIME');
    if ( ! runDupeCheck( $dbh, "select count(*) from NDE_USER
                                    where NDEUSERPRIVILEGES = \'$nupv\' and
                                          NDEUSERIDENTIFIER = \'$nuid\'" ) ) {
       if ( runSQL( $dbh, "insert into NDE_USER values (
                            S_NDE_USER.nextval, '$nupv', '$nuid', '$nupw', '$plut')" ) ) {
          logger( "INFO NDE_USER: $nuid successfully added" );
       } else {
          logger( "ERROR Problem attempting to add NDE_USER: $nuid" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe NDE_USER: $nuid ignored" );
    }
  }
  return $rc;
}

sub loadPLATFORMS {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "PLATFORMSENSOR" ), if ( $opt_i );
  initTable( $dbh, "PLATFORM" ), if ( $opt_i );
  initTable( $dbh, "PLATFORMORBITTYPE" ), if ( $opt_i );
  foreach my $platforms ($doc->findnodes('/PLATFORMS/PLATFORM')) {
    my $pn = $platforms->findnodes('./PLATFORMNAME');
    my $pmn = $platforms->findnodes('./PLATFORMMISSIONNAME');
    if ( ! runDupeCheck( $dbh, "select count(*) from PLATFORM
                                    where PLATFORMNAME = \'$pn\'
                                     and PLATFORMMISSIONNAME = \'$pmn\'" ) ) {
       if ( runSQL( $dbh, "insert into PLATFORM values ( S_PLATFORM.nextval,
                                    \'$pn\', \'$pmn\' )" ) ) {
          logger( "INFO PLATFORM: $pn successfully added" );
          foreach my $pss ($platforms->findnodes('./PLATFORMSENSORS/PLATFORMSENSOR')) {
            my $ps = $pss->findnodes('./PLATSENSOR');
            my $pmos = $pss->findnodes('./PLATMINORBITSECONDS');
            my $sth = runSQL( $dbh, "select PLATFORMID from PLATFORM where PLATFORMNAME=\'$pn\'" );
            my $pfi = $sth->fetchrow_array();
            if ( $pfi > 0 ) {
              if ( ! runDupeCheck( $dbh, "select count(*) from PLATFORMSENSOR
                                              where PLATFORMID = \'$pfi\' and PLATSENSOR=\'$ps\'" ) ) {
                 if ( runSQL( $dbh, "insert into PLATFORMSENSOR values ( S_PLATFORMSENSOR.nextval,
                                              $pfi, \'$ps\', \'$pmos\' )" ) ) {
                    logger( "INFO PLATFORMSENSOR: $pn\/$ps successfully added." );
                 } else {
                    logger( "ERROR Problem attempting to add PLATFORMSENSOR: $pn\/$ps" );
                    $rc = 1;
                 }
              } else {
                logger( "WARNING Dupe PLATFORMSENSOR: $pn\/$ps ignored" );
              }
            } else {
              logger( "ERROR Platform Name: $pn not found, $ps ignored" );
              $rc = 1;
            }
          }
          foreach my $pots ($platforms->findnodes('./PLATFORMORBITTYPES/PLATFORMORBITTYPE')) {
            my $pon = $pots->findnodes('./PLORBITNAME');
            my $sth = runSQL( $dbh, "select PLATFORMID from PLATFORM where PLATFORMNAME=\'$pn\'" );
            my $pfi = $sth->fetchrow_array();
            if ( $pfi > 0 ) {
              if ( ! runDupeCheck( $dbh, "select count(*) from PLATFORMORBITTYPE where PLATFORMID = \'$pfi\' and PLORBITNAME=\'$pon\'" ) ) {
              	my $sth = runSQL( $dbh, "select PLORBITTYPEID from PLATFORMORBITTYPE where PLORBITNAME=\'$pon\'" );
              	my $potID = $sth->fetchrow_array();
              	if (defined($potID)) {
              		runSQL( $dbh, "insert into PLATFORMORBITTYPE values ( $pfi, $potID, \'$pon\' )");
              		logger( "INFO PLATFORMORBITTYPE: $pn\/$pon successfully added." );
              	} else {
              		runSQL( $dbh, "insert into PLATFORMORBITTYPE values ( $pfi, S_PLATFORMORBITTYPE.nextval, \'$pon\' )");
              		logger( "INFO PLATFORMORBITTYPE: $pn\/$pon successfully added." );
              	}
              } else {
                logger( "WARNING PLATFORMORBITTYPE: $pn\/$pon already exists.")
              }
            } else {
              logger( "ERROR Platform Name: $pn not found, $ps ignored" );
              $rc = 1;
            }
          }
       } else {
          logger( "ERROR Problem attempting to add PLATFORM: $pn." );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe PLATFORM: $pn ignored" );
      logger( "INFO Checking PLATFORMORBITTYPE");
      foreach my $pots ($platforms->findnodes('./PLATFORMORBITTYPES/PLATFORMORBITTYPE')) {
      	my $pon = $pots->findnodes('./PLORBITNAME');
      	my $sth = runSQL( $dbh, "select PLATFORMID from PLATFORM where PLATFORMNAME=\'$pn\'" );
        my $pfi = $sth->fetchrow_array();
        if ( ! runDupeCheck( $dbh, "select count(*) from PLATFORMORBITTYPE where PLATFORMID = \'$pfi\' and PLORBITNAME=\'$pon\'" ) ) {
        	my $sth = runSQL( $dbh, "select PLORBITTYPEID from PLATFORMORBITTYPE where PLORBITNAME=\'$pon\'" );
        	my $potID = $sth->fetchrow_array();
        	if (defined($potID)) {
              		runSQL( $dbh, "insert into PLATFORMORBITTYPE values ( $pfi, $potID, \'$pon\' )");
              		logger( "INFO PLATFORMORBITTYPE: $pn\/$pon successfully added." );
            } else {
              		runSQL( $dbh, "insert into PLATFORMORBITTYPE values ( $pfi, S_PLATFORMORBITTYPE.nextval, \'$pon\' )");
              		logger( "INFO PLATFORMORBITTYPE: $pn\/$pon successfully added." );
            }
      } else {
      	logger( "WARNING PLATFORMORBITTYPE: $pn\/$pon already exists.")
      }
    }
  }
  }
  return $rc;
}


sub loadPRODUCTGROUP {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "PRODUCTGROUP" ), if ( $opt_i );
  foreach my $productgroups ($doc->findnodes('/PRODUCTGROUPS/PRODUCTGROUP')) {
    my $pgn = $productgroups->findnodes('./PRODUCTGROUPNAME');
    if ( ! runDupeCheck( $dbh, "select count(*) from PRODUCTGROUP
                                    where PRODUCTGROUPNAME = \'$pgn\'" ) ) {
       if ( runSQL( $dbh, "insert into PRODUCTGROUP values ( S_PRODUCTGROUP.nextval,
                                    \'$pgn\' )" ) ) {
          logger( "INFO PRODUCTGROUP: $pgn successfully added" );
       } else {
          logger( "ERROR Problem attempting to add PRODUCTGROUP: $pgn" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe PRODUCTGROUP: $pgn ignored" );
    }
  }
  return $rc;
}

sub loadCONFIGURATIONREGISTRY {
  my ( $dbh, $doc ) = @_;
  my $rc = 0;
  initTable( $dbh, "CONFIGURATIONREGISTRY" ), if ( $opt_i );
  foreach my $cvs ($doc->findnodes('/CONFIGURATIONREGISTRYVALUES/CONFIGURATIONREGISTRYVALUE')) {
    my $cpn = $cvs->findnodes('./CFGPARAMETERNAME');
    my $cpv = $cvs->findnodes('./CFGPARAMETERVALUE');
    my $cpd = $cvs->findnodes('./CFGPARAMETERDESCRIPTION');
    my $cc  = $cvs->findnodes('./CFGCLASS');
    my $csc = $cvs->findnodes('./CFGSUBCLASS');

    if ( ! runDupeCheck( $dbh, "select count(*) from CONFIGURATIONREGISTRY 
                                    where CFGPARAMETERNAME = \'$cpn\'" ) ) {
            if ( runSQL( $dbh, "insert into CONFIGURATIONREGISTRY values ( S_CONFIGURATIONREGISTRY.nextval, 
                                                                           \'$cpn\',
                                                                           \'$cpv\',
                                                                           \'$cpd\',
                                                                           \'$cc\',
                                                                           \'$csc\',
                                                                           localtimestamp  )" ) ) {
          logger( "INFO CFGPARAMETERNAME: $cpn successfully added" );
       } else {
          logger( "ERROR Problem attempting to add CFGPARAMETERNAME: $cpn" );
          $rc = 1;
       }
    } else {
      logger( "WARNING Dupe CFGPARAMETERNAME: $cpn ignored" );
    }
  }
  return $rc;
}

sub initTable () {
  my ( $dbh, $tablename ) = @_;
  $dbh->do( "delete from $tablename")
   or die "\nProblem deleting $DBI::errstr\n";
  logger( "INFO Database table: $tablename initialized" );
}

# assumed that queries will return a count(*)
#in ndeUtilties.pl
#sub runSQL() {
#  my ( $dbh, $sql ) = @_;
#  my $sth = $dbh->prepare( $sql );
#  $sth->execute;
#  return $sth;
#}

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

exit;
