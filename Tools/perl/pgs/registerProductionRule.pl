#!/usr/bin/env perl

#
# name: registerProductionRule.pl
#
# revised: 20090303 lhf, translate productFileHandle to PRISLUN in PRINPUTSPEC
#          20090310 lhf, update to work with prinputspec and proutputspec productfilehandle columns,
#                        commented a lot of print statements
#          20090417 tc,  update to work with mode NDE_B2P2
#          20090429 tjf, update added prSize and prPriority to productionRule
#          20090626 lhf, full verification of all columns for Prototype 3
#          20090811 lhf, updated to work with proto4
#          20091214 lhf, correct problem with TailoringSection transformation of xml
#          20091217 lhf, tailoring section null
#          20100127 lhf, add GAZETTEER lookup
#          20100205 lhf, correct messages, implement sqlCommandStack, disallow duplicate PRRULENAMEs
#          20100318 lhf, add PLATFORMORBITTYPE lookup
#          20110314 lhf, CR190 registerProductionRule doesn't appear to load PLORBITTYPEID correctly
#          20110504 lhf, -s --> -m, add PRISFILEHANDLENUMBERING
#          20110807 htp, general refactoring, added prints, added @MODE@ replacement for prParameters
#                                                added use of $dbh->begin_work, added measure/dim_group verification
#          20111227 lhf, add xsd validation NDE-603
#          20120530 lhf, correct logfile location NDE-603
#          20120626 dcp, changed $prdataselectionxml to array of CLOB strings so that XML Tailoring
#                                        can insert more than 4000 character strings
#          20120810 dcp, added GranuleExact rule type
#          20121023 lhf, add prNotifyOpsSeconds
#          20121113 lhf, incorporate NDE-551, NDE-657
#          20121212 lhf, add logs/pgs
#          20121218 awc, added check on input/output products (registered & assigned to algorithm)
#          20130213 lhf, NDE-896 (validate GZFEATURENAME), add @SANPATH@ replacement for prParameters
#		   20130605 dcp, added ability to update tables; some refactoring; error handling 
#		   20140225 dcp, added ability to update production rule type (PRRULTYPE)
#		   20140326 dcp, check for duplication of parameters, added capability to update the pr input specificaiton table
#          20140827 teh, add PRACTIVEFLAG_CBU|NSOF - ignore xml and set both to 0.  old just "PRACTIVEFLAG" is replaced
#		   20141013 dcp, added platform mapping to product IDs (prInputSpec)
#		   20151119 dcp, added ability to update the PROUTPUTSPEC with new products to replace old ones (e.g. new tailored version#)
#
use XML::Simple;
use XML::LibXML;
use Data::Dumper;

use Getopt::Std;
getopt('m:f:');


#
# Check for command line args
#
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
my $logfile = "/opt/data/nde/" . $opt_m . "/logs/pgs/PRODUCTIONRULE_" . reverse($rfn) . "_" . $YMDHMS . ".log";

open ( LOG, ">$logfile" ) or
  die "ERROR Error opening logfile $logfile, exiting...\n";

logger(  "INFO Execution started..." );

my $schema = XML::LibXML::Schema->new(location => "/opt/data/nde/$opt_m\/xsds/PRODUCTIONRULE.xsd");
my $parser = XML::LibXML->new;
my $doc = $parser->parse_file( $opt_f );

eval {
    $schema->validate($doc);
};

if ($@) {
    print "$opt_f file FAILED validation: $@" if $@;
    exit( 1 );
}

# Connect to Oracle
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

my %ref;
my $ref = XMLin($opt_f, forcearray => [ 'PRParameter', 'PRInputSpec', 'PRInputProduct', 'PROutputSpec', 'measure' ]);
#print Dumper($ref);



# Check for algorithm and version
logger( "INFO Checking algorithm and version ..." );
my $algoName = $ref->{Algorithm}{algorithmName};
my $algoVer  = $ref->{Algorithm}{algorithmVersion};
my $sth = runSQL( $dbh, "select ALGORITHMID from ALGORITHM where ALGORITHMNAME = '$algoName' and ALGORITHMVERSION='$algoVer'");
my ($algoID) = $sth->fetchrow_array();
if ($algoID eq "" ) {
        logger( "ERROR  $algoName v$algoVer not found" );
        $dbh->rollback(); 
        exit 1;
} else {
        logger( "INFO  Algorithm: $algoName v$algoVer verified (ID: $algoID)" );
}

# 20130603 allow duplicate if user wants to update the production rule
logger( "INFO Checking existing rules for $algoName v$algoVer..." );
my $update="n"; #initialize the update flag
if ( !runDupeCheck($dbh,"select count(*) from PRODUCTIONRULE where PRRULENAME = '$ref->{prRuleName}'") ) {
	logger( "INFO  No existing rule with name \"$ref->{prRuleName}\"" );
} else {
	logger( "WARN  Duplicate production rule for \"$ref->{prRuleName}\", continue with update (y/n)?" );
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
}

###### Verify various rule parameters exist and are correct  ###########
# Check rule type
logger( "INFO Verifying production rule entries are valid..." );
logger( "INFO  Checking Rule Type..." );
my %prRuleTypes = ( Granule => 1, Temporal => 1, Orbital => 1, GranuleExact =>1);
if ( ! $prRuleTypes{$ref->{prRuleType}} ){
        logger( "ERROR   Invalid Rule Type: $ref->{prRuleType}, valid types are Granule, GranuleExact, Temporal, or Orbital. Exiting..." );
        $dbh->rollback(); 
        exit 1;
}
logger( "INFO   Rule Type: $ref->{prRuleType}" );


# Check Job Class
logger( "INFO  Checking Job Class..." );
$sth = runSQL( $dbh, "select JOBCLASS from JOBCLASSCODE where JOBCLASSDESCRIPTION = '$ref->{jobClass}'");
my ($jobClass) = $sth->fetchrow_array();
if ($jobClass eq "" ) {
        logger( "ERROR   Job class: $ref->{jobClass} is not valid. Exiting..." );
        $dbh->rollback(); 
        exit 1;
}
logger( "INFO   Job Class: $ref->{jobClass}" );


# Check Job Priority
logger( "INFO  Checking Job Priority..." );
$sth = runSQL( $dbh, "select JOBPRIORITY from JOBPRIORITYCODE where JOBPRIORITYDESCRIPTION = '$ref->{jobPriority}'");
my $jobPriority = $sth->fetchrow_array();
if ($jobPriority eq "" ) {
        logger( "ERROR   Job priority: $ref->{jobPriority} is not valid. Exiting..." );
        $dbh->rollback(); 
        exit 1;
}
logger( "INFO   Job Priority: $ref->{jobPriority}" );

# Check Start Boundary Time
logger( "INFO  Checking Start Boundary Time if temporal rule (null if not)..." );
my $sbt = "null";
if ( $ref->{prStartBoundaryTime} ne "null" ) {
        $sbt = "to_timestamp('$ref->{prStartBoundaryTime}','yyyy-mm-dd hh24:mi:ss.ff')";
}
logger( "INFO   Start Boundary Time: $ref->{prStartBoundaryTime}" );

# Check Product Coverage Interval
logger( "INFO  Checking Product Coverage Interval if temporal rule (null if not)..." );
$pcids = "null";
if ( $ref->{prProductCoverageInterval_DS} ne "" ) {
        $pcids = $ref->{prProductCoverageInterval_DS};
}
logger( "INFO   Product Coverage Interval: $ref->{prProductCoverageInterval_DS}" );

#Check PRACTIVEFLAG (should always be set to zero when registering)
#Just make them both (PRACTIVEFLAG_NSOF and PRACTIVEFLAG_CBU) 0.  Period.
#logger( "INFO  Checking the PR Active Flag...");
#if ( ($ref->{prActiveFlag} ne '0') && ($ref->{prActiveFlag} ne '1') ) {
#	logger( "ERROR   Production Rule Active Flag: $ref->{prActiveFlag} is not valid (must be 0 or 1). Exiting..." );
#    $dbh->rollback(); 
#    exit 1;
#} elsif ( $ref->{prActiveFlag} eq 1 ) {
#	logger( "WARN   PR Active Flag is activated (set to 1). Is this intended (y/n)?");
#	chomp($response = <STDIN>);
#	if (($response ne 'y') && ($response ne 'n')) {
#		logger( "ERROR   You did not enter a \"y\" or a \"n\". Exiting...");
#		$dbh->rollback();
#		exit 1;
#	} elsif ($response eq "n") {
#		$ref->{prActiveFlag}=0;
#		logger("WARN   Recommend updating XML to deactivate production rule (set active flag to 0). Continuing with rule deactivated...");
#	}
#}
#logger("INFO   PR Active Flag: $ref->{prActiveFlag}");

# Check PrNotifyOpsSeconds
#logger( "INFO Checking PrNotifyOpsSeconds..." );
my $prNotifyOpsSeconds = "null";

# Added JRH, 6/26/2018 for NDE-in-the-Cloud
logger ( "INFO Hard-coding PrNotifyOpsSeconds to 0..." );
$ref->{prNotifyOpsSeconds} = '0';

# GAZETTEER validation NDE-896
logger( "INFO  Checking for gazetteer..." );
if ( ($ref->{gzFeatureName} eq "") || ($ref->{gzFeatureName} eq "null" )) {
  $gzID = "null";
} else {
  $sth = runSQL( $dbh, "select GZID from GAZETTEER where GZFEATURENAME = '$ref->{gzFeatureName}'");
  $gzID = $sth->fetchrow_array();
  if ($gzID eq "" ) {
        logger( "ERROR   Gazetteer: \"$ref->{gzFeatureName}\" is not a valid entry. Exiting..." );
        $dbh->rollback(); 
        exit 1;
  }
}
logger( "INFO   Gazetteer: $ref->{gzFeatureName}" );

# PLATFORMORBITTYPE lookup 20110314
logger( "INFO  Checking platform orbit type ..." );
my $plOrbitName = $ref->{plOrbitName};
my $plOrbitTypeId = "null";
if ( $plOrbitName ne '' && $plOrbitName ne 'null' ){
        $sth = runSQL( $dbh, "select PLORBITTYPEID from PLATFORMORBITTYPE where PLORBITNAME = '$plOrbitName'");
        $plOrbitTypeId = $sth->fetchrow_array();
       if ( ! $plOrbitTypeId ){
	        logger( "ERROR   Orbit name: $plOrbitName is not a valid entry. Exiting..." );
	        $dbh->rollback(); 
	        exit 1;
        }
        logger( "INFO   Orbit name: $plOrbitName" );
} else {
        if ( $ref->{prRuleType} eq 'Orbital' ){
                logger( "ERROR   plOrbitName is required for orbital rule. Exiting..." );
                $dbh->rollback(); 
                exit 1;
        }
        logger( "INFO   No orbit type defined" );
}


# get Tailoring xml in a string 20091214
# verify measure and dimension names
logger( "INFO  Checking tailoring section ..." );
my $tailoring = $ref->{TailoringSection};
my @prdataselectionxml;
if ( defined($tailoring) ){

        logger( "INFO  Found, verifying measure and dimensions ..." );
        for my $measureName ( keys %{$ref->{TailoringSection}{dss}{select}{measure}} ){
                my $dimListName = $ref->{TailoringSection}{dss}{select}{measure}{$measureName}{from};
                my $sth = runSQL($dbh, "select count(*) from ENTERPRISEMEASURE em, ENTERPRISEDIMENSIONLIST edl
                                where em.E_DIMENSIONLISTID = edl.E_DIMENSIONLISTID
                                and em.MEASURENAME = '$measureName' and edl.E_DIMENSIONLISTNAME = '$dimListName'" );
                my $success = $sth->fetchrow_array();
                if( $success != 1 ){
                        logger( "ERROR   Measure: $measureName | Dimension Group: $dimListName combination not found. Exiting..." );
                        $dbh->rollback(); 
                        exit 1;
                } else {
                	logger( "INFO   Tailoring measure: $measureName dimension group: $dimListName" );
                }
        }

        logger( "INFO  Complete, saving as XML string..." );
        open( INFILE, "$opt_f" ) or die "Can't open xml file: $opt_f\n";
        my @lines = <INFILE>;
        close ( INFILE );
        my $collectxml = 0;
        foreach my $line ( @lines ) {
          chomp( $line );
          if ( $collectxml ) {
            if ( $line =~ /<\/TailoringSection\>/ < 1 ) {
              push(@prdataselectionxml, "$line ");
            }
          }
          $collectxml = 1, if ( $line =~ /<TailoringSection\>/ );
          $collectxml = 0, if ( $line =~ /<\/TailoringSection\>/ );
        }
        my $size = @prdataselectionxml;
        $prdataselectionxml[$size-1] =~ s/\n.*$/)/;
        logger( "INFO  Completed XML string" );

} else {
	logger( "INFO  No tailoring section defined" );
}



#Insert production rule
if ( $update eq "n" ) { #New production rule
	logger( "INFO Inserting new production rule: \"$ref->{prRuleName}\"" );	
	$sql="insert into PRODUCTIONRULE (
	         PRID, MOVCODE, WEDID, JOBPRIORITY, ALGORITHMID,
	          GZID, JOBCLASS, PRRULENAME, PRRULETYPE, PRACTIVEFLAG_NSOF, PRACTIVEFLAG_CBU,
	           PRTEMPORARYSPACEMB, PRNOTIFYOPSSECONDS, PRESTIMATEDRAM_MB, PRESTIMATEDCPU,
	            PRPRODUCTCOVERAGEINTERVAL_DS, PRPRODUCTCOVERAGEINTERVAL_YM,
	             PRSTARTBOUNDARYTIME, PRRUNINTERVAL_DS, PRRUNINTERVAL_YM,
	              PRWEATHEREVENTDISTANCEKM, PRORBITSTARTBOUNDARY, PRPRODUCTORBITINTERVAL,
	               PRWAITFORINPUTINTERVAL_DS, PLORBITTYPEID, PRDATASELECTIONXML)
	        values (nextval('s_PRODUCTIONRULE'), null, null, $jobPriority, $algoID,
	                $gzID, $jobClass, '$ref->{prRuleName}', '$ref->{prRuleType}', 0, 0,
	                 '$ref->{prTemporarySpaceMB}', '-1', '$ref->{prEstimatedRAM_MB}', '$ref->{prEstimatedCPU}',
	                  $pcids, null,
	                   $sbt, $ref->{prRunInterval_DS}, null,
	                    null, null, null,
	                     $ref->{prWaitForInputInterval}, $plOrbitTypeId, ";
	$sql = $sql . "' @prdataselectionxml ' )", if ( @prdataselectionxml );
	$sql = $sql . "null )", if (! @prdataselectionxml );
	print "$sql\n";
	my $success = $dbh->do($sql);	
	if ($success != 1){
	    logger( "ERROR Insert in PRODUCTIONRULE table failed. Check Production Rule definition file." );
	    $dbh->rollback(); 
	    exit 1;
  	}
} else {   #Updating production rule
	logger( "INFO Updating production rule: \"$ref->{prRuleName}\"" );
	logger( "DEBUG JOBPRIORITY=$jobPriority, PRRULETYPE='$ref->{prRuleType}',GZID=$gzID, PLORBITTYPEID=$plOrbitTypeId,
                                                          JOBCLASS=$jobClass, PRACTIVEFLAG_NSOF=0, PRACTIVEFLAG_CBU=0,PRTEMPORARYSPACEMB='$ref->{prTemporarySpaceMB}',
                                                           PRNOTIFYOPSSECONDS='$ref->{prNotifyOpsSeconds}', PRESTIMATEDRAM_MB='$ref->{prEstimatedRAM_MB}',
                                                            PRESTIMATEDCPU='$ref->{prEstimatedCPU}', PRPRODUCTCOVERAGEINTERVAL_DS=$pcids,
                                                             PRSTARTBOUNDARYTIME=$sbt, PRRUNINTERVAL_DS=$ref->{prRunInterval_DS}, PRWAITFORINPUTINTERVAL_DS=$ref->{prWaitForInputInterval}
                                                                   where PRRULENAME='$ref->{prRuleName}' and ALGORITHMID=$algoID" );
	my $success = $dbh->do( "update PRODUCTIONRULE set JOBPRIORITY=$jobPriority, PRRULETYPE='$ref->{prRuleType}',GZID=$gzID, PLORBITTYPEID=$plOrbitTypeId,
							  JOBCLASS=$jobClass, PRACTIVEFLAG_NSOF=0, PRACTIVEFLAG_CBU=0,PRTEMPORARYSPACEMB='$ref->{prTemporarySpaceMB}', 
							   PRNOTIFYOPSSECONDS='$ref->{prNotifyOpsSeconds}', PRESTIMATEDRAM_MB='$ref->{prEstimatedRAM_MB}', 
							    PRESTIMATEDCPU='$ref->{prEstimatedCPU}', PRPRODUCTCOVERAGEINTERVAL_DS=$pcids,
							     PRSTARTBOUNDARYTIME=$sbt, PRRUNINTERVAL_DS=$ref->{prRunInterval_DS}, PRWAITFORINPUTINTERVAL_DS=$ref->{prWaitForInputInterval}
								   where PRRULENAME='$ref->{prRuleName}' and ALGORITHMID=$algoID" );
	if ($success != 1){
	    logger( "ERROR Update in PRODUCTIONRULE table failed. Check Production Rule definition file." );
	    $dbh->rollback(); 
	    exit 1;
  	}
  	if ( @prdataselectionxml ) {
  		my $success = $dbh->do( "update PRODUCTIONRULE set PRDATASELECTIONXML='@prdataselectionxml' where PRRULENAME='$ref->{prRuleName}' and ALGORITHMID=$algoID" );
  		if ($success != 1) {
  			logger( "ERROR Update in PRODUCTIONRULE table for PRDATASELECTIONXML column failed. Check Production Rule definition file." );
  			$dbh->rollback();
  			exit 1;
  		}
  	}
}

$sth = runSQL( $dbh, "select PRID from PRODUCTIONRULE where PRRULENAME = '$ref->{prRuleName}'");
my $prID= $sth->fetchrow_array();
logger( "INFO  Production Rule inserted/updated, rule ID = $prID" );

# Add PR Parameters
chomp($sanpath = `. ~/.set_NDE_ENV_vars $opt_m > /dev/null; echo \$SAN_HOME`);
logger( "INFO Production Rule Parameters:" );
if ( $update eq "n" ) { #New production rule
	for my $prParam (@{$ref->{ProductionRuleParameters}{PRParameter}}) {
	        my $paramName = $prParam->{algoParameterName};
	        my $paramVal = $prParam->{prParameterValue};
	
	        $paramVal =~ s/\@MODE\@/$opt_m/;
	        $paramVal =~ s/\@SANPATH\@/$sanpath/;
	        
	        logger( "INFO  Adding: $paramName = $paramVal" );
	        
	        $sth = runSQL($dbh, "select ALGOPARAMETERID from ALGOPARAMETERS where ALGOPARAMETERNAME='$paramName' and ALGORITHMID=$algoID" );
	        my $algoParamID = $sth->fetchrow_array();
	        if ( defined($algoParamID) ) {
	        	#Check and see if production rule parameter has already been assigned a value (prevents registering same parameter twice)
	        	if ( !runDupeCheck($dbh, "select PRPARAMETERSEQNO from PRPARAMETER where ALGOPARAMETERID=$algoParamID and PRID=$prID and PRPARAMETERVALUE='$paramVal'") ) {
	        	    my $success = $dbh->do( "insert into PRPARAMETER (PRPARAMETERSEQNO, ALGOPARAMETERID, PRID, PRPARAMETERVALUE)
		                 select nextval('s_algoparameters'), algoParameterId, $prID, '$paramVal'
		                  from ALGOPARAMETERS
		                   where ALGORITHMID = $algoID and ALGOPARAMETERNAME = '$paramName'" );
		                   
			        if ($success != 1) {
			  			logger( "ERROR   Insert in PRPARAMETER table failed. Check Production Rule definition file. Exiting..." );
			  			$dbh->rollback();
			  			exit 1;
		  			}
	        	} else {logger("WARN  Parameter $paramName already has value of $paramVal. Check production rule for duplication of parameter. Continuing.");}
			} else {
				logger( "ERROR   PR parameter: $paramName does not exist in ALGOPARAMETERS. Exiting...");
			    $dbh->rollback();
			    exit 1;
			}
	}
} else { #Updating production rule parameters or adding any new ones
	for my $prParam (@{$ref->{ProductionRuleParameters}{PRParameter}}) {
	        my $paramName = $prParam->{algoParameterName};
	        my $paramVal = $prParam->{prParameterValue};
	
	        $paramVal =~ s/\@MODE\@/$opt_m/;
	        $paramVal =~ s/\@SANPATH\@/$sanpath/;
	        
	        $sth = runSQL($dbh, "select pp.ALGOPARAMETERID from ALGOPARAMETERS ap, PRPARAMETER pp 
	        					where ALGOPARAMETERNAME='$paramName' and ALGORITHMID=$algoID and ap.ALGOPARAMETERID=pp.ALGOPARAMETERID and PRID=$prID" );
	        my $algoParamID = $sth->fetchrow_array();
			if ( defined($algoParamID) ) {
				logger( "INFO  Updating: $paramName = $paramVal (algorithm param id: $algoParamID)" );
				my $success = $dbh->do( "update PRPARAMETER set PRPARAMETERVALUE='$paramVal' where ALGOPARAMETERID=$algoParamID and PRID=$prID");
				if ($success != 1) {
		  			logger( "ERROR   Update to PRPARAMETER table failed. Check Production Rule definition file." );
		  			$sth = runSQL($dbh,"select count(*) from PRPARAMETER where ALGOPARAMETERID=$algoParamID and PRID=$prID");
		  			my $rows = $sth->fetchrow_array();
		  			if ($rows > 1){
		  				logger("WARN    Duplicate pr parameters for above algorithm parameter ID. Ignore (no update to this parameter) and continue updates? (y/n)");
		  				chomp(my $cont = <STDIN>);
						if (($cont ne 'y') && ($cont ne 'n')) {
							logger( "ERROR  You did not enter a \"y\" or a \"n\". Exiting...");
							$dbh->rollback();
							exit 1;
						} elsif ($cont eq "n") {
							logger( "INFO  Not updating rule $ref->{prRuleName} and exiting...");
							$dbh->rollback();
							exit 1;
						}
		  			}
	  			}
			} else {
				logger( "INFO  Adding: $paramName = $paramVal (param id: $algoParamID)" );
				$sth = runSQL($dbh, "select ALGOPARAMETERID from ALGOPARAMETERS where ALGOPARAMETERNAME='$paramName' and ALGORITHMID=$algoID" );
	        	my $algoParamID = $sth->fetchrow_array();
	        	if ( defined($algoParamID) ) {	        	
					my $success = $dbh->do( "insert into PRPARAMETER (PRPARAMETERSEQNO, ALGOPARAMETERID, PRID, PRPARAMETERVALUE)
		                 select nextval('s_algoparameters'), algoParameterId, $prID, '$paramVal'
		                  from ALGOPARAMETERS
		                   where ALGORITHMID = $algoID and ALGOPARAMETERNAME = '$paramName'" );
		                   
			        if ($success != 1) {
			  			logger( "ERROR   Insert in PRPARAMETER table failed. Check Production Rule definition file." );
			  			$dbh->rollback();
			  			exit 1;
	  				}
	        	} else {
		        	logger( "ERROR   PR parameter: $paramName does not exist in ALGOPARAMETERS. Exiting...");
		        	$dbh->rollback();
		        	exit 1;
	        	}
			}
	}
}


#
# Do PR Input Spec
#
# 20090310 lhf, changed prisLUN to productfilehandle in insert statement below
# 20090313 tjf, changed productfilehandle to prisfilehandle
# 20090811 lhf, updated to work with proto4, structural change
# 20141009 dcp, updated to include platform with prInputSpec (for multiple satellites)
#
logger( "INFO Production Rule inputs:" );
if ( $update eq "n" ) { #New production rule
	for my $prInput (@{$ref->{ProductionRuleInputs}{PRInputSpec}}) {
		logger( "INFO  $prInput->{prisNeed}:" );
		
		#Check pris file handle numbering
        if ( $prInput->{prisFileHandleNumbering} eq '' ) {
                logger( "ERROR  prisFileHandleNumbering attribute missing for $prInput->{prisFileHandle}, please correct XML and retry. Exiting..." );
                $dbh->rollback(); exit 1;
        }
        if ( $prInput->{prisFileHandleNumbering} ne 'Y' && $prInput->{prisFileHandleNumbering} ne 'N' ) {
                logger( "ERROR  prisFileHandleNumbering attribute: $prInput->{prisFileHandleNumbering} must be \"Y\" or \"N\", please correct XML and retry. Exiting..." );
                $dbh->rollback(); exit 1;
        }
		
		my $sth = runSQL( $dbh, "select nextval('S_PRINPUTSPEC')" );
        my ($prisID) = $sth->fetchrow_array();
        
        my $success = $dbh->do( "insert into PRINPUTSPEC (PRISID, PRID, PRISFILEHANDLE, PRISNEED, PRISFILEHANDLENUMBERING,
                PRISTEST, PRISLEFTOFFSETINTERVAL,PRISRIGHTOFFSETINTERVAL,PRISFILEACCUMULATIONTHRESHOLD)
                 values ($prisID, $prID, '$prInput->{prisFileHandle}',
                  '$prInput->{prisNeed}','$prInput->{prisFileHandleNumbering}','$prInput->{prisTest}',
                   $prInput->{prisLeftOffsetInterval}, $prInput->{prisRightOffsetInterval},
                    '$prInput->{prisFileAccumulationThreshold}')" );
        if ($success != 1) {
        	logger( "ERROR   Insert in PRINPUTSPEC table failed. Check Production Rule definition file. Exiting..." );
			$dbh->rollback();
			exit 1;
	  	}
        
		# Confirm input product is registered and assigned to specific algorithm.
        for my $inProd (@{$prInput->{PRInputProduct}}) {
                        logger( "INFO   Product: $inProd->{productShortName}" );
                        my $productid;
                        my $sth = runSQL($dbh, "select count(pd.PRODUCTID) from PRODUCTDESCRIPTION pd, ALGOINPUTPROD aip 
                        						 where pd.PRODUCTID = aip.PRODUCTID and aip.ALGORITHMID = $algoID 
                        						   and PRODUCTSHORTNAME = '$inProd->{productShortName}'");
                        my $count = $sth->fetchrow_array();
                        my $sth = runSQL($dbh, "select pd.PRODUCTID from PRODUCTDESCRIPTION pd, ALGOINPUTPROD aip 
                        						 where pd.PRODUCTID = aip.PRODUCTID and aip.ALGORITHMID = $algoID 
                        						   and PRODUCTSHORTNAME = '$inProd->{productShortName}'");
                        						      
                        #Check for platform xref. If ANC data then no platform. If not ANC and no platform identified then default is SNPP.
                        my $platformName = $inProd->{platformName};
                        
                        if ( $count > 1 && !defined($platformName) ) { #Not ancillary data
                        	$platformName = 'SNPP';
                        }					   							
                        if ( defined($platformName) ) {
                        	$sth = runSQL( $dbh, "select PLATFORMID from PLATFORM where PLATFORMNAME = '$platformName'" );
							my $platformID = $sth->fetchrow_array();
							logger( "INFO   Platform: $platformName" );
							
							if (!defined($platformID)) {
								logger( "ERROR   Platform: $platformName is not registered. Exiting..." );
							    $dbh->rollback();
								exit 1;
							}
							
							my $sth = runSQL($dbh, "select pd.PRODUCTID from PRODUCTDESCRIPTION pd, ALGOINPUTPROD aip, PRODUCTPLATFORM_XREF prx 
                        						 where pd.PRODUCTID = aip.PRODUCTID and aip.ALGORITHMID = $algoID 
                        						   and PRODUCTSHORTNAME = '$inProd->{productShortName}'
                        						   	and pd.PRODUCTID = prx.PRODUCTID and prx.PLATFORMID=$platformID");
                        	$productid = $sth->fetchrow_array();
                        } else {
                        	logger( "INFO   Platform: no platform identified (ANCILLARY DATA)" );
                        	$productid = $sth->fetchrow_array();
                        }
                        if ( !defined($productid) ){
                                logger( "ERROR    Invalid input product ($inProd->{productShortName}) or hasn't been assigned to algorithm input. Exiting...");
                                $dbh->rollback(); 
                                exit 1;
                        }	
		                
		                my $success = $dbh->do( "insert into PRINPUTPRODUCT (PRODUCTID, ALGORITHMID, PRISID, PRIPID, PRINPUTPREFERENCE)
		                                        values ($productid, $algoID, $prisID, nextval('S_PRINPUTPRODUCT'),
		                                        '$inProd->{prInputPreference}') ");
		                if ($success != 1) {
				        	logger( "ERROR   Insert in PRINPUTPRODUCT table failed. Check Production Rule definition file. Exiting..." );
							$dbh->rollback();
							exit 1;
					  	} else {
					  		logger("INFO  	 PRISID: $prisid");
						  	logger("INFO  	 PRISFILEHANDLENUMBERING: $prInput->{prisFileHandleNumbering}");
						  	logger("INFO  	 PRISTEST: $prInput->{prisTest}");
						  	logger("INFO  	 PRISLEFTOFFSETINTERVAL: $prInput->{prisLeftOffsetInterval}");
						  	logger("INFO  	 PRISRIGHTOFFSETINTERVAL: $prInput->{prisRightOffsetInterval}");
						  	logger("INFO  	 PRISFILEACCUMULATIONTHRESHOLD: $prInput->{prisFileAccumulationThreshold}");
					  	}
         }
	}
} else { #Can only update the product specification and input preference (cannot change/add input products - requires new production rule)
	logger( "WARN  Continue with update of input product specifications (y/n)?" );
	chomp($update = <STDIN>);
	if (($update ne 'y') && ($update ne 'n')) {
		logger( "ERROR  You did not enter a \"y\" or a \"n\". Exiting...");
		$dbh->rollback();
		exit 1;
	} elsif ($update eq "n") {
		logger( "INFO  Not updating input product specifications and exiting...");
		$dbh->commit;
		logger ("INFO Addition of Production Rule \"$ref->{prRuleName}\" has been successful." );
		my $rc = system("cp $opt_f /opt/data/nde/$opt_m\/xmls");
		if ( $rc ) {
		  logger( "ERROR Failed to copy xml file: $opt_f" );
		} else {
		  logger( "INFO $opt_f successfully copied");
		}
		
		logger( "INFO Execution complete, results: $logfile" );
		exit 0;
	}
	for my $prInput (@{$ref->{ProductionRuleInputs}{PRInputSpec}}) {
		logger( "INFO  Updating $prInput->{prisNeed}:" );
		
		for my $inProd (@{$prInput->{PRInputProduct}}) {
			my $sth = runSQL($dbh,"select PIS.PRISID from PRINPUTSPEC PIS, PRINPUTPRODUCT PIP, PRODUCTDESCRIPTION PD, ALGOINPUTPROD AIP
									where PRID=$prID and PIS.PRISID=PIP.PRISID and PIP.PRODUCTID=AIP.PRODUCTID and
									  AIP.ALGORITHMID = $algoID and AIP.PRODUCTID=PD.PRODUCTID and PD.PRODUCTSHORTNAME='$inProd->{productShortName}'");
			my $prisid = $sth->fetchrow_array();
			if ( !defined($prisid) ) {
				logger( "ERROR   No matching input product: \"$inProd->{productShortName}\" for this input specification. Exiting..." );
				$dbh->rollback();
				exit 1;
			}
			
			logger( "INFO   $inProd->{productShortName}" );
			
			$sth = runSQL($dbh,"select count(*) from PRINPUTSPEC PIS, PRINPUTPRODUCT PIP, PRODUCTDESCRIPTION PD, ALGOINPUTPROD AIP
									where PRID=$prID and PIS.PRISID=PIP.PRISID and PIP.PRODUCTID=AIP.PRODUCTID and
									  AIP.ALGORITHMID = $algoID and AIP.PRODUCTID=PD.PRODUCTID and PD.PRODUCTSHORTNAME='$inProd->{productShortName}'");
			my $rows = $sth->fetchrow_array();
			if ($rows > 1) {
				logger( "ERROR  More than one input specification has above product as input. Cannot update. Exiting..." );
				$dbh->rollback();
				exit 1;
			}
									  
			$sth = runSQL($dbh,"select PIP.PRIPID from PRINPUTPRODUCT PIP, ALGOINPUTPROD AIP, PRODUCTDESCRIPTION PD
									where PIP.PRISID=$prisid and AIP.ALGORITHMID=$algoID and PIP.PRODUCTID=AIP.PRODUCTID and 
									  AIP.PRODUCTID=PD.PRODUCTID and PD.PRODUCTSHORTNAME='$inProd->{productShortName}'");
			my $pripid = $sth->fetchrow_array();
			if (!defined($pripid)){
				logger( "ERROR   No matching input product for $inProd->{productShortName}. Exiting..." );
				$dbh->rollback();
				exit 1;
			}

			my $success = $dbh->do( "update PRINPUTPRODUCT set PRINPUTPREFERENCE='$inProd->{prInputPreference}'
	  								where PRIPID=$pripid");
	  		logger("INFO     PRINPUTPREFERENCE: $inProd->{prInputPreference}");
	  		if ($success != 1) {
				logger( "ERROR   Update to PRINPUTPRODUCT: PRINPUTPREFERENCE failed. Exiting..." );
				$dbh->rollback();
				exit 1;
	  		}
	  		
			$success = $dbh->do( "update PRINPUTSPEC set PRISFILEHANDLE='$prInput->{prisFileHandle}',
					PRISNEED='$prInput->{prisNeed}', PRISFILEHANDLENUMBERING='$prInput->{prisFileHandleNumbering}',
					PRISTEST='$prInput->{prisTest}',PRISLEFTOFFSETINTERVAL=$prInput->{prisLeftOffsetInterval},
					PRISRIGHTOFFSETINTERVAL=$prInput->{prisRightOffsetInterval},
					PRISFILEACCUMULATIONTHRESHOLD='$prInput->{prisFileAccumulationThreshold}'
					where PRISID=$prisid");
			if ($success != 1) {
				logger( "ERROR   Update to PRINPUTSPEC table failed. Check Production Rule definition file. Exiting..." );
				$dbh->rollback();
				exit 1;
	  		} else {
	  			logger("INFO  	 PRISID: $prisid");
		  		logger("INFO  	 PRISFILEHANDLENUMBERING: $prInput->{prisFileHandleNumbering}");
		  		logger("INFO  	 PRISTEST: $prInput->{prisTest}");
		  		logger("INFO  	 PRISLEFTOFFSETINTERVAL: $prInput->{prisLeftOffsetInterval}");
		  		logger("INFO  	 PRISRIGHTOFFSETINTERVAL: $prInput->{prisRightOffsetInterval}");
		  		logger("INFO  	 PRISFILEACCUMULATIONTHRESHOLD: $prInput->{prisFileAccumulationThreshold}");
	  		}
		}
		
	}
	logger("WARN  Make sure the same pr input spec (PRISID) wasn't repeated. Database will update to most recent.");
}

#
# Do Output Products
#
# 20090310 lhf, added productfilehandle to end of column list and '$input->{productFileHandle}' to set below
# 20090313 tjf, changed productfilehandle to prosfilehandle
# 20090313 tjf, remove prosfilehandle
#
logger( "INFO Production rule outputs:" );
if ( $update eq "n" ) { #New production rule
	for my $prOutput (@{$ref->{ProductionRuleOutputs}{PROutputSpec}}) {
		logger( "INFO  Output Product: $prOutput->{productShortName}" );
        
        #Check for platform xref. If no platform identified then default is SNPP.
        my $platformName = $prOutput->{platformName};
        if ( !defined($platformName) ) {$platformName = 'SNPP';}
        $sth = runSQL( $dbh, "select PLATFORMID from PLATFORM where PLATFORMNAME = '$platformName'" );
	    my $platformID = $sth->fetchrow_array();
		logger( "INFO Platform: $platformName" );
		if ( !defined($platformID) ) {
			logger( "ERROR   Platform: $platformName is not registered. Exiting..." );
		    $dbh->rollback();
			exit 1;
		}
							
		my $sth = runSQL($dbh, "select pd.PRODUCTID from PRODUCTDESCRIPTION pd, ALGOOUTPUTPROD aop, PRODUCTPLATFORM_XREF prx 
                        			 where pd.PRODUCTID = aop.PRODUCTID and aop.ALGORITHMID = $algoID 
                        			   and PRODUCTSHORTNAME = '$prOutput->{productShortName}'
                        			   	and pd.PRODUCTID = prx.PRODUCTID and prx.PLATFORMID=$platformID");
        $productid = $sth->fetchrow_array();

        if ( !defined($productid) ) {
                logger( "ERROR  Invalid output product ($prOutput->{productShortName}) or hasn't been assigned to algorithm output. Exiting..." );
                $dbh->rollback(); exit 1;
        }
        
        my $success = $dbh->do( "insert into PROUTPUTSPEC (PROSID, PRID, PRODUCTID, ALGORITHMID)
                                select nextval('s_proutputspec'), $prID, $productid, $algoID
                                from PRODUCTDESCRIPTION pd
                                where pd.PRODUCTID = $productid");
        if ($success != 1) {
				logger( "ERROR   Update to PROUTPUTSPEC table failed. Check Production Rule definition file. Exiting..." );
				$dbh->rollback();
				exit 1;
	  	}
	}
} else {
	$update = 'n';
	logger( "WARN  Continue with update of output product specifications, this will replace old product ids for each output spec (y/n)?" );
	chomp($update = <STDIN>);
	if ($update eq 'y') {
		my $size_ids = @{$ref->{ProductionRuleOutputs}{PROutputSpec}};
		my $sql = "select count(PROSID) from PROUTPUTSPEC where prid=$prID";
		$sth = runSQL( $dbh, $sql);
		my $num_prosids = $sth->fetchrow_array();
		if ($num_prosids ne $num_prosids) {
			logger ("ERROR Invalid number of output products for updating PROUTPUTSPEC. #PROSIDs = $num_prosids AND #PROutputProducts = $size_ids. Exiting...");
			$dbh->rollback(); exit 1;
		}
		my $sql = "select PROSID from PROUTPUTSPEC where prid=$prID";
		$sth = runSQL( $dbh, $sql);
		my $i=0;
		while ( my $prosid = $sth->fetchrow_array() ) {
			my $prodShortName = @{$ref->{ProductionRuleOutputs}{PROutputSpec}}[$i]->{productShortName};
			my $platformName = @{$ref->{ProductionRuleOutputs}{PROutputSpec}}[$i]->{platformName};
			if ( !defined($platformName) ) {$platformName = 'SNPP';}
			$sth = runSQL( $dbh, "select PLATFORMID from PLATFORM where PLATFORMNAME = '$platformName'" );
	    	my $platformID = $sth->fetchrow_array();
			logger("INFO  Updating PROSID ($prosid) to $prodShortName for $platformName");
			my $sql = "update PROUTPUTSPEC set PRODUCTID=(select pd.PRODUCTID from PRODUCTDESCRIPTION pd, PRODUCTPLATFORM_XREF prx 
						where PRODUCTSHORTNAME like '$prodShortName' and pd.PRODUCTID = prx.PRODUCTID and prx.PLATFORMID=$platformID) 
							where PROSID=$prosid";
			my $sth_2 = runSQL($dbh,$sql);
			$i++; 
		}
		
	}
}

$dbh->commit;

logger ("INFO Addition of Production Rule \"$ref->{prRuleName}\" has been successful." );
my $rc = system("cp $opt_f /opt/data/nde/$opt_m\/xmls");
if ( $rc ) {
  logger( "ERROR Failed to copy xml file: $opt_f" );
} else {
  logger( "INFO $opt_f successfully copied");
}

logger( "INFO Execution complete, results: $logfile" );

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
