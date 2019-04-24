#!/usr/bin/env perl

#
# name: registerAlgorithm.pl
# revised: 20111220 lhf, add xsd validation
#          20090225 lhf, updated
#          20090417 tc, updated for mode NDE_B2P2
#          20100115 lhf, updated so algorithmcommandprefix contains a single space if specified
#          20100203 lhf, implement transaction, sqlCommandStack
#          20100329 lhf, process UNDEFINED log context
#          20110412 lhf, -s --> -m
#                  20111102 htp, updated logger( s, rollback, error handling for unregistered prods
#          20120522 lhf, build 4/5, xsd validation, file move, NDE-603
#          20120530 lhf, correct log file NDE-603
#          20120808 htp, updated to handle algorithmexecutablename
#          20121212 lhf, add logs/pgs
#		   20130604 dcp, added ability to update tables; some refactoring; error handling
#		   20140721 dcp, added capability to update algorithm executable name
#		   20141013 dcp, added platform mapping to product IDs
#
# Input:
#       Database Name
#       Algorithm Definition XML File
# Output:
#       Updated Oracle Database
#

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
my $logfile = "/opt/data/nde/" . $opt_m . "/logs/pgs/ALGORITHM_" . reverse($rfn) . "_" . $YMDHMS . ".log";
open ( LOG, ">$logfile" ) or
  die "ERROR Error opening logfile $logfile, exiting...\n";

logger(  "INFO Execution started..." );

my $schema = XML::LibXML::Schema->new(location => "/opt/data/nde/$opt_m\/xsds/ALGORITHM.xsd");
my $parser = XML::LibXML->new;
my $doc = $parser->parse_file( $opt_f );

eval {
    $schema->validate($doc);
};

if ($@) {
    logger( "ERROR $opt_f file FAILED XSD validation: $@");
    exit( 1 );
}

if (-r "../common/ndeUtilities.pl") {
  require "../common/ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}
my $pw = promptForPassword("\n  Enter $opt_m password: ");
my $dbh = DBI->connect( "dbi:Pg:dbname=nde_dev1;host=nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com", lc($opt_m), $pw )
          or die "\nDatabase Connection Error: $DBI::errstr\n";
#$dbh->{LongReadLen}=64000;
$dbh->begin_work;

my %ref;
my $ref = XMLin($opt_f, forcearray => [ 'AlgoParameter', 'AlgoInputProd', 'AlgoOutputProd' ]);

# Check for existing algorithm. If exists check to see if user wants to update (only adds products and parameters - deleting must be database patch)
my $update="n";  # initialize the update variable
my $algoName = $ref->{algorithmName};
my $algoVer  = $ref->{algorithmVersion};
logger(  "INFO Checking existing algorithms for duplicate..." );

my $sth = runSQL($dbh,"select ALGORITHMID from ALGORITHM where ALGORITHMNAME = '$algoName' and ALGORITHMVERSION='$algoVer'");
my $algoID = $sth->fetchrow_array();
if ( !$algoID ) {
        $sth = runSQL( $dbh, "select nextval('s_algorithm')" );
        $algoID = $sth->fetchrow_array();
}
else {
        logger( "WARNING   Algorithm \"$algoName v$algoVer\" exists (ID: $algoID), continue with update? (y/n): ");
        chomp($update=<STDIN>);
        if (($update ne 'y') && ($update ne 'n')) {
			logger( "ERROR You did not enter a \"y\" or a \"n\". Exiting...");
			$dbh->rollback();
			exit 1;
		} elsif($update eq "n"){  
        	logger( "INFO Not updating duplicate algorithm \"$algoName v$algoVer\" and exiting..."); 
        	$dbh->rollback; 
        	exit 1;
        }
}

# fix algorithmCommandPrefix for single space issue - 20100115
my $algoCommandPrefix = $ref->{algorithmCommandPrefix};
if ( length($algoCommandPrefix) == 0 ) {
        $algoCommandPrefix = ' ';
}

# process UNDEFINED log message context
$algoLogMsgContext = $ref->{algorithmLogMessageContext};
if ( $algoLogMsgContext eq "UNDEFINED" ) {
        $algoLogMsgContext = '.';
}

# Insert ALGORITHM
if ( $update eq "n" ) {   #New algorithm
  logger("INFO Inserting new algorithm: \"$algoName v$algoVer\"");
  my $success = $dbh->do(" insert into ALGORITHM (ALGORITHMID, ALGORITHMNAME, ALGORITHMEXECUTABLENAME, ALGORITHMVERSION)
            values( $algoID, '$ref->{algorithmName}', '$ref->{algorithmExecutableName}', '$ref->{algorithmVersion}' )");
#  if( $success == 1) {
#    my $sth = runSQL($dbh, "select ALGORITHMID from ALGORITHM where ALGORITHMNAME = '$algoName' and ALGORITHMVERSION='$algoVer'" );
#    $algoID = eval { $sth->fetchrow_array()};
#  } else {
  if ($success != 1){
    logger( "ERROR Insert in ALGORITHM table failed. Check Algorithm definition file." );
    $dbh->rollback(); 
    exit 1;
  }
}

logger( "INFO Registering algorithm: \"$algoName v$algoVer\"" );
my $success = $dbh->do(" update ALGORITHM set ALGORITHMNOTIFYOPSECONDS='$ref->{algorithmNotifyOpSeconds}',
          ALGORITHMCOMMANDPREFIX='$algoCommandPrefix', ALGORITHMEXECUTABLELOCATION='$ref->{algorithmExecutableLocation}',
           ALGORITHMTYPE='$ref->{algorithmType}', ALGORITHMPCF_FILENAME='$ref->{algorithmPcf_Filename}',
            ALGORITHMPSF_FILENAME='$ref->{algorithmPsf_Filename}', ALGORITHMLOGFILENAME='$ref->{algorithmLogFilename}',
             ALGORITHMLOGMESSAGECONTEXT='$algoLogMsgContext', ALGORITHMLOGMESSAGEWARN='$ref->{algorithmLogMessageWarn}',
              ALGORITHMLOGMESSAGEERROR='$ref->{algorithmLogMessageError}', ALGORITHMEXECUTABLENAME='$ref->{algorithmExecutableName}'
         where ALGORITHMID=$algoID ");
if( $success != 1 ){
  logger(  "ERROR Insert in ALGORITHM table failed. Check Algorithm definition file." );
  $dbh->rollback(); 
  exit 1;
}


# Insert ALGOPARAMETERS
logger(  "INFO Algorithm parameters:" );
if ( $update eq "n" ){ #New algorithm
	for my $parm (@{$ref->{AlgorithmParameters}{AlgoParameter}}) {
		if (!runDupeCheck($dbh,"select algoParameterName from algoParameters where algoParameterName='$parm->{algoParameterName}' and algorithmId=$algoID")) {
			logger(  "INFO   Adding: $parm->{algoParameterName} | $parm->{algoParameterDataType}" );
	        my $success = $dbh->do(" insert into AlgoParameters (algoParameterId, algorithmId, algoParameterName,
	                 algoParameterDataType) values (nextval('s_algoparameters'), $algoID,
	                  '$parm->{algoParameterName}', '$parm->{algoParameterDataType}') ");
	        if( $success != 1 ){
  					logger(  "ERROR Insert in ALGOPARAMETERS table failed. Check Algorithm definition file." );
  					$dbh->rollback(); 
  					exit 1;
			}
		}
	}
} else {  #Update existing algorithm
	for my $parm (@{$ref->{AlgorithmParameters}{AlgoParameter}}) {
	        if ( runDupeCheck($dbh,"select algoParameterName from algoParameters where algoParameterName='$parm->{algoParameterName}' and algorithmId=$algoID") ) {
	        	logger(  "INFO   Updating parameter: $parm->{algoParameterName} | $parm->{algoParameterDataType}" );
	        	my $success = $dbh->do(" update AlgoParameters set algoParameterDataType='$parm->{algoParameterDataType}' 
	        		where algoParameterName='$parm->{algoParameterName}' and algorithmId=$algoID");
	        	if( $success != 1 ){
  					logger(  "ERROR    Update to ALGOPARAMETERS table failed. Check Algorithm definition file." );
  					$dbh->rollback(); 
  					exit 1;
				}
	        } else {
	        	logger(  "INFO   Adding parameter: $parm->{algoParameterName} | $parm->{algoParameterDataType}" );
	        	my $success = $dbh->do(" insert into AlgoParameters (algoParameterId, algorithmId, algoParameterName,
	                 algoParameterDataType) values (nextval('s_algoparameters'), $algoID,
	                  '$parm->{algoParameterName}', '$parm->{algoParameterDataType}') ");
	            if( $success != 1 ){
  					logger(  "ERROR    Insert in ALGOPARAMETERS table failed. Check Algorithm definition file." );
  					$dbh->rollback(); 
  					exit 1;
				}
	        }
	}
}

# Insert ALGOINPUTPROD. Default is to add all product IDs with the same productShortName to the possible inputs (i.e. all platforms)
logger(  "INFO Algorithm input products:" );
if ( $update eq "n" ){ #New algorithm
	for my $input (@{$ref->{AlgorithmInputs}{AlgoInputProd}}) {
	        logger(  "INFO   Adding: $input->{productShortName}" );
	        $sth = runSQL($dbh, "select productId from productDescription where productShortName = '$input->{productShortName}'" );
	        
	        my $i=0;
	        while ( my $prodID = $sth->fetchrow_array() ) {
	        	my $success = $dbh->do( "insert into AlgoInputProd (productId, algorithmId) values ($prodID, $algoID)" );
		        if( $success != 1 ){
		        	logger(  "ERROR    Insert into ALGOINPUTPROD table failed. Check Algorithm definition file." );
	  				$dbh->rollback(); 
	  				exit 1;
				}
				$i++;
	        }
	        if ($i == 0) {
	        	logger(  "ERROR    Product: $input->{productShortName} is not registered. Exiting..." );
	  			$dbh->rollback(); 
	  			exit 1;
	        }
	        
	}
} else { #Update existing algorithm
	for my $input (@{$ref->{AlgorithmInputs}{AlgoInputProd}}) {
	        $sth = runSQL($dbh, "select productId from productDescription where productShortName = '$input->{productShortName}'" );
			
			my $i=0;
	        while ( my $prodID = $sth->fetchrow_array() ) {
	        	if ( !runDupeCheck($dbh,"select * from ALGOINPUTPROD where PRODUCTID=$prodID and ALGORITHMID=$algoID") ) {
		        	logger(  "INFO   Adding: $input->{productShortName}" );
		        	my $success = $dbh->do( "insert into AlgoInputProd (productId, algorithmId) 
		        							values ($prodID, $algoID)" );
		        	if( $success != 1 ){
			        	logger(  "ERROR    Insert in ALGOINPUTPROD table failed. Check Algorithm definition file." );
		  				$dbh->rollback(); 
		  				exit 1;
					}
		        }
		        $i++;
	        }
	        if ($i == 0) {
	        	logger(  "ERROR    Product: $input->{productShortName} is not registered. Exiting..." );
	  			$dbh->rollback(); 
	  			exit 1;
	        }
	           
	}
}

# Insert ALGOOUTPUTPROD
logger(  "INFO Algorithm output products:" );
if ( $update eq "n" ){ #New algorithm
	for my $output (@{$ref->{AlgorithmOutputs}{AlgoOutputProd}}) {
		    logger(  "INFO   Adding: $output->{productShortName}" );
	        $sth = runSQL($dbh, "select productId from productDescription where productShortName = '$output->{productShortName}'" );
	        my $i=0;
	        while ( my $prodID = $sth->fetchrow_array() ) {
		        my $success = $dbh->do( "insert into AlgoOutputProd (productId, algorithmId) values ($prodID, $algoID)" );
		        if( $success != 1 ){
		        	logger(  "ERROR    Insert into ALGOOUTPUTPROD table failed. Check Algorithm definition file." );
	  				$dbh->rollback(); 
	  				exit 1;
				}
				$i++;
	        }
	        if ($i == 0) {
	        	logger(  "ERROR    Product: $output->{productShortName} is not registered. Exiting..." );
	  			$dbh->rollback(); 
	  			exit 1;
	        }
	}
} else { #Update existing algorithm
	for my $output (@{$ref->{AlgorithmOutputs}{AlgoOutputProd}}) {
	        $sth = runSQL($dbh, "select productId from productDescription where productShortName = '$output->{productShortName}'" );
	        my $i=0;
	        while ( my $prodID = $sth->fetchrow_array() ) {
		        if ( !runDupeCheck($dbh,"select * from ALGOOUTPUTPROD where PRODUCTID=$prodID and ALGORITHMID=$algoID") ) {
		        	logger(  "INFO   Adding: $output->{productShortName}" );
		        	my $success = $dbh->do( "insert into AlgoOutputProd (productId, algorithmId) 
		        							values ($prodID, $algoID)" );
		        	if( $success != 1 ){
			        	logger(  "ERROR    Insert in ALGOOUTPUTPROD table failed. Check Algorithm definition file." );
		  				$dbh->rollback(); 
		  				exit 1;
					}
		        }
		        $i++;
	        }
	        if ($i == 0) {
	        	logger(  "ERROR    Product: $output->{productShortName} is not registered. Exiting..." );
	  			$dbh->rollback(); 
	  			exit 1;
	        }   
	}
}

###

$dbh->commit();

( $update eq "n" ) ? $action = "added" : $action = "updated";
logger(  "INFO Algorithm \"$algoName v$algoVer\" has been successfully $action (ID: $algoID)" );
my $rc = system("cp $opt_f /opt/data/nde/$opt_m\/xmls");
if ( $rc ) {
  logger( "ERROR Failed to copy xml file: $opt_f" );
} else {
  logger( "INFO $opt_f successfully copied");
}
logger(  "INFO Execution completed" );
print "\nLog file: $logfile\n";

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


exit 0;
