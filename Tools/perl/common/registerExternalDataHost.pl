#!/usr/bin/env perl

#
# name: registerExternalDataHost.pl
# revised: 20101021 lhf, creation
#          20130131 teh, require ndeUtiltiles.pl for pwd prompt
#          20141029 teh, ENTR-650 Do an "update", vs just add when hostname matches previous entry.
#
use XML::Simple;
use Data::Dumper;

use Getopt::Std;
getopt('m:f:');

### Check for comand line args
if ( ! $opt_m || ! $opt_f ) {
        print "Usage $0 -m <mode> -f <XML Filename>\n";
        exit(1);
}

### Connect to Oracle
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

print "$0 Execution started: ", `date` ,"\n";

### create the exthostpwd file by invoking java class
my $ehostpw = promptForExtHostPassword("\n  Enter External Host FTPS  password: ");
`rm -rf /tmp/.exthost`;
### Read the exthost pwd file
my $cmd = "$ENV{JAVA_HOME}/bin/java -Xms128m -Xmx128m  -cp $ENV{deploydir}/../lib/commons-codec-1.3.jar:$ENV{deploydir}/utilities.jar org.noaa.espc.nde.dhs.util.DeEncrypter $ehostpw /tmp/.exthost";
$rc = `$cmd`;
my $filename = '/tmp/.exthost';
open(my $fh, '<:encoding(UTF-8)', $filename)
  or die "Could not create encrypted password. $!";

my $exthostpwd = <$fh>;

my %ref;
my $ref = XMLin($opt_f);

### Check to see if this externaldatahost/hostaccessloginid combination already exists
#ENTR-650 In context of NDE 2.0, no push hosts, only ftpdownloader (data source hosts).  
#Only will allow 1 ID per host so as to not confuse which data host a product definition should map to.
#my $sql=qq{select HOSTID from EXTERNALDATAHOST where HOSTNAME='$ref->{hostName}' and HOSTACCESSLOGINID='$ref->{hostAccessLoginId}'} ;
my $sql=qq{select HOSTID from EXTERNALDATAHOST where HOSTNAME='$ref->{hostName}'} ;
my $sth=$dbh->prepare($sql);
$sth->execute;
my $update = n;
my ($hostId) = eval { $sth->fetchrow_array()};
if ($hostId ne "" ) {
  print "HOSTNAME ( $ref->{hostName} already exists.  Do you want to update? (y/n): )\n";
  chomp($update = <STDIN>);
  if (($update ne 'y') && ($update ne 'n')) {
		print "ERROR  You did not enter a \"y\" or a \"n\". Exiting...\n";
		exit 1;
	} elsif ($update eq "n") {
		print "INFO  Not updating host $ref->{hostName} and exiting...\n";
		exit 1;
	}
}

### Update the host
if ($update eq 'y') {
  $sql = qq{ update EXTERNALDATAHOST set HOSTNAME = '$ref->{hostName}', HOSTADDRESS = '$ref->{hostAddress}', 
	  HOSTSTATUS = '$ref->{hostStatus}', HOSTACCESSTYPE = '$ref->{hostAccessType}',
               HOSTACCESSDESCRIPTION = '$ref->{hostAccessDescription}', HOSTACCESSLOGINID = '$ref->{hostAccessLoginId}', 
               HOSTACCESSPASSWORD = '$exthostpwd', MAXCONCURRENTCONNECTIONS = $ref->{maxConcurrentConnections},
               CERTIFICATES = '$ref->{certificates}' where HOSTID = $hostId };
} else {
### If host is not in the table, we're going to insert, assume rest of the columns are populated
  $sql = qq{ insert into EXTERNALDATAHOST (HOSTID, HOSTNAME, HOSTADDRESS, HOSTSTATUS, HOSTACCESSTYPE,
               HOSTACCESSDESCRIPTION, HOSTACCESSLOGINID, HOSTACCESSPASSWORD, MAXCONCURRENTCONNECTIONS,
                CERTIFICATES) values ( nextval('S_EXTERNALDATAHOST'), '$ref->{hostName}',
                 '$ref->{hostAddress}', '$ref->{hostStatus}', '$ref->{hostAccessType}',
                  '$ref->{hostAccessDescription}', '$ref->{hostAccessLoginId}',
                   '$exthostpwd', $ref->{maxConcurrentConnections},
                    '$ref->{certificates}' ) };
}
my $sth=$dbh->prepare($sql);
$sth->execute
      or die "\nDatabase Error, registration unsuccessful.\n";
 
### Check success
my $sql=qq{select HOSTID from EXTERNALDATAHOST where HOSTNAME='$ref->{hostName}'} ;
my $sth=$dbh->prepare($sql);
$sth->execute;
my ($hostId) = eval { $sth->fetchrow_array()};

if ($hostId ne "" ) {
  print "Successfully " . ($update eq 'y'?"updated":"added") . " ExternalDataHost [ $ref->{hostName} ] as HostID: $hostId\n";
}

print "\n$0 Execution completed.\n";
exit( 0 );
