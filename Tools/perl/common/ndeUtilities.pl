#!/usr/bin/env perl

#
# name: ndeUtilties.pl
# purpose: Provide NDE some common use sub functions in one place to simplify maintenance.
#    Currently: 
#      promptForPassword( $promptString )
#           Purpose: No echo pwd prompt - usually for the DB pwd
#      runSQL ( $dbh, $sql ) 
#           Purpose: Against given DBI->connect, prep & execute given sql command string, return result.
#
#    Currently commented out, but likely to be added:
#      logger( $messageToLog )
#           Purpose: Currently: A quick "output to STDOUT a string with a date stamp"
#
# revised: 20130129 teh, creation  (stealing lavishly from lhf)
#


# To use: 
# At top do this: (after $opt_m has been tested to exist)
#    require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
#
# Or if you are feeling dev-friendly:
#
#    if (-r "./ndeUtilities.pl") {   #or possibly:   if (-r "../common/ndeUtilities.pl")
#      require "./ndeUtilities.pl";
#    } else {
#       require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
#    }
#
# Later just use them, eg:  
#    my $pw = promptForPassword("\n  Enter $opt_m password: "); 
#

sub promptForPassword {
  my ($prompt) = @_;
  my $word = $ENV{NDE_DB_KEY};
  if (length $word < 1) {
    system "stty -echo";
    print $prompt;
    chomp($word = <STDIN>);
    print "\n";
    system "stty echo";
    #Test pwd, exit if no good.  
    use DBI;
    my $dbh = DBI->connect("dbi:Pg:dbname=nde_dev1;host=nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com", lc($opt_m), $word)
       or die $DBI::errstr;
  }
  return $word;
}

sub promptForExtHostPassword {
  my ($prompt) = @_;
  my $word = "";
  if (length $word < 1) {
    system "stty -echo";
    print $prompt;
    chomp($word = <STDIN>);
    print "\n";
    system "stty echo";
  }
  return $word;
}



#old form:
#sub runSQL() {
#  my ( $dbh, $sql ) = @_;
#  my $sth = $dbh->prepare( $sql );
#  $sth->execute;
#  return $sth;
#}

sub runSQL() {
 my ( $dbh, $sql ) = @_;
 my $sth = $dbh->prepare($sql);
 $sth->execute
   or print "Can't execute SQL statement: $sql \n\nError:$DBI::errstr\n";
 return $sth;
}


##old version - still used in some scripts
#sub logger() {
#  my ( $msg ) = @_;
#  print "$msg\n";
#  chomp( $HMS = `date +%H:%M:%S,%N` );
#  print LOG substr($HMS,0,12) . " $msg\n";
#}
#
##Newer version
#sub logger() {
#  my ( $logfile, $message ) = @_;
#  open( LOG, ">>$logfile" ) or die "Can't open $logfile.\n";
#  chomp(my $d = `date "+%Y-%m-%d %H:%M:%S,%s"`);
#  print "$d $message\n";
#  print LOG "$d $message\n";
#  close( LOG );
#}

1;  #Perl wants a "true" at the end
