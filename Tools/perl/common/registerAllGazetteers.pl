#!/usr/bin/env perl

#
# name: registerAllGazetteers.pl
# purpose: given a directory containing _Definition files, register them all 
# 
# revised: 20100108 angela, creation

use Getopt::Std;

getopt('m:d:');

if ( ! $opt_m || ! $opt_d ) {
	print "Usage $0 -m <mode> -d <directory (full of Definition files)>\n";
	exit(1);
}
if (-r "./ndeUtilities.pl") {
  require "./ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}

print "\n ===>> $0 Execution started... <<=== \n\n";

my @files = `ls $opt_d`;
my @failedFiles;
my @passedFiles;
my $passedCount = 0;
my $pw = promptForPassword("\n  Enter $opt_m password: ");

$ENV{NDE_DB_KEY} = $pw; 
foreach my $definition_file ( @files ) {

  chomp( $definition_file );
  print "Processing $opt_d\/$definition_file...\n";
  my $exitcode = system("/opt/apps/nde/$opt_m/common/pg/registerGazetteer.pl -m $opt_m -f $opt_d\/$definition_file");
  $exitcode = $exitcode >> 8;
  if ( $exitcode ne 0 ) {
    push(@failedFiles,$definition_file);
  }
  else{
    push(@passedFiles,$definition_file);
    $passedCount++;
  }
  print "\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n\n";
}

my $failCount = scalar @failedFiles;

print "\n$passedCount out of " . @files . " products registered successfully:\n";
if( $passedCount gt 0 ){
  foreach my $passedFile ( @passedFiles ){
    print "  $passedFile\n";
  }
}
if( $failCount gt 0 ){
  print "\nThe following files failed:\n";
  foreach my $failedFile ( @failedFiles ){
    print "  $failedFile\n";
  }
}
print "\n ===>> $0 Execution completed. <<===\n\n";


