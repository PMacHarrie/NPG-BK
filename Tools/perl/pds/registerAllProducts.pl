#!/usr/bin/env perl
my $filecount = $#files + 1;

#
# name: registerAllProducts.pl
# purpose: given a directory containing _Definition files, register them all 
# note: currently used for just HDF5 products from IDPS
# revised: 20100108 lhf, creation
#          20110126 htp, update to use with new registerProduct.pl script
#          20110314 htp, hardcode path to registerProduct and print out success files
#          20141103 teh, added -r to recursively register sub directories

use Getopt::Std;

getopts('m:d:r');

if ( ! $opt_m || ! $opt_d ) {
	print "Usage $0 -m <mode> -d <directory (full of Definition files)> [-r]\n";
        print "   Optional -r recursively registers the contents of all directories in -d.\n";
	exit(1);
}

if (-r "../common/ndeUtilities.pl") {
  require "../common/ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}

print "\n ===>> $0 Execution started... <<=== \n\n";

my @files = `ls $opt_d`;
my @failedFiles;
my @passedFiles;
my $passedCount = 0;
my $pw = promptForPassword("\n  Enter $opt_m password: ");
my $filecount = $#files + 1;

$ENV{NDE_DB_KEY} = $pw; 
#Note: this consumes @files:
my $definition_file;
while ( @files ) { #went to while/shift vs foreach to make adding on to the end "safe" for future perl implementations.
  $definition_file = shift @files; #sometimes definition_file might be "subdir/filename" vs just filename.  See below.
  chomp( $definition_file );
  if ($opt_r && -d "$opt_d/$definition_file") { #didn't just recursively call this script to simplify the end summary.
    print "Adding contents of directory $opt_d/$definition_file to end of file list\n";
    my @subfiles = `ls $opt_d/$definition_file`;
    $filecount--; #the folder doesn't count as a product file...
    foreach my $subfile (@subfiles) {
      push(@files,"$definition_file/$subfile"); #And if THAT's a folder, it will be processed as such in turn.
      $filecount++; #...but the files in the subfolder do!
    }
  } else {
    print "Processing $opt_d/$definition_file...\n";
    my $exitcode = system("/opt/apps/nde/$opt_m/pds/registerProduct.pl -m $opt_m -f $opt_d/$definition_file");
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
}

my $failCount = scalar @failedFiles;

print "\n$passedCount out of " . $filecount . " products registered successfully:\n";
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


