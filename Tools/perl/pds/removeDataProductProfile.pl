#!/usr/bin/env perl

#
# name: removeDataProductProfile.pl
# revised: 20091026 lhf, add prompt for productid
#          20091027 lhf, rename, document, and place under CM control in the NDE_Tools project
#          20091208 lhf, added nc4 removal option w/transaction capability
#          20110225 htp, correct rollback and success checking statements
#          20110706 lhf, rollback only on error condition, not 0 rows returned, NDE-52
#	 	   20130403 dcp, added checks for NC4 products and tied list of possible products to select to product profile tables
#

use Getopt::Std;
getopt('m:t:p');
use DBI;

if (! $opt_m || ! $opt_t ) {
  print "Usage: $0 -m <mode> -t <type 'hdf' or 'nc4'> [-p <productid> <all>]\n";
  exit(1);
}
if (-r "../common/ndeUtilities.pl") {
  require "../common/ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}
my $pw = promptForPassword("\n  Enter $opt_m password: ");

my $dbh = DBI->connect( "dbi:Pg:dbname=nde_dev1;host=nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com", lc($opt_m), $pw )
          or die "\nDatabase Connection Error: $DBI::errstr\n";
$dbh->begin_work;

my $prodID = $opt_p;

if (! $opt_p ) {
  if ($opt_t eq "nc4") {
  	$sth = $dbh->prepare( "select unique pd.PRODUCTID,PRODUCTSHORTNAME 
  								from PRODUCTDESCRIPTION pd, NC4_GROUP ng
  									where pd.productid = ng.productid
  										order by pd.PRODUCTID" ) 
   		or die "Couldn't prepare statement: " . $dbh->errstr;
  }	else {
   	$sth = $dbh->prepare( "select unique pd.PRODUCTID,PRODUCTSHORTNAME 
  								from PRODUCTDESCRIPTION pd, HDF5_GROUP hg
  									where pd.productid = hg.productid
  										order by pd.PRODUCTID" ) 
   		or die "Couldn't prepare statement: " . $dbh->errstr;
  }
  $sth->execute
   or die "Couldn't execute statement: " . $sth->errstr;
  print "\n  No [-p <productid>] entered, choose from the following list or <all>:\n\n";
  print "  ProductId - ProductShortName\n";
  print "  ----------------------------\n";
  while ( my ($pi,$psn) = $sth->fetchrow_array()) {
    print "  $pi - $psn\n";
  }
  print "\n"; 
  exit (1);
} elsif ($opt_p =~ m/all/) {
	if ($opt_t eq "nc4") {
  	$sth = $dbh->prepare( "select unique pd.PRODUCTID 
  								from PRODUCTDESCRIPTION pd, NC4_GROUP ng
  									where pd.productid = ng.productid
  										order by pd.PRODUCTID" ) 
   		or die "Couldn't prepare statement: " . $dbh->errstr;
	  }	else {
	   	$sth = $dbh->prepare( "select unique pd.PRODUCTID
	  								from PRODUCTDESCRIPTION pd, HDF5_GROUP hg
	  									where pd.productid = hg.productid
	  										order by pd.PRODUCTID" ) 
	   		or die "Couldn't prepare statement: " . $dbh->errstr;
	  }
	  $sth->execute
	   or die "Couldn't execute statement: " . $sth->errstr;	
	  $prodID = 0;
}

$dbh->{LongReadLen}=64000;

if ( $prodID ne 0 ) {
	remove_hdf( $dbh, $opt_p ), if ( $opt_t eq "hdf" );
	remove_nc4( $dbh, $opt_p ), if ( $opt_t eq "nc4" );
} else {
	while ($prodID=$sth->fetchrow_array()) {
		remove_hdf( $dbh, $prodID ), if ( $opt_t eq "hdf" );
		remove_nc4( $dbh, $prodID ), if ( $opt_t eq "nc4" );
	}
}


sub remove_hdf {
  my ( $dbh, $productid ) = @_;

  # Abort if product does not exist
  my $sth = $dbh->prepare( qq{ select count(*) from HDF5_GROUP where PRODUCTID = $productid } );
  $sth->execute;
  if ( ! $sth->fetchrow_array() ) {
    print "\nERROR : PRODUCTID: $productid not registered to HDF5 product profile tables, no action taken\n";
    print "$0 : Execution Complete.\n\n";
    exit(1);
  }

  my $sth = $dbh->prepare("select count(*) from ENTERPRISEMEASURE em, MEASURE_H_ARRAY_XREF mhx, HDF5_ARRAY nca, HDF5_GROUP ncg where
  							em.MEASUREID = mhx.MEASUREID and mhx.H_ARRAYID = nca.H_ARRAYID and nca.H_GROUPID = ncg.H_GROUPID
  								and ncg.PRODUCTID = $productid");
  $sth->execute;
  my $count = $sth->fetchrow_array();
  if ($count > 0) {
  	print "\nERROR : PRODUCTID: $productid has enterprise measures registered. Please remove these first with removeEnterpriseMeasures.pl\n";
    print "$0 : Execution Complete.\n\n";
    exit(1);
  }

  eval {
	$dbh->begin_work;
	my $affected;
	$affected = $dbh->do( qq{ delete from HDF5_DIMENSIONLIST
                 where H_ARRAYID in
                  ( select H_ARRAYID
                   from HDF5_ARRAY ha, HDF5_GROUP hg
                    where ha.H_GROUPID = hg.H_GROUPID and PRODUCTID = $productid ) } ) or
	die "No HDF5_DIMENSIONLIST delete for PRODUCTID: $productid\n" . $dbh->errstr;

	$affected = $dbh->do( qq{ delete from MHA_SUBSET
                 where H_ARRAYID in 
                  (select H_ARRAYID
                   from HDF5_ARRAY ha, HDF5_GROUP hg
                    where ha.H_GROUPID = hg.H_GROUPID and PRODUCTID = $productid ) } ) or
	die "No MHA_SUBSET delete for PRODUCTID: $productid\n" . $dbh->errstr; 

	$affected = $dbh->do( qq{ delete from MEASURE_H_ARRAY_XREF
                 where H_ARRAYID in
                  (select H_ARRAYID
                   from HDF5_ARRAY ha, HDF5_GROUP hg
                    where ha.H_GROUPID = hg.H_GROUPID and PRODUCTID = $productid ) } ) or
        die "No MEASURE_H_ARRAY_XREF delete for PRODUCTID: $productid\n" . $dbh->errstr;

	$affected = $dbh->do( qq{ delete from HDF5_ARRAYATTRIBUTE
                 where H_ARRAYID in
                  (select H_ARRAYID
                   from HDF5_ARRAY ha, HDF5_GROUP hg
                    where ha.H_GROUPID = hg.H_GROUPID and PRODUCTID = $productid ) } ) or
        die "No HDF5_ARRAYATTRIBUTE delete for PRODUCTID: $productid\n" . $dbh->errstr;

	$affected = $dbh->do( qq{ delete from HDF5_ARRAY
                 where H_GROUPID in
                  (select H_GROUPID
                    from HDF5_GROUP
                     where PRODUCTID = $productid ) } ) or 
        die "No HDF5_ARRAY delete for PRODUCTID: $productid\n" . $dbh->errstr;

	$affected = $dbh->do( qq{ delete from HDF5_GROUPATTRIBUTE
                 where H_GROUPID in
                  (select H_GROUPID
                    from HDF5_GROUP
                     where PRODUCTID = $productid ) } ) or
        die "No HDF5_GROUPATTRIBUTE delete for PRODUCTID: $productid\n" . $dbh->errstr;
	
	$affected = $dbh->do( qq{ delete from HDF5_GROUP
                 where PRODUCTID = $productid } ) or
        die "No HDF5_GROUP delete for PRODUCTID: $productid\n" . $dbh->errstr;
  };

  if ( $@ ) {
    $dbh->rollback;
    print "ERROR : SQL error, could NOT finish removing PRODUCTID: $productid." . $dbh->errstr;
  } else {
    $dbh->commit;
    print "\nRemoval of PRODUCTID: $productid has been successful.";
  }

  print "\n$0 : Execution Complete.\n\n";
}


sub remove_nc4 {
  my ( $dbh, $productid ) = @_;
  my ( $dimlist_handle, $subset_handle, $xref_handle, $attr_handle, $array_handle, $groupattr_handle, $dim_handle, $group_handle );
  
  # Abort if product does not exist
  my $sth = $dbh->prepare( qq{ select count(*) from NC4_GROUP where PRODUCTID = $productid } );
  $sth->execute;
  if ( ! $sth->fetchrow_array() ) {
    print "\nERROR : PRODUCTID: $productid not registered to NC4 product profile tables, no action taken\n";
    print "$0 : Execution Complete.\n\n";
    exit(1);
  }
  
  my $sth = $dbh->prepare("select count(*) from ENTERPRISEMEASURE em, MEASURE_N_ARRAY_XREF mhx, NC4_ARRAY nca, NC4_GROUP ncg where
  							em.MEASUREID = mhx.MEASUREID and mhx.N_ARRAYID = nca.N_ARRAYID and nca.N_GROUPID = ncg.N_GROUPID
  								and ncg.PRODUCTID = $productid");
  $sth->execute;
  my $count = $sth->fetchrow_array();
  if ($count > 0) {
  	print "\nERROR : PRODUCTID: $productid has enterprise measures registered. Please remove these first with removeEnterpriseMeasures.pl\n";
    print "$0 : Execution Complete.\n\n";
    exit(1);
  }
  
  my $sql = qq{ delete from NC4_DIMENSIONLIST 
                 where N_ARRAYID in  
                  (select N_ARRAYID 
                    from NC4_ARRAY na, NC4_GROUP ng 
                     where na.N_GROUPID = ng.N_GROUPID 
                      and PRODUCTID = ?) };
  $dimlist_handle = $dbh->prepare_cached( $sql );

  $sql = qq{ delete from MNA_SUBSET
                 where N_ARRAYID in 
                  (select N_ARRAYID
                    from NC4_ARRAY na, NC4_GROUP ng
                     where na.N_GROUPID = ng.N_GROUPID
                      and PRODUCTID = ?) };
  $subset_handle = $dbh->prepare_cached( $sql );

  $sql = qq{ delete from MEASURE_N_ARRAY_XREF
                 where N_ARRAYID in 
                  (select N_ARRAYID
                    from NC4_ARRAY na, NC4_GROUP ng
                     where na.N_GROUPID = ng.N_GROUPID
                      and PRODUCTID = ?) };
  $xref_handle = $dbh->prepare_cached( $sql );

  $sql = qq{ delete from NC4_ARRAYATTRIBUTE 
                 where N_ARRAYID in
                  (select N_ARRAYID
                    from NC4_ARRAY na, NC4_GROUP ng
                     where na.N_GROUPID = ng.N_GROUPID
                      and PRODUCTID = ?) };
  $attr_handle = $dbh->prepare_cached( $sql );

  $sql = qq{ delete from NC4_ARRAY
                 where N_GROUPID in 
                  (select N_GROUPID 
                    from NC4_GROUP 
                     where PRODUCTID = ?) };
  $array_handle = $dbh->prepare_cached( $sql );

  $sql = qq{ delete from NC4_GROUPATTRIBUTE
                 where N_GROUPID in
                  (select N_GROUPID
                    from NC4_GROUP
                     where PRODUCTID = ?) };
  $groupattr_handle = $dbh->prepare_cached( $sql );

  $sql = qq{ delete from NC4_DIMENSION
                 where N_GROUPID in
                  (select N_GROUPID
                    from NC4_GROUP
                     where PRODUCTID = ?) };
  $dim_handle = $dbh->prepare_cached( $sql );

  $sql = qq{ delete from NC4_GROUP
                 where PRODUCTID = ? };
  $group_handle = $dbh->prepare_cached( $sql );

  $dbh->{AutoCommit} = 0;
  my $success = 1;
  $success &&= $dimlist_handle->execute( $productid );
  $success &&= $subset_handle->execute( $productid ) or print "some kinda prob " . $subset_handle->errstr;
  $success &&= $xref_handle->execute( $productid );
  $success &&= $attr_handle->execute( $productid );
  $success &&= $array_handle->execute( $productid );
  $success &&= $groupattr_handle->execute( $productid );
  $success &&= $dim_handle->execute( $productid );
  $success &&= $group_handle->execute( $productid );

  if( $success eq 1 ){
    $dbh->commit;
    print "Removal of Data Product Profile: $productid has been successful.\n";
  }
  else{
    $dbh->rollback;
    die "There was an SQL error, could NOT finish removing Data Product Profile.\n" . $dbh->errstr;
  }

};

exit(0);
