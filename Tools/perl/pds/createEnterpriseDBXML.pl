#!/usr/bin/env perl

#
# name: create_nc4_EnterpriseDimensionLists.pl
# revised: 20091222, lhf sort order
#          20110721, lhf add separator
#          20110726, htp added ability to generate XML for specific products only
#		   20110728, d to the c to the p, changed setting for dim size from 'dim_maxindex' to 'dim_minindex'
#		   20110803, htp updated dbh to use SID from environment variable
#		   20120223, d to the c to the p, inserted code for creating xml for all registered Data Product Profiles
#		   20120607, dcp, updated to account for tailoring tailored products (NC4)
#		   20120613, dcp, added to dimension name query so that 4-D arrays can be registered properly
#		   20120618 dcp, added code to use dimension maximum size for dimSize if dimension minimum size is zero (e.g. vector products like Active Fires)
#		   20130326 dcp, added quality flag bit information to em XML - quality flag bit EMs are named like "arrayName_flag1@productShortName", 
#							"arrayName_flag2@productShortName", ... 
#		   20130402 dcp, added array scale reference attribute to em XML 
#		   20130425 dcp, updated to write the maximum array size for dimension storage max size
#		   20140721 dcp, added capability to insert bit mask information
#		   20180319 pgm, postgres port
#

use Getopt::Std;
getopt('m:o:g');

use XML::Simple;
use Data::Dumper;
my $separator = "@";

# Check for command line args

if ( !$opt_m || !$opt_o || !$opt_g ) {
        print "Usage $0 -m <mode> -o <output file path> -g <Y/N? print Granule boundary flag> <[productID 1] [productID 2] ... [productID n] or [all]>\n";
        exit(1);
}

open ( OUTFILE, ">$opt_o" ) or die "ERROR Error opening output file $opt_o, exiting...\n";

$opt_g=uc $opt_g;

my @files = @ARGV;

use DBI;
if (-r "../common/ndeUtilities.pl") {
  require "../common/ndeUtilities.pl";
} else {
  require "/opt/apps/nde/$opt_m/common/ndeUtilities.pl";
}
my $pw = promptForPassword("\n  Enter $opt_m password: ");
my $dbh = DBI->connect("dbi:Pg:dbname=nde_dev1;host=nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com", lc($opt_m), $pw) ;my $h5From  = "";
my $h5Where = "";
my $ncFrom  = "";
my $ncWhere = "";

my $ncIDs = "";
my $h5IDs = "";

print "\nCreating XML... ";

my $dimRef = "dim_minindex";

#Process all loaded profiles for HDF5 and NC4
if ($files[0] =~ m/all/){
	my $sth = $dbh->prepare("select distinct PRODUCTID from HDF5_GROUP");
	$sth->execute;
	my $i=0;
	while (my $id = $sth->fetchrow_array()) {
		$files[$i] = $id;
		$i++;		
	}
	$sth = $dbh->prepare("select distinct PRODUCTID from NC4_GROUP");
	$sth->execute;
	while (my $id = $sth->fetchrow_array()) {
		$files[$i] = $id;
		$i++;		
	}
}

if( @files ){
	
	foreach my $productID ( @files ){
		#print $productID . ", ";
		my $sth = $dbh->prepare("select PRODUCTFILENAMEEXTENSION from PRODUCTDESCRIPTION where PRODUCTID = $productID");
		$sth->execute;
		my $pfne = $sth->fetchrow_array();
		if ( $pfne eq "h5" ){
			$h5IDs = $h5IDs . $productID . ",";
			#print $h5IDs . "\n";
		}
		elsif( $pfne eq "nc" ){
			$ncIDs = $ncIDs . $productID . ",";
			#print $ncIDs . "\n";
		}
		elsif ( $pfne eq "" ){
			print "ERROR: Product filename extension undefined or Invalid product ID entered";
			exit 1;
		}
		else {
			print "ERROR: Product filename extension ($pfne) for Product ID $productID is not supported (nc or h5 only)";
			exit 1;
		}
	}
	
	$allIDs = $h5IDs . $ncIDs;
	chop( $allIDs );
	
	$h5From =  ", HDF5_GROUP hg ";
	$h5Where = " and hg.H_GROUPID = a.H_GROUPID and hg.PRODUCTID in ($allIDs) ";
	$ncFrom =  ", NC4_GROUP ng ";
	$ncWhere = "  and ng.N_GROUPID = na.N_GROUPID and ng.PRODUCTID in ($allIDs) ";

}

# retrieve dimension by array 
my $sql = "
select * from
(select
        rtrim( (regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',' ) )[1] ) dimName,
        rtrim( (regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',' ) )[1] ) dimSize,
        rtrim( (regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',' ) )[1] ) dimDP,
        rtrim( (regexp_split_to_array ( aa4.h_arrayAttributeStringValue || ',', ',' ) )[1] ) dimMaxSize
from
        HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAYATTRIBUTE aa4, HDF5_ARRAY a, HDF5_GROUP hg
where aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa4.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = 'dim_minindex'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb' 
        and aa4.H_ARRAYATTRIBUTENAME = 'dim_maxindex'" . $h5Where . "
union
select
        rtrim( (regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',' ) )[2] ) dimName,
        rtrim( (regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',' ) )[2] ) dimSize,
        rtrim( (regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',' ) )[2] ) dimDP,
        rtrim( (regexp_split_to_array ( aa4.h_arrayAttributeStringValue || ',', ',' ) )[2] ) dimMaxSize
from
        HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAYATTRIBUTE aa4, HDF5_ARRAY a, HDF5_GROUP hg
where aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa4.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = 'dim_minindex'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb' 
        and aa4.H_ARRAYATTRIBUTENAME = 'dim_maxindex'" . $h5Where . "
union
select
        rtrim( (regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',' ) )[3] ) dimName,
        rtrim( (regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',' ) )[3] ) dimSize,
        rtrim( (regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',' ) )[3] ) dimDP,
        rtrim( (regexp_split_to_array ( aa4.h_arrayAttributeStringValue || ',', ',' ) )[3] ) dimMaxSize
from
        HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAYATTRIBUTE aa4, HDF5_ARRAY a, HDF5_GROUP hg
where aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa4.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = 'dim_minindex'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb' 
        and aa4.H_ARRAYATTRIBUTENAME = 'dim_maxindex'" . $h5Where . "
union
select
        rtrim( (regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',' ) )[4] ) dimName,
        rtrim( (regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',' ) )[4] ) dimSize,
        rtrim( (regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',' ) )[4] ) dimDP,
        rtrim( (regexp_split_to_array ( aa4.h_arrayAttributeStringValue || ',', ',' ) )[4] ) dimMaxSize
from
        HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAYATTRIBUTE aa4, HDF5_ARRAY a, HDF5_GROUP hg
where aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa4.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = 'dim_minindex'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb' 
        and aa4.H_ARRAYATTRIBUTENAME = 'dim_maxindex'" . $h5Where . "
union
select distinct
        N_DIMENSIONNAME,
        cast( N_DIMENSIONSIZE as varchar(255)),
        cast( N_DATAPARTITION as varchar(255)),
        cast( N_DIMENSIONMAXIMUMSIZE as varchar(255))
from
        NC4_DIMENSIONLIST ndl,
        NC4_DIMENSION nd,
        NC4_ARRAY na" . $ncFrom . "
where
        ndl.N_ARRAYID=na.N_ARRAYID
        and ndl.N_DIMENSIONID=nd.N_DIMENSIONID" . $ncWhere . "
) t
where t.dimName != ''";

#print $sql;

my $sth = $dbh->prepare($sql);
$sth->execute;

my %enterpriseDB=();
while ( my ( $dimName, $dimSize, $dimDP, $dimMaxSize ) = $sth->fetchrow_array() ) {
	my $enterpriseDimName="";
	if ($dimName =~ m/\-/) {$dimName =~ s/\-.*$//}; #If a dimension name already has dimension size in name, delete it so no duplication in name
#	if ($dimSize =~ m/^0/) {
#		$dimSize = $dimMaxSize;
#		$dimRef = 'dim_maxindex';
#	} #We do not want dimension sizes of zero (e.g. VIIRS-AF-EDR)
	$dimName =~ s/\s//g; #Cannot have white space in dimension name
	$enterpriseDimName="$dimName~$dimSize~$dimDP"; 
	$enterpriseDB{'EnterpriseDimension'}{$enterpriseDimName}{'StorageSize'}=$dimSize;	
	$enterpriseDB{'EnterpriseDimension'}{$enterpriseDimName}{'StorageMaxSize'}=$dimMaxSize;	
	$enterpriseDB{'EnterpriseDimension'}{$enterpriseDimName}{'Start'}=0;	
	$enterpriseDB{'EnterpriseDimension'}{$enterpriseDimName}{'Interval'}=1;
	#$enterpriseDB{'EnterpriseDimension'}{$enterpriseDimName}{'Interval'}=1;
	if ($dimDP == 1) {
		$enterpriseDB{'EnterpriseDimension'}{$enterpriseDimName}{'End'}='*';	
	}
	else {
		$enterpriseDB{'EnterpriseDimension'}{$enterpriseDimName}{'End'}=$dimSize;	
	}
}

$dimRef = 'dim_minindex'; #reinitialize

# Get the Enterprise Measure Names

$sql="
select distinct
        dim1, dim2, dim3, dim4, dim5, rank
from
(
        Select t1.h_arrayId,
        cast(t1.h_arrayName as varchar(255)),
        cast(t1.h_dataType as varchar(255)),
        rank,
        cast (
        (select
        rtrim( ( regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',') )[1] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',') )[1] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',') )[1])
                from
                HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAY a
                where
		a.h_arrayId = t1.h_arrayId and
        aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = '$dimRef'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb'
        )
        as varchar(190)) as dim1,
        cast (
        (select
        rtrim( ( regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',') )[2] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',') )[2] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',') )[2])
        from
         HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAY a
        where
                a.h_arrayId = t1.h_arrayId and
                aa1.H_ARRAYID=a.H_ARRAYID
                and aa2.H_ARRAYID=a.H_ARRAYID
                and aa3.H_ARRAYID=a.H_ARRAYID
                and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
                and aa2.H_ARRAYATTRIBUTENAME = '$dimRef'
                and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb'
        ) as varchar(190))as dim2,
        cast (
        (select
        rtrim( ( regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',') )[3] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',') )[3] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',') )[3])
                from
                HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAY a
                where
                        a.h_arrayId = t1.h_arrayId and
        aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = '$dimRef'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb'
        )
        as varchar(190)) as dim3,
        cast (
        (select
        rtrim( ( regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',') )[4] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',') )[4] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',') )[4])
                from
                HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAY a
                where
                        a.h_arrayId = t1.h_arrayId and
        aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = '$dimRef'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb'
        )
        as varchar(190)) as dim4,
        cast (
        (select
        rtrim( ( regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',') )[5] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',') )[5] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',') )[5])
                from
                HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAY a
                where
                        a.h_arrayId = t1.h_arrayId and
        aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = '$dimRef'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb'
        )
        as varchar(190)) as dim5
from
(select a.h_arrayId, h_arrayName, h_dataType, count(*) as rank
from
        hdf5_array a, hdf5_dimensionList l" . $h5From . "
where
        a.h_arrayId = l.h_arrayId" . $h5Where . "
group by
        a.h_arrayId, h_arrayName, h_DataType
) t1
) t2
union
select distinct dim1, dim2, dim3, dim4, dim5, rank
from (
select n_arrayId, n_arrayName, n_DataType, rank,
        cast(
                (select n_dimensionName||'~'||n_DimensionSize||'~'||n_DataPartition
                from nc4_DimensionList dl, nc4_Dimension d
                 where dl.n_dimensionOrder = 1 and dl.n_ArrayId = t1.N_ArrayId
                        and dl.n_DimensionId = d.n_DimensionId)
                as varchar(190)) as dim1,
        cast(
                (select n_dimensionName||'~'||n_DimensionSize||'~'||n_DataPartition
                from nc4_DimensionList dl, nc4_Dimension d
                 where dl.n_dimensionOrder = 2 and dl.n_ArrayId = t1.N_ArrayId
                        and dl.n_DimensionId = d.n_DimensionId)
                as varchar(190))as dim2,
        cast(
                (select n_dimensionName||'~'||n_DimensionSize||'~'||n_DataPartition
                from nc4_DimensionList dl, nc4_Dimension d
                 where dl.n_dimensionOrder = 3 and dl.n_ArrayId = t1.N_ArrayId
                        and dl.n_DimensionId = d.n_DimensionId)
                as varchar(190))as dim3,
        cast(
                (select n_dimensionName||'~'||n_DimensionSize||'~'||n_DataPartition
                from nc4_DimensionList dl, nc4_Dimension d
                 where dl.n_dimensionOrder = 4 and dl.n_ArrayId = t1.N_ArrayId
                        and dl.n_DimensionId = d.n_DimensionId)
                as varchar(190))as dim4,
        cast(
                (select n_dimensionName||'~'||n_DimensionSize||'~'||n_DataPartition
                from nc4_DimensionList dl, nc4_Dimension d
                 where dl.n_dimensionOrder = 5 and dl.n_ArrayId = t1.N_ArrayId
                        and dl.n_DimensionId = d.n_DimensionId)
                as varchar(190))as dim5
from
(select
        na.n_arrayId, n_arrayName, n_dataType, count(*) as rank
from
        NC4_DIMENSIONLIST ndl,
        NC4_DIMENSION nd,
        NC4_ARRAY na" . $ncFrom . "
where
        ndl.N_ARRAYID=na.N_ARRAYID and ndl.N_DIMENSIONID=nd.N_DIMENSIONID" . $ncWhere . "
group by
        na.n_arrayId,
        n_arrayName,
        n_dataType
) t1
) t2";

#print "$sql\n";

$sth = $dbh->prepare($sql);
$sth->execute;
while ( my ( $dim1, $dim2, $dim3, $dim4, $dim5, $rank ) = $sth->fetchrow_array() ) {

	# if not reviewing chop of the Granule Boundary Flag
	if ($opt_g eq 'N') {
		$dim1 =~ s/\-.$//;
		$dim2 =~ s/\-.$//;
		$dim3 =~ s/\-.$//;
		$dim4 =~ s/\-.$//;
		$dim5 =~ s/\-.$//;
	}
	my @tempDim=($dim1, $dim2, $dim3, $dim4, $dim5);
	my $EnterpriseDimensionGroupName="";
	for (my $i=0; $i<$rank; $i++) {
		if ($tempDim[$i] ne "--") {
			if ($tempDim[$i] =~ m/\-/) {$tempDim[$i] =~ s/\-.*?\~/~/;} #if dimension name already has dimension size, then delete (added later)
			$tempDim[$i] =~ s/\s//g;
			$EnterpriseDimensionGroupName.= $separator . $tempDim[$i];
		}
	}
	$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'dim'}{'rank'}=$rank;
	for (my $i=0; $i<$rank; $i++) {
		$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'dim'}{'list'}[$i]=$tempDim[$i];
	}
}


$sql="
Select t1.h_arrayId,
        cast(t1.h_arrayName as varchar(255)),
	cast((select productShortName
                from productDescription where productId in
                (select productId from hdf5_group g, hdf5_array a
                        where a.h_arrayId = t1.h_arrayId and g.h_groupId = a.h_groupId))
                                        as varchar(255)),
	cast(t1.h_dataType as varchar(255)),
        rank,
        cast (
        (select
        rtrim( ( regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',') )[1] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',') )[1] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',') )[1])
                from
                HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAY a
                where
                        a.h_arrayId = t1.h_arrayId and
        aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = '$dimRef'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb'
        )
        as varchar(190)) as dim1,
        cast (
        (select
        rtrim( ( regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',') )[2] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',') )[2] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',') )[2])
        from
         HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAY a
        where
                a.h_arrayId = t1.h_arrayId and
                aa1.H_ARRAYID=a.H_ARRAYID
                and aa2.H_ARRAYID=a.H_ARRAYID
                and aa3.H_ARRAYID=a.H_ARRAYID
                and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
                and aa2.H_ARRAYATTRIBUTENAME = '$dimRef'
                and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb'
        ) as varchar(190))as dim2,
        cast (
        (select
        rtrim( ( regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',') )[3] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',') )[3] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',') )[3])
                from
                HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAY a
                where
                        a.h_arrayId = t1.h_arrayId and
        aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = '$dimRef'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb'
        )
        as varchar(190)) as dim3,
        cast (
        (select
        rtrim( ( regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',') )[4] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',') )[4] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',') )[4])
                from
                HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAY a
                where
                        a.h_arrayId = t1.h_arrayId and
        aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = '$dimRef'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb'
        )
        as varchar(190)) as dim4,
        cast (
        (select
        rtrim( ( regexp_split_to_array ( aa1.h_arrayAttributeStringValue || ',', ',') )[5] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa2.h_arrayAttributeStringValue || ',', ',') )[5] ) ||'~'||
        rtrim( ( regexp_split_to_array ( aa3.h_arrayAttributeStringValue || ',', ',') )[5])
                from
                HDF5_ARRAYATTRIBUTE aa1, HDF5_ARRAYATTRIBUTE aa2, HDF5_ARRAYATTRIBUTE aa3, HDF5_ARRAY a
                where
                        a.h_arrayId = t1.h_arrayId and
        aa1.H_ARRAYID=a.H_ARRAYID
        and aa2.H_ARRAYID=a.H_ARRAYID
        and aa3.H_ARRAYID=a.H_ARRAYID
        and aa1.H_ARRAYATTRIBUTENAME = 'dim_name'
        and aa2.H_ARRAYATTRIBUTENAME = '$dimRef'
        and aa3.H_ARRAYATTRIBUTENAME = 'dim_gb'
        )
        as varchar(190)) as dim5
from
(select a.h_arrayId, h_arrayName, h_dataType, count(*) as rank
from
        hdf5_array a, hdf5_dimensionList l" . $h5From . "
where
        a.h_arrayId = l.h_arrayId" . $h5Where . "
group by
        a.h_arrayId, h_arrayName, h_DataType
) t1
union
select n_arrayId, 
	   n_arrayName,
	   cast((select productShortName
                from productDescription where productId in
                (select productId from nc4_group g, nc4_array a
                        where a.n_arrayId = t1.n_arrayId and g.n_groupId = a.n_groupId))
                                        as varchar(255)), 
	   n_DataType, 
	   rank,
        cast(
                (select n_dimensionName||'~'||n_DimensionSize||'~'||n_DataPartition
                from nc4_DimensionList dl, nc4_Dimension d
                 where dl.n_dimensionOrder = 1 and dl.n_ArrayId = t1.N_ArrayId
                        and dl.n_DimensionId = d.n_DimensionId)
                as varchar(190)) as dim1,
        cast(
                (select n_dimensionName||'~'||n_DimensionSize||'~'||n_DataPartition
                from nc4_DimensionList dl, nc4_Dimension d
                 where dl.n_dimensionOrder = 2 and dl.n_ArrayId = t1.N_ArrayId
                        and dl.n_DimensionId = d.n_DimensionId)
                as varchar(190))as dim2,
        cast(
                (select n_dimensionName||'~'||n_DimensionSize||'~'||n_DataPartition
                from nc4_DimensionList dl, nc4_Dimension d
                 where dl.n_dimensionOrder = 3 and dl.n_ArrayId = t1.N_ArrayId
                        and dl.n_DimensionId = d.n_DimensionId)
                as varchar(190))as dim3,
        cast(
                (select n_dimensionName||'~'||n_DimensionSize||'~'||n_DataPartition
                from nc4_DimensionList dl, nc4_Dimension d
                 where dl.n_dimensionOrder = 4 and dl.n_ArrayId = t1.N_ArrayId
                        and dl.n_DimensionId = d.n_DimensionId)
                as varchar(190))as dim4,
        cast(
                (select n_dimensionName||'~'||n_DimensionSize||'~'||n_DataPartition
                from nc4_DimensionList dl, nc4_Dimension d
                 where dl.n_dimensionOrder = 5 and dl.n_ArrayId = t1.N_ArrayId
                        and dl.n_DimensionId = d.n_DimensionId)
                as varchar(190))as dim5
from
(select
        na.n_arrayId, n_arrayName, n_dataType, count(*) as rank
from
        NC4_DIMENSIONLIST ndl,
        NC4_DIMENSION nd,
        NC4_ARRAY na" . $ncFrom . "
where
        ndl.N_ARRAYID=na.N_ARRAYID and ndl.N_DIMENSIONID=nd.N_DIMENSIONID" . $ncWhere . "
group by
        na.n_arrayId,
        n_arrayName,
        n_dataType
) t1";

print $sql;

$sth = $dbh->prepare($sql);
$sth->execute;

while ( my ( $n_ArrayId, $n_ArrayName, $n_productShortName, $n_ArrayDataType, $rank, $dim1, $dim2, $dim3, $dim4, $dim5 ) = $sth->fetchrow_array() ) {
	my @tempDim=($dim1, $dim2, $dim3, $dim4, $dim5);
	my $EnterpriseDimensionGroupName="";
	for (my $i=0; $i<$rank; $i++) {
		if ($tempDim[$i] ne "--") {
			if ($tempDim[$i] =~ m/\-/) {$tempDim[$i] =~ s/\-.*?\~/~/;} #if dimension name already has dimension size, then delete (added later)
			$tempDim[$i] =~ s/\s//g;
			$EnterpriseDimensionGroupName.= $separator . $tempDim[$i];
		}
	}
	
	#Add product file name extension
	my $sql = "select productfilenameextension from productdescription where productshortname = '$n_productShortName'";
	my $sth = $dbh->prepare($sql);
	$sth->execute;
	my $productExt = $sth->fetchrow_array();
	
#	if ($n_ArrayName =~ m/$separator/) { #Check if array name already has product short name (e.g. tailoring already tailored products)
#		$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$n_ArrayName}{'dataType'}=$n_ArrayDataType;
#		$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$n_ArrayName}{'productExt'}=$productExt;
#	} else {
	$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$n_ArrayName.$separator.$n_productShortName}{'dataType'}=$n_ArrayDataType;
	$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$n_ArrayName.$separator.$n_productShortName}{'productExt'}=$productExt;
#	}

	#Retrieve IDPS and NUP xDR quality flag measures and factors array reference where applicable
	if ($productExt eq 'h5') {
		#bit offset...
		my $sql = "select h_arrayattributename,h_arrayattributestringvalue from hdf5_arrayattribute 
				where h_arrayid = $n_ArrayId and h_arrayattributename like '%bit_offset'";
		my $sth = $dbh->prepare($sql);
		$sth->execute;
		while ( my ($hAttributeName,$bitOffset) = $sth->fetchrow_array()) {
			$hAttributeName =~ m/(^.+flag\d+)/;
			my $measureName = $1;
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$measureName.$separator.$n_productShortName}{'dataType'}=$n_ArrayDataType;
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$measureName.$separator.$n_productShortName}{'productExt'}=$productExt;
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$measureName.$separator.$n_productShortName}{'transform'}="bit";
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$measureName.$separator.$n_productShortName}{'bitOffset'}=$bitOffset;
		}
		#bit length...
		my $sql = "select h_arrayattributename,h_arrayattributestringvalue from hdf5_arrayattribute 
						where h_arrayid = $n_ArrayId and h_arrayattributename like '%bit_length'";
		my $sth = $dbh->prepare($sql);
		$sth->execute;
		while ( my ($hAttributeName,$bitLength) = $sth->fetchrow_array()) {
			$hAttributeName =~ m/(^.+flag\d+)/;
			my $measureName = $1;
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$measureName.$separator.$n_productShortName}{'bitLength'}=$bitLength;
		}
		#Add scale/offset (factors) array reference
		my $sql = "select h_arrayattributename,h_arrayattributestringvalue from hdf5_arrayattribute 
						where h_arrayid = $n_ArrayId and h_arrayattributename like 'add_offset'";
		my $sth = $dbh->prepare($sql);
		$sth->execute;
		while (my $factors_reference = $sth->fetchrow_array()) {
			#print "$factors_reference\n";
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$n_ArrayName.$separator.$n_productShortName}{'scaleRef'}=$factors_reference;
		}
	} else { #NC4 bit masks
		#bit offset...
		my $sql = "select n_arrayattributename,n_arrayattributestringvalue from nc4_arrayattribute 
				where n_arrayid = $n_ArrayId and n_arrayattributename like '%bit_offset'";
		my $sth = $dbh->prepare($sql);
		$sth->execute;
		while ( my ($nAttributeName,$bitOffset) = $sth->fetchrow_array()) {
			$nAttributeName =~ m/(^.+flag\d+)/;
			my $measureName = $1;
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$measureName.$separator.$n_productShortName}{'dataType'}='byte'; #set all bit masks to byte
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$measureName.$separator.$n_productShortName}{'productExt'}=$productExt;
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$measureName.$separator.$n_productShortName}{'transform'}="bit";
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$measureName.$separator.$n_productShortName}{'bitOffset'}=$bitOffset;
		}
		#bit length...
		my $sql = "select n_arrayattributename,n_arrayattributestringvalue from nc4_arrayattribute 
						where n_arrayid = $n_ArrayId and n_arrayattributename like '%bit_length'";
		my $sth = $dbh->prepare($sql);
		$sth->execute;
		while ( my ($nAttributeName,$bitLength) = $sth->fetchrow_array()) {
			$nAttributeName =~ m/(^.+flag\d+)/;
			my $measureName = $1;
			$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroupName}{'Measure'}{$measureName.$separator.$n_productShortName}{'bitLength'}=$bitLength;
		}
	}
	
#	}
}

#print Dumper(%enterpriseDB);

# Dump out the Enterprise XML Structure
my $date = `date +%m/%d/%Y`; chomp($date);

print OUTFILE "<!-- " . $date . " -->\n";
print OUTFILE "<!-- Enterprise Measure DB XML for: -->\n";
if( $h5IDs ne "" ){	chop($h5IDs); print OUTFILE "<!-- HDF5 ProductIDs: $h5IDs -->\n" }
if( $ncIDs ne "" ){	chop($ncIDs); print OUTFILE "<!-- NetCDF ProductIDs: $ncIDs -->\n" }
if( $h5IDs ne "" && $ncIDs ne "" ){	print OUTFILE "<!-- All Products: $allIDs -->\n"; }

print OUTFILE "<ndeEnterpriseSchema>\n";
print OUTFILE "\t<EnterpriseDimensions>\n";
for my $dimension ( sort keys %{$enterpriseDB{'EnterpriseDimension'}} ) {
	my $dimension_print = $dimension;
	print "Debug dim=$dimension, ";
	$dimension_print =~ s/\~/\-/g;
	print " dimprt=$dimension_print<<<<\n";
	if ($opt_g eq "N") { $dimension_print =~ s/\-.$//g; }
	my $tag=qq{<EnterpriseDimension name="$dimension_print" }; 

	$tag .= qq{start="$enterpriseDB{'EnterpriseDimension'}{$dimension}{'Start'}" };
	$tag .= qq{interval="$enterpriseDB{'EnterpriseDimension'}{$dimension}{'Interval'}" };
	$tag .= qq{end="$enterpriseDB{'EnterpriseDimension'}{$dimension}{'End'}" };
	$tag .= qq{storagesize="$enterpriseDB{'EnterpriseDimension'}{$dimension}{'StorageSize'}" };
	$tag .= qq{storageMaxSize="$enterpriseDB{'EnterpriseDimension'}{$dimension}{'StorageMaxSize'}" };
	$tag .= qq{\/>};
	print OUTFILE "\t\t$tag\n";
}
print OUTFILE "\t</EnterpriseDimensions>\n";

print OUTFILE "\t<EnterpriseDimensionGroups>\n";
for my $EnterpriseDimensionGroup ( sort keys %{$enterpriseDB{'EnterpriseDimensionGroup'}} ) {
        $edg = substr $EnterpriseDimensionGroup, 1;
	my @edg_print = split($separator, $edg);
	for (my $i=0; $i <= $#edg_print; $i++) {
		$edg_print[$i] =~ s/\~/\-/g;
		if ($opt_g eq "N") { $edg_print[$i] =~ s/\-.$//g; }
	}
	$edg = join($separator, @edg_print);
	print OUTFILE qq{\t\t<EnterpriseDimensionGroup name="$edg" >\n};
	print OUTFILE qq{\t\t\t<OrderedDimensions>\n};
	for (my $i=0; $i < $enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'dim'}{'rank'}; $i++) {
		my $dimName=$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'dim'}{'list'}[$i];
		my @dimInfo=split ("~", $dimName);
		$dimName =~ s/\~/\-/g;
		if ($opt_g eq "N") { $dimName =~ s/\-.$//g; }
		my $tag =qq{\t\t\t\t<OrderedDimension name="$dimName" };
		my $dimensionOrder=$i+1;
		$tag.=qq{e_DimensionOrder="$dimensionOrder" };
		$tag.=qq{e_dimensionDataPartition="$dimInfo[2]" };
		print OUTFILE "$tag\/>\n";
	}
	print OUTFILE qq{\t\t\t</OrderedDimensions>\n};

	# Print out XML for the Enterprise Measures
	print OUTFILE "\t\t\t<Measures>\n";
	for my $measure ( sort keys %{$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'Measure'}} ) {
		my $tmeasure = $enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'Measure'}{$measure}{'transform'};
		if ($tmeasure eq "bit") {
			$tag=qq{\t\t\t\t<Measure name="$measure" dataType="$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'Measure'}{$measure}{'dataType'}" }
			. qq{productExt="$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'Measure'}{$measure}{'productExt'}" }
			. qq{transform="$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'Measure'}{$measure}{'transform'}" }
			. qq{bitOffset="$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'Measure'}{$measure}{'bitOffset'}" }
			. qq{bitLength="$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'Measure'}{$measure}{'bitLength'}" />};
		} else {
			$tag=qq{\t\t\t\t<Measure name="$measure" dataType="$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'Measure'}{$measure}{'dataType'}" productExt="$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'Measure'}{$measure}{'productExt'}" scaleRef="$enterpriseDB{'EnterpriseDimensionGroup'}{$EnterpriseDimensionGroup}{'Measure'}{$measure}{'scaleRef'}"/>};
		}
		print OUTFILE "$tag\n";
	}

	print OUTFILE "\t\t\t</Measures>\n";

	# Print the closing data cube tag
	print OUTFILE "\t\t</EnterpriseDimensionGroup>\n";
}

# Close Data Cubes Tag
print OUTFILE "\t</EnterpriseDimensionGroups>\n";

# Close first tag
print OUTFILE "</ndeEnterpriseSchema>\n";

#print Dumper(\%enterpriseDB);

close (OUTFILE);
print "\nXML created: $opt_o\n";
exit();
