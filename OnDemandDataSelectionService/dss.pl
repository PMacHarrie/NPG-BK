#!/usr/bin/env perl

#
# name: dss.pl (Data Selection Service)
#
# Input
#	PCF  (used to create output fileName).
#	JobId	(last name of current director, e.g. /opt/../working/27 jobId=27)
#	Tailoring XML command (gotten via Job's Production Rule)
#	The data files to be tailored (Job Input Speicification Table via JobId)
# Output
#	A file that is the result of the XML command
#	PSF (the name of the created file.)
#
# revised: 20100219 tjf, add output file name from product short name + time range
#          20100219 tjf, remove hard-coded modes
#          20100223 tjf, Added integer and short integer datatypes, fixed double to = 64bit, added 32bit float
#          20100305 pgm, Changed initMask run to use $command, $Mode concat doesn't work inside `...`.
#          20100407 tjf, Added support fill value, takes from PRDATASELECTIONXML apply to only measures will fill value specified
#          20100420 add processing for scaled="1" in prdataselectionxml
#          20100420 add processing for scaled="1" in prdataselectionxml
#          20110614 dcp, update to adhere to file naming conventions
#          20110624 lhf, add log lines
#		   20110815 htp, update to use GMT instead of localtime
#		   20120322 dcp, added error code trapping for calls to c executables and DBI (die)
#		   20120519 dcp, corrected concatenation (translate timestamp to 24 hours and sort on keys)
#						  so that granules are in proper order for temporal/orbital rules
#	       20120605 dcp, Added changes to allow for NC4 data types (enable tailoring of NC4 files)
#		   20120612 dcp, Updated the way count is calculated when stride is over an entire array so that the start element is included in subsamples
#		   20121009 dcp, Added check/fix to make sure netcdf c code is called with uchar data type instead of ubyte
#		   20121024 dcp, added partition id to mask file name for uniqueness when filtering data
#		   20121205 dcp, added capability to specify output array names using xml attribute in production rule (outputArrayName=value)
#		   20130328 dcp, added fix to filter using quality flag bit masks
#		   20130423 dcp, enabled dynamic array aggregation using actualDimSize from h5dump/ncdump
#          20130515 teh, use encrypted access vs hardcoded cleartext pwd for db access
#		   20140128 dcp, removed 2nd instance of "my" from line 678 - causing warnings
#		   20140514 dcp, added capability to specify dimension name in production rule for writing to NC4 file (user request)
#		   20141027 dcp, added capability to execute unique formatting scripts (wmoHeader.pl, ghrsst_formatter.pl, etc.)
#		   20171002 jrh, added check to ensure the fill value of a netCDF-4 array is scaled only if a scale and/or offset is defined. Also made it so that result{...}{'Measure'}{$measureName}{'scaled'} is set to 1 only if a scale and/or offset is defined.
#		   20180112 pgm, changed db queries to use bind variables for better performance
#		   20180831 jrh, fixed some DB queries which initialize the "input" structure
#		   20180927 jrh, extensive modified to become DSS On-demand.

use FileHandle;
use Sys::Syslog;
use diagnostics;
use Benchmark;
use Data::Dumper;
use JSON;
##`ulimit -s unlimited`;


#
# Do PCF stuff
#

# get name and mode
my $commandLine = $0;
chomp($commandLine);
my @list = split('/',$commandLine);
my $Name = $list[-1];
my $PROGRAM = $Name;
my $Mode = $ENV{'mode'};

print "Debug mode=$Mode<<<\n";

my $log_file = "dss.pl.log";
`echo Begin Execution of dss.pl >> $log_file`;
`date +"%Y-%m-%d %T" >> $log_file`;
`echo $Mode >> $log_file`;


print "logfile=$log_file<<<<\n";

open (STDOUT, ">>$log_file");
open (STDERR, ">&" . STDOUT);

my $rc=system("/opt/apps/nde/" . $Mode . "/pgs/dss/init_nc4_file.pl");
doErrorCheck($rc,"init_nc4_file"); 

require "/opt/apps/nde/" . $Mode . "/pgs/dss/init_nc4_file.pl";

#
# Input Parameters
#
my $job_coverage_start="";
my $job_coverage_end="";
my $current_directory="";
my $formatter="";

#
# Output Parameters
#

open(PCF,"<${PROGRAM}.PCF") or die "Can't open PCF file\n";
my @Line_List = <PCF>;
foreach my $line(@Line_List){
  if($line=~m/=/){
    chomp(my ($Keyword_And_Argument,$Value)=split('=',$line));
    $Value=~s/^\s+//;
    $Value=~s/\s+$//;
    $job_coverage_start = $Value, if ($Keyword_And_Argument=~m/job_coverage_start/);
    $job_coverage_end = $Value, if ($Keyword_And_Argument=~m/job_coverage_end/);
    $current_directory = $Value, if ($Keyword_And_Argument=~m/working_directory/);
    $formatter = $Value, if ($Keyword_And_Argument=~m/formatter/);
  }
}
`echo "start=$job_coverage_start" >> $log_file`;
`echo "end=$job_coverage_end" >> $log_file`;
`echo "dir=$current_directory" >> $log_file`;

chomp($current_directory);
my @dirs=split('/', $current_directory);
my $jobId=$dirs[-1];
`echo "jobId=$jobId" >> $log_file`;
close(PCF);

# XML Simple

use XML::Simple ;
use Data::Dumper;

#Get current time

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=gmtime;

$year += 1900;

$mon = $mon+1;
if ($mon < 10){ $mon = "0$mon"; }

if ($mday < 10){ $mday = "0$mday" }

if ($hour < 10){ $hour = "0$hour"; }

if ($min < 10){ $min = "0$min"; }
    
if ($sec < 10){ $sec = "0$sec"; }


# Connect to Oracle
use DBI;
my $dbh = connectNDEDB($Mode) ;

# Get input spec JSON
my $sql=qq{select ODJOBSPEC from ONDEMANDJOB where ODJOBID = ?};
my $sth = $dbh->prepare($sql);
$sth->execute($jobId);

my $odJobSpecJSON = $sth->fetchrow_array();
my $odJobSpec = decode_json($odJobSpecJSON);

print "\$odJobSpec = \n";
print Dumper($odJobSpec);

# Determine output product short name (first part of filename)
my $prodShortName = $odJobSpec->{'outputs'}{'fileNamePrefix'};

# Determine the platform(s) of the input files -- used as second part of filename
my @allInputFileIds = getAllInputFileIds($odJobSpec);

# This creates a string like "?, ?, ?", with as many ?'s as the number of elements in @allInputFileIds.
my $inputFileIdPlaceholders = getPlaceholderStringForArray( \@allInputFileIds );

$sql=qq{select distinct p.PLATFORMNAME 
         from FILEMETADATA fm, PRODUCTPLATFORM_XREF ppx, PLATFORM p
           WHERE fm.FILEID in (} . $inputFileIdPlaceholders . qq{) and 
            fm.PRODUCTID = ppx.PRODUCTID and 
             ppx.PLATFORMID = p.PLATFORMID};

$sth=$dbh->prepare($sql);
$sth->execute(@allInputFileIds);

my $inputPlatforms = $sth->fetchall_arrayref();
my $platformName;
if ( @$inputPlatforms == 1) {
	$platformName = $inputPlatforms->[0]->[0];
} else {
	print "WARNING: there were " . @$inputPlatforms . " distinct input platforms. Output file's platform name will be blank.\n";
}

my $filePlatformName="";
if ($platformName =~ m/SNPP/) {
	$filePlatformName="npp";
} elsif ($platformName =~ m/JPSS1/) {
	$filePlatformName="j01";
} elsif ($platformName =~ m/JPSS2/) {
	$filePlatformName="j02";
}

my $outfile = $prodShortName . "_" . $filePlatformName . "_s" . $job_coverage_start
               . "_e" . $job_coverage_end
                . "_c" . $year . $mon . $mday . $hour . $min . $sec . "0.nc";

print "Output filename = " . $outfile . "\n";

# Supported Data Types

# ------------------------------------------------------------------------------------------------------------------------------------------------------------+
# NetCDF-3 Classic    | CDL Primitive Data Types NC4	| HDF 5 Native C types                  | IDPS			   				| DSS
# byte NC_BYTE 8      | byte NC_BYTE 8		 	    	| H5T_NATIVE_SCHAR signed char          | signed 8-bit character   		| schar
# 		      		  | ubyte NC_UBYTE 8	            | H5T_NATIVE_UCHAR unsigned char        | unsigned 8-bit character 	   	| uchar
# char NC_CHAR 8      | char NC_CHAR 8 			    	| H5T_C_S1  char                        | signed 8-bit character   		| char (bin2cdf4 only)
# short NC_SHORT 16   | short NC_SHORT 16		    	| H5T_NATIVE_SHORT short                | 16-bit integer 		   		| short
# 		      		  | ushort NC_USHORT 16	            | H5T_NATIVE_USHORT unsigned short      | unsigned 16-bit integer 	    | ushort
# int   NC_INT   32   | int (long) NC_INT 32 		   	| H5T_NATIVE_INT int                    | 32-bit integer 		   		| int
# 		      		  | unint NC_UINT 32 	            | H5T_NATIVE_UINT unsigned int          | unsigned 32-bit integer 	    | uint
# 		      		  | int64 NC_INT64 64	            | H5T_NATIVE_LLONG long long            | 64-bit integer 		  	    | longlong
# 		      		  | uint64 NC_UINT64 64 	        | H5T_NATIVE_ULLONG unsigned long long  | unsigned 64-bit integer       | ulonglong
# float NC_FLOAT 32   | float (real) NC_FLOAT 32    	| H5T_NATIVE_FLOAT float                | 32-bit floating point number  | float
# double NC_DOUBLE 64 | double NC_DOUBLE 64		    	| H5T_NATIVE_DOUBLE double              | 64-bit floating point number  | double
# 		      		  | string NC_STRING stringlength+1 | H5T_C_S1 N/A		                    | string  			   			| 
# ------------------------------------------------------------------------------------------------------------------------------------------------------------+

my %dataTypeH = ();
$dataTypeH{'signed 8-bit character'}{'hdf'}='schar';
$dataTypeH{'signed 8-bit char'}{'hdf'}='schar';
$dataTypeH{'unsigned 8-bit character'}{'hdf'}='uchar';
$dataTypeH{'unsigned 8-bit char'}{'hdf'}='uchar';
$dataTypeH{'unsigned 16-bit integer'}{'hdf'}='ushort';
$dataTypeH{'16-bit integer'}{'hdf'}='short';
$dataTypeH{'unsigned 32-bit integer'}{'hdf'}='uint';
$dataTypeH{'32-bit integer'}{'hdf'}='int';
$dataTypeH{'32-bit floating point'}{'hdf'}='float';
$dataTypeH{'64-bit floating point'}{'hdf'}='double';
$dataTypeH{'64-bit integer'}{'hdf'}='longlong';
$dataTypeH{'unsigned 64-bit integer'}{'hdf'}='ulonglong';
$dataTypeH{'signed 8-bit character'}{'netCDF-4'}='schar';
$dataTypeH{'signed 8-bit char'}{'netCDF-4'}='schar';
$dataTypeH{'unsigned 8-bit character'}{'netCDF-4'}='uchar';
$dataTypeH{'unsigned 8-bit char'}{'netCDF-4'}='uchar';
$dataTypeH{'unsigned 16-bit integer'}{'netCDF-4'}='ushort';
$dataTypeH{'16-bit integer'}{'netCDF-4'}='short';
$dataTypeH{'unsigned 32-bit integer'}{'netCDF-4'}='uint';
$dataTypeH{'32-bit integer'}{'netCDF-4'}='int';
$dataTypeH{'32-bit floating point'}{'netCDF-4'}='float';
$dataTypeH{'64-bit floating point'}{'netCDF-4'}='double';
$dataTypeH{'64-bit integer'}{'netCDF-4'}='longlong';
$dataTypeH{'unsigned 64-bit integer'}{'netCDF-4'}='ulonglong';
$dataTypeH{'byte'}{'netCDF-4'}='schar';
$dataTypeH{'ubyte'}{'netCDF-4'}='uchar';
$dataTypeH{'char'}{'netCDF-4'}='char';
$dataTypeH{'ushort'}{'netCDF-4'}='ushort';
$dataTypeH{'short'}{'netCDF-4'}='short';
$dataTypeH{'uint'}{'netCDF-4'}='uint';
$dataTypeH{'int'}{'netCDF-4'}='int';
$dataTypeH{'long'}{'netCDF-4'}='int';
$dataTypeH{'float'}{'netCDF-4'}='float';
$dataTypeH{'real'}{'netCDF-4'}='float';
$dataTypeH{'double'}{'netCDF-4'}='double';
$dataTypeH{'int64'}{'netCDF-4'}='longlong';
$dataTypeH{'uint64'}{'netCDF-4'}='ulonglong';

#
# Get Tailoring XML
#
$tailoringJSONToXMLScript = "/opt/apps/nde/" . $Mode . "/pgs/dss/tailoring_json_to_xml.py";
$tailoringJSONString = quotemeta(encode_json($odJobSpec->{'prDataSelectionJSON'}));

print encode_json($odJobSpec) . "\n";

my $tailoringXML=`$tailoringJSONToXMLScript $tailoringJSONString`;
doErrorCheck($?, $tailoringJSONToXMLScript);

my $ds = XMLin($tailoringXML, forcearray => [ 'dimension', 'arg', 'measure' ]);

#print "Data Selection XML: *********************************************\n";
#print Dumper($ds);

# Input Data Structure
# <input>
#   <dataPartition time="begingingDateTime" spatial="GPOLY(...)">
#	<dims><dim name= size=/></dims>
#	<dc name= dimList='dim1,...,dimn'>
#		<measure name= fileFormat= fileName= arrayName=/>
#	</dc>
#  </dataPartition>
# </input>


my %input = initInputStruct($odJobSpec, $dbh);


#print "DYLAN****\n\n";
#print Dumper(%input);

#
# Dec 1, 2009:
#
#	Also for each product, need to extract scale and offset and place
#	in the result->partition's->Measure's->attributes
#


my %result= initResultStruct($odJobSpec, $dbh);

print "Result: *************************************************\n";
print Dumper(%result);
print "Done Result\n\n";
#print "Input: *************************************************\n";
#print Dumper(sort keys %input);
#print "Before\n";

#$ds = verifyDataSelection($ds, {%input});

#print "Data Selection XML, after validation:\n";
#print Dumper($ds);
#print "Done Data Selection XML.\n";

#
# Process a DSS XML Command
#
$t0=Benchmark->new;

my $partitionId=0;
for my $partition (sort keys %input) {
    print "\n***$partition***\n";
    
    #
    # Initialize the result structures data partition
    #
	%{$result{'partition'}{$partitionId}}=%{$input{$partition}};
    #
    # Set the partitions dimensions size according to any subSample Specification else set to default which is the actual size of dimension (accomidates dynamic arrays)
    #
    #$t00=Benchmark->new;
	for my $dimension (keys %{$result{'partition'}{$partitionId}{'dim'}}) {
		# print "dim=$dimension\n";
		my $start=0;
		my $stride=1;
		my $count=0;
		my $dimSize =  $result{'partition'}{$partitionId}{'dim'}{$dimension}{'size'};
		my $dimMaxSize = $result{'partition'}{$partitionId}{'dim'}{$dimension}{'maxSize'};
		my $dimActualSize = $result{'partition'}{$partitionId}{'dim'}{$dimension}{'actualDimSize'};
		my $setSize = 0;
		if ($dimActualSize le 0) {$setSize = $dimSize;} else {$setSize = $dimActualSize;}
		
		if (exists $ds->{'select'} && exists $ds->{'select'}{'where'} && exists $ds->{'select'}{'where'}{'subSample'} && exists $ds->{'select'}{'where'}{'subSample'}{'dimension'} && exists $ds->{'select'}{'where'}{'subSample'}{'dimension'}{$dimension}) {
			#print "DEBUG: Inside subSample <<<++++\n";
			$start=$ds->{'select'}{'where'}{'subSample'}{'dimension'}{$dimension}{'start'};
			$stride=$ds->{'select'}{'where'}{'subSample'}{'dimension'}{$dimension}{'stride'};
			$result{'partition'}{$partitionId}{'dim'}{$dimension}{'start'}=$start;
			$result{'partition'}{$partitionId}{'dim'}{$dimension}{'stride'}=$stride;
			if ($ds->{'select'}{'where'}{'subSample'}{'dimension'}{$dimension}{'count'} eq '*') {
				$count = int (($setSize-$start-1) / $stride +1); #dcp - updated so we count properly
				}
			else { 
				$count = $ds->{'select'}{'where'}{'subSample'}{'dimension'}{$dimension}{'count'};
				}
			$result{'partition'}{$partitionId}{'dim'}{$dimension}{'count'}=$count;
			}
		else {
			
			$result{'partition'}{$partitionId}{'dim'}{$dimension}{'start'}=0;
			$result{'partition'}{$partitionId}{'dim'}{$dimension}{'stride'}=1;
			$result{'partition'}{$partitionId}{'dim'}{$dimension}{'count'}=$setSize;

			}
		}

	#$t10=Benchmark->new;
	#$td=timediff($t10,$t00);
	#print "\tFirst loop takes: ",timestr($td),"\n";          
        #
        # 04/27/2010 adding scale and offset processing 
        # determine whether to apply scale and add to result struct
        #
        #$t01=Benchmark->new;
        for my $cubeName (keys %{$result{'partition'}{$partitionId}{'Cube'}}) {

          for my $measureName (keys %{$result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}}) {

            if (exists $ds->{'select'} && exists $ds->{'select'}{'measure'} && exists $ds->{'select'}{'measure'}{$measureName} && exists $ds->{'select'}{'measure'}{$measureName}{'applyScale'}) {
              # update results to include whether to apply the scale (applyScale='1'), and both scale and offset
              if (($ds->{'select'}{'measure'}{$measureName}{'applyScale'}) eq '1') {
                $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'applyScale'} = 1; 
              }else{
                $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'applyScale'} = 0;
              }
            }else{
              $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'applyScale'} = 0;
            }
          
            #
            # 4-28-10 tjf for each partition add scale factor array name and path to result struct for h5
            #
            my $productId = $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'productId'};
            my $fn = $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'file'}; 
       
            my $sql = qq{
            select distinct H_GROUPNAME,H_ARRAYATTRIBUTESTRINGVALUE
            from   HDF5_ARRAYATTRIBUTE haa,
                   HDF5_ARRAY ha,
                   HDF5_GROUP hg,
                   ENTERPRISEMEASURE em,
                   MEASURE_H_ARRAY_XREF mhax
            where  haa.H_ARRAYID=ha.h_ARRAYID and
                   ha.H_GROUPID = hg.H_GROUPID and
                   hg.PRODUCTID = $productId and
                   em.MEASUREID = mhax.MEASUREID and
                   mhax.H_ARRAYID = ha.H_ARRAYID and
                   MEASURENAME = '$measureName' and
                   H_ARRAYATTRIBUTENAME in ('scale_factor', 'add_offset')
            };
	    print "sql=$sql\n";
            my $sth = $dbh->prepare( $sql );
            $sth->execute;   
            $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'scaled'} = 0;
 
            while ( my ( $gn, $array ) = $sth->fetchrow_array()) {
              my $arrayName = "$gn" . "$array";
              my $scale = get_Scale('h5',$fn,$arrayName,0);
              my $offset = get_Offset('h5',$fn,$arrayName,0);
              print "***DYLAN scale: -->$scale<-- offset: -->$offset<--\n";
              $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'scaled'} = 1;
              $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'scale'} = $scale;
              $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'offset'} = $offset; 
            }
            
            #
            # 20160502 dcp now do it for NC4 files
            #
            $sql = qq{
            select distinct N_GROUPNAME,N_ARRAYATTRIBUTENAME, N_ARRAYATTRIBUTESTRINGVALUE
            from   NC4_ARRAYATTRIBUTE naa,
                   NC4_ARRAY na,
                   NC4_GROUP ng,
                   ENTERPRISEMEASURE em,
                   MEASURE_N_ARRAY_XREF mnax
            where  naa.N_ARRAYID=na.N_ARRAYID and
                   na.N_GROUPID = ng.N_GROUPID and
                   ng.PRODUCTID = $productId and
                   em.MEASUREID = mnax.MEASUREID and
                   mnax.N_ARRAYID = na.N_ARRAYID and
                   MEASURENAME = '$measureName' and
                   N_ARRAYATTRIBUTENAME in ('scale_factor', 'add_offset','_FillValue')
            };
            #print "\n***DYLAN START SQL\n$sql\n";
            $sth = $dbh->prepare( $sql );
            $sth->execute;   
            $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'scaled'} = 0;
            my $fillValue;
            my $scale;
            my $offset;
            while ( my ( $gn, $aAttName, $aAttValue ) = $sth->fetchrow_array()) {
              if ($aAttName =~ m/scale/) {
              	$scale = $aAttValue;
              	$result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'scale'} = $scale;
              	print "\tScale factor is: $scale\n";
              } elsif ($aAttName =~ m/offset/) {
              	$offset = $aAttValue;
              	$result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'offset'} = $offset;
              	print "\tAdd offset is: $offset\n";
              } else {
              	$fillValue = $aAttValue;
              	print "\tFill value is prior to scale is: $fillValue\n";
              }
            }

            # Don't scale the fill value unless the scale factor and/or offset is defined. (ENTR-4210)
            if (defined ($scale) or defined($offset)) {
         	  $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'scaled'} = 1;

              if (defined($fillValue)) {
                if (not defined ($scale) and defined ($offset)) {
                  $fillValue = $fillValue + $offset;
                } elsif (defined ($scale) and not defined ($offset)) {
                  $fillValue = $fillValue * $scale;
                } else {
                  $fillValue = ($fillValue * $scale) + $offset;
              	}
                print "\tFill value after scale is: $fillValue\n";
              }

            } elsif (defined($fillValue)) {
               print "\tFill value was not scaled.\n";
            }
            if ( defined ($fillValue) ) {
              $result{'partition'}{$partitionId}{'Cube'}{$cubeName}{'Measure'}{$measureName}{'fillValue'} = $fillValue;
            }

          }
        }
#$t11=Benchmark->new;
#$td=timediff($t11,$t01);
#print "\tSecond loop took: ",timestr($td),"\n";


# call h5dump to get the scale offset
# for each partiton, $partitionID, cube, $dimlist, measure, $measurename 
#    ADD: SCALE
#         OFFSET 
          


	#
	# Process DSS XML Filters
	#

	my %uniqueDataCubeFilterList=();
	my %uniqueMeasureFilterList=();

	if (exists $ds->{'select'} && exists $ds->{'select'}{'where'} && exists $ds->{'select'}{'where'}{'filter'}) {
		#print "DYLAN ************* Do Filter *******************\n";
		#print Dumper(%result);

		print "*****Entering filtering\n";

		#
		# Get the unique list of data cubes of all filters and initialize mask for each
		# Get the unique list of measures 
		#

		# Go through the list of measures in each filter and look up their data cube
		for my $filter (@{$ds->{'select'}{'where'}{'filter'}{'arg'}}) {
			my %filterRef = %$filter;
			my $filterMeasure=$filterRef{'measureName'};
			my $filterFrom=$filterRef{'from'};
			print "\tFiltering measure: $filterMeasure\n";
			
			# Calculate number of elements for use below
			my $dimension = $result{'partition'}{$partitionId}{'Cube'}{$filterFrom}{'List'}[0];
			my $maskSize  = $result{'partition'}{$partitionId}{'dim'}{$dimension}{'count'};
			for (my $i=1; $i < $result{'partition'}{$partitionId}{'Cube'}{$filterFrom}{'Rank'}; $i++) {
				# $maskSize =  $result{'partition'}{$partitionId}{$filterFrom}{'List'}[1];
				$dimension = $result{'partition'}{$partitionId}{'Cube'}{$filterFrom}{'List'}[$i];
				$maskSize *= $result{'partition'}{$partitionId}{'dim'}{$dimension}{'count'};
				}

			$uniqueDataCubeFilterList{$filterFrom}=$maskSize;
			$uniqueMeasureFilterList{$filterFrom}{$filterMeasure}=$maskSize;
			}
	
		# For each datacube identified above, create mask

		for my $dataCube (keys %uniqueDataCubeFilterList) {
			my $maskFileName="$partitionId.mask.$dataCube.bin";
			my $maskSize=$uniqueDataCubeFilterList{$dataCube};

			# Initialize the Data Cube's mask.
	
			my $command="/opt/apps/nde/" . $Mode . "/pgs/dss/bin/initMask $maskFileName $maskSize";
			print "=====================>command=$command\n";
			my $rc=system("$command");
			doErrorCheck($rc,"initMask");
			
			#print "initMask rc=$rc\n";
			
			# Store the file name containing the mask in %result.
			
			$result{'partition'}{$partitionId}{'Cube'}{$dataCube}{'mask'}=$maskFileName;
			}


		#
		# Process the filters
		#


		# For the unique measure used by all filters, convert measure to binary
		for my $dataCube (keys %uniqueMeasureFilterList) {
			#print "DataCube=$dataCube\n";
			for my $measure (keys %{$uniqueMeasureFilterList{$dataCube}}) {
				#print "\tMeasure=$measure\n";
				#print "partition=$result{'partition'}{$partitionId}\n";
				do_Measure2Bin($Mode, $result{'partition'}{$partitionId}, $dataCube, $measure);
				}
			}
	
		#
		# Go through the list of filters process left to right assuming that filters are ANDed to each other
		# End result will be a mask for referenced data cube(s) that has 1 - true; 0 - false
		#     for each array element.
		# If present, the mask will be used to replace array values with fill values if false.
		#

		print "************* Applying Filter *******************\n";
		#print Dumper(%result);

		for my $filter (@{$ds->{'select'}{'where'}{'filter'}{'arg'}}) {
			my %filterRef = %$filter;
			
			my $filterMeasure=$filterRef{'measureName'};
			my $filterFrom=$filterRef{'from'};
			my $filteroperator=$filterRef{'operator'};
			my $filteroperand=$filterRef{'operands'};
            
            #print "****DYLAN**** Filtering: $filterMeasure\n \tfrom: $filterFrom for: $filteroperator using: $filteroperand\n";
            
			# filterBin.c parameters

			my $maskFileName = $result{'partition'}{$partitionId}{'Cube'}{$filterFrom}{'mask'};
			my $maskSize  = $uniqueMeasureFilterList{$filterFrom}{$filterMeasure};
			my $arrayFileName = $result{'partition'}{$partitionId}{'Cube'}{$filterFrom}{'Measure'}{$filterMeasure}{'file'};
			my $arrayDataType = $result{'partition'}{$partitionId}{'Cube'}{$filterFrom}{'Measure'}{$filterMeasure}{'dT'};
			my $fileType = $result{'partition'}{$partitionId}{'Cube'}{$filterFrom}{'Measure'}{$filterMeasure}{'productFileFormat'};
			if ($fileType eq "netCDF") {$dataType = $dataTypeH{$arrayDataType}{'netCDF-4'}} else {$dataType = $dataTypeH{$arrayDataType}{'hdf'}};
			if ($bitMask == 1) {$dataType = 'short';}		
			# filterBin command line

			my $filterCommand="/opt/apps/nde/" . $Mode . "/pgs/dss/bin/filterBin";
			$filterCommand .= " $maskFileName $arrayFileName $maskSize $dataType $filteroperator $filteroperand";
			print "=====================>command=$filterCommand\n";
			
			my $rc=system("$filterCommand");
			doErrorCheck($rc,"filterbin");
			
			#print "binfilter rc=$rc\n";
			}

		#print "**************Done Filter ******************\n";
		#print "Result=";
		#print Dumper(%result);
	
		}


	#
	# Process the selected measures (to be output)
	#
#$t03=Benchmark->new;
	for my $selectMeasure (keys %{$ds->{'select'}{'measure'}}) {

		my $dataCube=$ds->{'select'}{'measure'}{$selectMeasure}{'from'};

		# If the selectMeasure is in the filters, don't convert, already done above

		if (exists $uniqueMeasureFilterList{$dataCube}{$selectMeasure}) {;}
		else {
			#$t04=Benchmark->new;
			do_Measure2Bin($Mode, $result{'partition'}{$partitionId}, $dataCube, $selectMeasure);
			#$t14=Benchmark->new;
			#$td=timediff($t14,$t04);
			#print "\tdo_Measure2Bin code took: ",timestr($td),"\n";
			}
	
		}
#$t13=Benchmark->new;
#$td=timediff($t13,$t03);
#print "\tThird loop took: ",timestr($td),"\n";
	

	#
	# Done processing a data partition
	#  Update the partition's dimensions' size
	#  Update the result's dimensions' size 
	#
#$t04=Benchmark->new;
	for my $dimension (keys %{$result{'dim'}}) {
		#print "dim=$dimension\n";
		$result{'partition'}{$partitionId}{'dim'}{$dimension}{'size'} = $result{'partition'}{$partitionId}{'dim'}{$dimension}{'count'};
		if ($result{'dim'}{$dimension}{'DP'} == 1) {
			$result{'dim'}{$dimension}{'size'} += $result{'partition'}{$partitionId}{'dim'}{$dimension}{'count'};
			}
		else {
			$result{'dim'}{$dimension}{'size'} = $result{'partition'}{$partitionId}{'dim'}{$dimension}{'count'};
			}
		}
#$t14=Benchmark->new;
#$td=timediff($t14,$t04);
#print "\tFourth loop took: ",timestr($td),"\n";			

	$partitionId++;

	}

$t1=Benchmark->new;
$td=timediff($t1,$t0);
print "\tWriting binaries took:",timestr($td),"\n";
	
#print "DYLAN Result: *************************************************\n";
#print Dumper(\%result);


#
# Use the 'dim' and 'Cube' hashes in the result hash to initialized the output file
#

#print "create nc4 needs to ignore input partition and use result to create variables
#it is picking up filter that are not used in select list\n";
print "THE OUT FILE: $outfile\n\n";
# 4-20-10 tjf add $ds

#$t05=Benchmark->new;
create_nc4_cf ($ds, $jobId, $dbh, $outfile, \%result, $odJobSpec);
#$t15=Benchmark->new;
#$td=timediff($t15,$t05);
print "\tcreate_nc4_cf code took: ",timestr($td),"\n";

#
# Write the binary files to the created output file.
#

$t06=Benchmark->new;

	# Process each measure
for my $selectMeasure (keys %{$ds->{'select'}{'measure'}}) {
    print "==============Processing Measures================= \n\n";
    my $fillValue = "";
    my $outputDimName ="";
    if (exists $ds->{'select'} && exists $ds->{'select'}{'measure'} && exists $ds->{'select'}{'measure'}{$selectMeasure} && exists $ds->{'select'}{'measure'}{$selectMeasure}{'fillValue'}) {
        $fillValue = $ds->{'select'}{'measure'}{$selectMeasure}{'fillValue'};
    }
    my $outputArrayName = $ds->{'select'}{'measure'}{$selectMeasure}{'outputArrayName'};
    $outputDimName = $ds->{'select'}{'measure'}{$selectMeasure}{'outputDimName'};
	if (!$outputArrayName) {$outputArrayName = $selectMeasure;}
        print "\tSelect measure: $selectMeasure\n";
        print "\tOutput array name: $outputArrayName\n";
        print "\tOutput dimension name: $outputDimName\n", if (defined($outputDimName));
        print "\tFill value: $fillValue\n\n";
	my $dataCube=$ds->{'select'}{'measure'}{$selectMeasure}{'from'};

	#
	# For each partition for this measure calculate the start for this partition
	#
#$t07=Benchmark->new;
	my @start=();
	my $switch=1;
	for my $partitionId (sort {$a<=>$b} keys %{$result{'partition'}}) {
		#print "\nPartition ID: $partitionId\n";
		# 1st time thru set start for each dimension to 0

		if ($switch) {
			$switch=0;
			for (my $i=0; $i < $result{'partition'}{$partitionId}{'Cube'}{$dataCube}{'Rank'}; $i++) {
				$start[$i]=0;
				}
			}

		# Copy the binary file partitition into the output file's array
        #$t04=Benchmark->new;
		do_Bin2Out($outputArrayName, $fillValue, $Mode, $result{'partition'}{$partitionId}, $dataCube, $selectMeasure, \@start);
		#$t14=Benchmark->new;
		#$td=timediff($t14,$t04);
		#print "\tdo_Bin2Out code took: ",timestr($td),"\n";
		#print "partition=$partitionId\n";

		# For subsequent passed through this loop, increment start for dimensions that are being aggregated

		for (my $i=0; $i < $result{'partition'}{$partitionId}{'Cube'}{$dataCube}{'Rank'}; $i++) {
			my $dimensionName=$result{'partition'}{$partitionId}{'Cube'}{$dataCube}{'List'}[$i];
			if ($result{'partition'}{$partitionId}{'dim'}{$dimensionName}{'DP'} == 1) {
				$start[$i] += $result{'partition'}{$partitionId}{'dim'}{$dimensionName}{'size'};
				}
			
			}
		}
#$t17=Benchmark->new;
#$td=timediff($t17,$t07);
#print "\tFirst (and only) Loop took: ",timestr($td),"\n";
	}
$t16=Benchmark->new;
$td=timediff($t16,$t06);
print "\tWriting output file code took: ",timestr($td),"\n";

#
# Write the output file name into the PSF for the NDE data handling system
#

#print "***DYLAN***RESULT***\n\n";
#print Dumper(\%result);

open( PSF, ">${PROGRAM}.PSF" ) or die "Can't open PSF file: ${PROGRAM}.PSF\n";
print PSF "$outfile\n";
print PSF "\#END-of-PSF";
close( PSF );

#If there is a script for additional formatting defined in the PCF, execute it. 
if ($formatter ne "" && $formatter !~ m/cdf/) {
	$ENV{'PYTHONPATH'}="/opt/apps/nde/" . $Mode . "/pgs/dss/formatter"; #for python formatters
	`echo "Executing additional formatting: $formatter" >> $log_file`;
	system("/opt/apps/nde/" . $Mode . "/pgs/dss/formatter/" . $formatter . " " . $current_directory) == 0 or die \
		"Formatter: " . $formatter . " had a non-zero return. Exiting dss.";
}

`echo "dss.pl Execution Complete." >> $log_file`;

exit(0);

#
# Connect to the Database
#
sub connectNDEDB { 

# Section of code below is commented out: database URL/password is hard-coded instead for now.

#my $Mode = $_[0];
#my $runscrpt = "/opt/apps/nde/" . $Mode . "/common/rundbscript.sh";
##DO interpolate $$ but NOT $NDE_... and put it in '' so it isn't later:
#$runscrpt = $runscrpt . " \'" . 'echo $NDE_DB_KEY > ' . "/tmp/dssdbtmp_$$" . "\'";
#my $runawayCheck = 0;
#while (! -r "/tmp/dssdbtmp_$$") {
#  system(". ~/.set_NDE_ENV_vars $Mode;$runscrpt");
#  my $rc = `sleep 1`;
#  $runawayCheck++;
#  if ($runawayCheck > 5) {
#  	die "Database Connection Error: Cannot determine login information for mode: $Mode.";
#  }
#}
#chomp( $dbp = `cat /tmp/dssdbtmp_$$` );
#system("rm -f /tmp/dssdbtmp_$$");
my $dbh = DBI->connect("dbi:Pg:dbname=nde_dev1;host=nde-pgsql.cixifosqwtgg.us-east-1.rds.amazonaws.com", lc($Mode), 'nde')
        or die "\nDatabase Connection Error: $DBI::errstr\n";
$dbh->{LongReadLen}=640000;
return $dbh;

}

sub initInputStruct {

#
# Subroutine: initInputStruct
# Input: On demand Job Spec
#	 Database Handle
# Output: The input structure hash in the format
#	<input>
#		<$fileStartTime>
#			<'dim'>
#				<$e_dimName 
#					DP='0|1'   (Data Partition (DP) if = 1 concatenate array on this dimension)
#					size='n'  (storageSize in enterprise tables)
#					actualSize='n' (partition's actual dimension size from h5dump/ncdump)
#					maxSize='n'    (storageMaxSize in enterprise tables)
#				/>
#			</'dim'>
#			<'Cube'>
#				<$e_dimListName>
#					<'Rank'> = '...' (number of dimensions) />
#					<'List'>
#						'$e_dimName1'
#						'$e_dimName2'
#						'...'
#					/>
#					<'measure'>
#						<$e_measureName
#							array='...' (Array name inside source file)
#							group='...' (Group name that holds array)
#							dT='...' (DataType of the array)
#							file='...'  (FileName array is stored in)
#							productID='...' (product ID associated with the array)
#							productFileFormat='netCDF|hdf5-1.6'
#							[
#							 tranform='bit' (if this array is a bit array then measure name represents particular bit, e.g., quality or mask arrays)
#							 bitOffset='...' (offset in the bit array for this specific data)
#							 bitLength='...' (number of bits this value occupies)
#							]
#						/>
#					</measure>
#				</$e_dimListName>
#			</Cube>
#		</$fileStartTime>
#	<input>
#
#
							
my ($odJobSpec, $dbh) = @_;

my %input=();



#
# Get Dimensions and Data Partitions
#

#
# Dec 1, 2009 Need to added whether the dimension is dynamic
# Dec 9, 2009 Need to add the from attribute into the where clauses of below SQL
#


@allMeasureNames = ();
@allMeasureDimensionLists = ();

foreach my $measure ( @{$odJobSpec->{'prDataSelectionJSON'}{'measures'}} ) {
	push @allMeasureNames, $measure->{'name'};
	push @allMeasureDimensionLists, $measure->{'from'};
}

if (defined( $odJobSpec->{'prDataSelectionJSON'}{'where'}{'filter'} )) {
	foreach my $filterMeasure ( @{$odJobSpec->{'prDataSelectionJSON'}{'where'}{'filter'}{'args'}} ) {
		push @allMeasureNames, $filterMeasure->{'measureName'};
		push @allMeasureDimensionLists, $filterMeasure->{'from'};
	}
}

@allInputFileIds = getAllInputFileIds($odJobSpec);

# SQL Comments
# select partition and dimensions 
# from ( distinct list of measures in the Data Selection XML) + other tables
#


my $sql = qq { 
select distinct
        TO_CHAR(f.fileStartTime,'YYYY-MM-DD HH24:MI:SS'),
        d.e_dimensionName,
        e_dimensionStorageSize,
        e_dimensionStorageMaxSize,
        e_dimensionDataPartition
from
        enterpriseMeasure m,
        EnterpriseDimensionList l,
        FileMetadata f,
        EnterpriseOrderedDimension o,
        EnterpriseDimension d
where
        m.measureName in (} . getPlaceholderStringForArray( \@allMeasureNames ) . qq{) and
        m.e_dimensionListId = l.e_dimensionListId and
	l.e_dimensionListName in (} . getPlaceholderStringForArray ( \@allMeasureDimensionLists ) . qq{) and
        f.fileId in (} . getPlaceholderStringForArray ( \@allInputFileIds ) . qq{) and
        l.e_dimensionListId = o.e_dimensionListId and
        o.e_dimensionId = d.e_dimensionId
order by
        TO_CHAR(f.fileStartTime,'YYYY-MM-DD HH24:MI:SS')
};

#print "sql=$sql\n";
my $sth = $dbh->prepare($sql);

$sth->execute(@allMeasureNames, @allMeasureDimensionLists, @allInputFileIds);

while (my @data = $sth->fetchrow_array()) { ;
	my $fileStartTime=$data[0];
	my $e_dimensionName=$data[1];
	my $e_dimensionStorageSize=$data[2];
	my $e_dimensionStorageMaxSize=$data[3];
	my $e_dimensionDataPartition=$data[4];
	
	$input{$fileStartTime}{'dim'}{$e_dimensionName}{'size'}=$e_dimensionStorageSize;
	$input{$fileStartTime}{'dim'}{$e_dimensionName}{'maxSize'}=$e_dimensionStorageMaxSize;
	$input{$fileStartTime}{'dim'}{$e_dimensionName}{'DP'}=$e_dimensionDataPartition;
}

#
# Get Data Cube / Dimensions List Info
#


$sql= qq{ 
select distinct
        TO_CHAR(f.fileStartTime,'YYYY-MM-DD HH24:MI:SS'),
        l.e_dimensionListName,
        o.e_dimensionOrder,
        e_dimensionName
from
        enterpriseMeasure m,
        EnterpriseDimensionList l,
        FileMetadata f,
        EnterpriseOrderedDimension o,
        EnterpriseDimension d
where
        m.measureName in (} . getPlaceholderStringForArray( \@allMeasureNames ) . qq{) and
        m.e_dimensionListId = l.e_dimensionListId and
	f.fileId in (} . getPlaceholderStringForArray( \@allInputFileIds ) . qq{) and
        l.e_dimensionListId = o.e_dimensionListId and
	l.e_DimensionListName in (} . getPlaceholderStringForArray( \@allMeasureDimensionLists ) . qq{) and
        o.e_dimensionId = d.e_dimensionId
order by
        TO_CHAR(f.fileStartTime,'YYYY-MM-DD HH24:MI:SS'),
        l.e_dimensionListName,
        e_DimensionOrder
};

#print "sql=$sql\n";

$sth = $dbh->prepare($sql);
$sth->execute(@allMeasureNames, @allInputFileIds, @allMeasureDimensionLists);

while (my @data = $sth->fetchrow_array()) { ;
	my $fileStartTime=$data[0];
	my $e_dimensionListName=$data[1];
	my $e_dimensionOrder=$data[2];
	my $e_dimensionName=$data[3];
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Rank'}=$e_dimensionOrder;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'List'}[($e_dimensionOrder-1)]=$e_dimensionName;
	}


#
# Get ArrayInfo -> DataType, File, Group, etc.
#

# For HDF 5 Input

$sql= qq{ 
select
	TO_CHAR(f.fileStartTime,'YYYY-MM-DD HH24:MI:SS'),
        f.fileName,
        m.measureName,
        e_dimensionName,
        a.h_dataType,
        g.h_groupName,
        a.h_arrayName,
	l.e_dimensionListName,
	f.productId,
	p.productFileFormat,
	x.h_transformType,
	x.h_bitOffset,
	x.h_bitLength
from
        enterpriseMeasure m,
        EnterpriseDimensionList l,
        measure_h_array_xref x ,
        hdf5_array a,
        hdf5_Group g,
        FileMetadata f,
        EnterpriseOrderedDimension o,
        EnterpriseDimension d,
        ProductDescription p
where
        m.measureName in (} . getPlaceholderStringForArray( \@allMeasureNames ) . qq{) and
        m.e_dimensionListId = l.e_dimensionListId and
        m.measureId = x.measureId and
        x.h_arrayId = a.h_arrayId and
        f.fileId in (} . getPlaceholderStringForArray( \@allInputFileIds ) . qq{) and
        f.productId = g.productId and
        a.h_groupId = g.h_groupId and
        l.e_dimensionListId = o.e_dimensionListId and
        l.e_dimensionListName in (} . getPlaceholderStringForArray( \@allMeasureDimensionLists ) . qq{) and
        o.e_dimensionId = d.e_dimensionId and
	f.productId = p.productId
order by
        TO_CHAR(f.fileStartTime,'YYYY-MM-DD HH24:MI:SS'),
        measureName,
        e_DimensionOrder
};

#print "sql=$sql\n";
$sth = $dbh->prepare($sql);

$sth->execute(@allMeasureNames, @allInputFileIds, @allMeasureDimensionLists);

while (my @data = $sth->fetchrow_array()) { ;
	my $fileStartTime=$data[0];
	my $fileName=$data[1];
	my $measureName=$data[2];
	my $e_dimensionName=$data[3];
	my $h_dataType=$data[4];
	my $h_groupName=$data[5];
	my $h_arrayName=$data[6];
	my $e_dimensionListName=$data[7];
	my $productId=$data[8];
	my $productFileFormat=$data[9];
	my $transformType=$data[10];
	my $bitOffset=$data[11];
	my $bitLength=$data[12];
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'dT'}=$h_dataType;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'group'}=$h_groupName;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'file'}=$fileName;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'array'}=$h_arrayName;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'productId'}=$productId;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'productFileFormat'}=$productFileFormat;
	if ($transformType eq "bit") {
		$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'transform'}=$transformType;
		$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'bitOffset'}=$bitOffset;
		$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'bitLength'}=$bitLength;
		}
	#
	# Get the actual dimension size for this array
	#
	my $command = "h5dump -H -d $h_groupName" . "$h_arrayName $fileName | grep DATASPACE";
	my $rc = `$command`;
	chomp($rc);
	$rc =~ m/\( ([\d+, ]+) \)?/;
	my @dims = split(',',$1);
	my $i=0;
#	print "***DYLAN***\n\t$command\n";
	for my $dims (@dims) {
		$dims =~ s/ //g;
		my $dimN = $input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'List'}[$i];
		$input{$fileStartTime}{'dim'}{$dimN}{'actualDimSize'} = $dims;	
		$i++;
	}
}

# For netCDF4 Input

$sql= qq{ 
select
	TO_CHAR(f.fileStartTime,'YYYY-MM-DD HH24:MI:SS'),
        f.fileName,
        m.measureName,
        e_dimensionName,
        a.n_dataType,
        g.n_groupName,
        a.n_arrayName,
	l.e_dimensionListName,
	f.productId,
	p.productFileFormat,
	x.n_transformType,
	x.n_bitOffset,
	x.n_bitLength
from
        enterpriseMeasure m,
        EnterpriseDimensionList l,
        measure_n_array_xref x,
        nc4_array a,
        nc4_Group g,
        FileMetadata f,
        EnterpriseOrderedDimension o,
        EnterpriseDimension d,
        ProductDescription p
where
        m.measureName in (} . getPlaceholderStringForArray ( \@allMeasureNames ) . qq{) and
        m.e_dimensionListId = l.e_dimensionListId and
        m.measureId = x.measureId and
        x.n_arrayId = a.n_arrayId and
        f.fileId in (} . getPlaceholderStringForArray ( \@allInputFileIds ) . qq{) and
        f.productId = g.productId and
        a.n_groupId = g.n_groupId and
        l.e_dimensionListId = o.e_dimensionListId and
        l.e_dimensionListName in (} . getPlaceholderStringForArray ( \@allMeasureDimensionLists ) . qq{) and
        o.e_dimensionId = d.e_dimensionId and
	f.productId = p.productId
order by
        TO_CHAR(f.fileStartTime,'YYYY-MM-DD HH24:MI:SS'),
        measureName,
        e_DimensionOrder
};

#print "sql=$sql\n";
$sth = $dbh->prepare($sql);

$sth->execute(@allMeasureNames, @allInputFileIds, @allMeasureDimensionLists);

while (my @data = $sth->fetchrow_array()) { ;
	my $fileStartTime=$data[0];
	my $fileName=$data[1];
	my $measureName=$data[2];
	my $e_dimensionName=$data[3];
	my $h_dataType=$data[4];
	my $h_groupName=$data[5];
	my $h_arrayName=$data[6];
	my $e_dimensionListName=$data[7];
	my $productId=$data[8];
	my $productFileFormat=$data[9];
	my $transformType=$data[10];
	my $bitOffset=$data[11];
	my $bitLength=$data[12];
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'dT'}=$h_dataType;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'group'}=$h_groupName;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'file'}=$fileName;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'array'}=$h_arrayName;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'productId'}=$productId;
	$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'productFileFormat'}=$productFileFormat;
	if ($transformType eq "bit") {
		$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'transform'}=$transformType;
		$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'bitOffset'}=$bitOffset;
		$input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}{'bitLength'}=$bitLength;
		}
	#
	# Get the actual dimension size for this file
	#
	my $command = "ncdump -h $fileName | grep $h_arrayName\\(";
	my $rc = `$command`;
	chomp($rc);
	$rc =~ m/\((.*)\)/;
	my @dims = split(',',$1);
	my $i = 0;
	for my $dims (@dims) {
		$dims =~ s/ //g;
		my $command = "ncdump -h $fileName | grep \"$dims =\"";
		my $rc =`$command`;
		$rc =~ s/\t//g;
		$rc =~ s/ //g;
		$rc =~ m/=(\d+)/;
		my $dimensionSize = $1;
		my $dimN = $input{$fileStartTime}{'Cube'}{$e_dimensionListName}{'List'}[$i];
		$input{$fileStartTime}{'dim'}{$dimN}{'actualDimSize'} = $dimensionSize;	
		$i++;
	}
}	

print "INIT*****************************\n";
print Dumper(\%input);


#for my $partition (keys %input) {
#	for my $measure (keys %{$input{$partition}{'Measure'}}) {
#		print "measure=$measure\n";
#		}
#	}

#$sql = qq{
#select
#	fileStartTime,
#	sdo_util.to_wktgeometry(f.fileSpatialArea)
#from
#        productionJob pj,
#        jobSpecInput jsi,
#        filemetadata f
#where
#        prJobId = $jobId and
#        pj.prodPartialJobId = jsi.prodPartialJobId and
#        f.fileid = jsi.fileid
#order by
#        f.filestarttime
#        };

#my $sth = $dbh->prepare($sql);

#$sth->execute;

#while (my @data = $sth->fetchrow_array()) {
#        print "$data[0], $data[1]\n";
#        }

#print "Done initInput\n";
return %input;


}

sub initResultStruct {

my ($odJobSpec, $dbh) = @_;

my %result=();

#
# Subroutine: initResultStruct
# Input: On demand job spec
#	 Database Handle
# Output: result Structure Hash
# <result>
#		<'dim'>
#			<$e_dimName>
#				size='n' (n=0, if DP=1 and will grow as partitions are aggregated OR n=storagSize, if DP=0)
#				maxSize='n' (storageMaxSize from the enterprise tables)
#				DP='0|1' (Data Partition (DP) if = 1 concatenate array on this dimension)
#			/>
#		<'dim'/>
#		<'Cube'>
#			<$e_dimListName>
#				Rank = '...' (number of dimensions)
#				<'List'>
#					'$e_dimName1'
#					'$e_dimName2'
#					'...'
#				/>
#				<'measure'>
#					<$e_measureName>
#						dataType='..'
#					/>
#				<'measure'/>
#		<'Cube'/>
	#	<'partition'>  NOTE--------> This portion of the hash is added immediately after this subroutine executes so it's included here. More information is added as dss executes.
	#		<$partitionID> (equivalent to the file start time - 0 to the number of granules in aggregation)
	#			<'dim'>
	#				<$e_dimName 
	#					DP='0|1'   (Data Partition (DP) if = 1 concatenate array on this dimension)
	#					size='n'  (storageSize in enterprise tables)
	#					actualSize='n' (partition's actual dimension size from h5dump/ncdump)
	#					maxSize='n'    (storageMaxSize in enterprise tables)
	#					count='n'  (n, if n is specified in production rule OR actualSize of array is default)
	#				/>
	#			</'dim'>
	#			<'Cube'>
	#				<$e_dimListName>
	#					Rank = '...' (number of dimensions)
	#					<'List'>
	#						'$e_dimName1'
	#						'$e_dimName2'
	#						'...'
	#					/>
	#					<'measure'>
	#						<$e_measureName
	#							array='...' (Array name inside source file)
	#							group='...' (Group name that holds array)
	#							dT='...' (DataType of the array)
	#							file='...'  (becomes the name of the binary file that the source file array is dumped to)
	#							originalFile='...' (file name of the source file for the array)
	#							productID='...' (product ID associated with the array)
	#							productFileFormat='netCDF|hdf5-1.6'
	#							start='n' (index to start dumping the array)
	#							stride='n' (increment through the array)
	#							count='n' (index on how far to go through array)
	#							applyScale = '0|1' (1 means apply the scale and offset to the array)
	#							scaled = '0|1' (same as above)
	#							scale='n' (scale value retrieved from the file)
	#							offset='n' (offset value retrieved from the file)
	#							[
	#							 tranform='bit' (if this array is a bit array then measure name represents particular bit, e.g., quality or mask arrays)
	#							 bitOffset='...' (offset in the bit array for this specific data)
	#							 bitLength='...' (number of bits this value occupies)
	#							]
	#						/>
	#					[
	#					 <'mask'> = maskFileName />
	#					]
	#					</measure>
	#				</$e_dimListName>
	#			</Cube>
	#		<$partitionID/>
	#	<'partition'/>
# <result/>

@allInputFileIds = getAllInputFileIds($odJobSpec);

@selectedMeasureNames = ();
@selectedMeasureDimensionLists = ();

foreach my $measure ( @{$odJobSpec->{'prDataSelectionJSON'}{'measures'}} ) {
	push @selectedMeasureNames, $measure->{'name'};
	push @selectedMeasureDimensionLists, $measure->{'from'};
}

#
# Get Dimensions of the measures in the select list
#


my $sql = qq { select distinct
        ed.e_dimensionName,
        e_dimensionStorageSize,
        e_dimensionStorageMaxSize,
        e_dimensionDataPartition
from
        enterpriseMeasure em,
        EnterpriseDimensionList edl,
        FileMetadata fm,
        EnterpriseOrderedDimension eod,
        EnterpriseDimension ed
where
        em.measureName in (} . getPlaceholderStringForArray( \@selectedMeasureNames ) . qq{) and
        edl.e_dimensionListName in (} . getPlaceholderStringForArray( \@selectedMeasureDimensionLists ) . qq{) and
        em.e_dimensionListId = edl.e_dimensionListId and
        fm.fileId in (} . getPlaceholderStringForArray( \@allInputFileIds ) . qq{) and
        edl.e_dimensionListId = eod.e_dimensionListId and
        eod.e_dimensionId = ed.e_dimensionId
order by
        ed.e_dimensionName
	
};

print "sql=$sql\n";
my $sth = $dbh->prepare($sql);

$sth->execute(@selectedMeasureNames, @selectedMeasureDimensionLists, @allInputFileIds);

while (my @data = $sth->fetchrow_array()) { ;

	my $e_dimensionName=$data[0];
	my $e_dimensionStorageSize=$data[1];
	my $e_dimensionStorageMaxSize=$data[2];
	my $e_dimensionDataPartition=$data[3];
	
	if ($e_dimensionDataPartition == 1) { $result{'dim'}{$e_dimensionName}{'size'}=0; }
	else { $result{'dim'}{$e_dimensionName}{'size'}=$e_dimensionStorageSize; } 

	$result{'dim'}{$e_dimensionName}{'DP'}=$e_dimensionDataPartition;
	$result{'dim'}{$e_dimensionName}{'maxSize'}=$e_dimensionStorageMaxSize;
	
	for my $selectMeasure (keys %{$ds->{'select'}{'measure'}}) {
		my $outputDimName;
		$outputDimName = $ds->{'select'}{'measure'}{$selectMeasure}{'outputDimName'};
		if (defined $outputDimName) {
			my @dimName = split(/ /, $outputDimName);
			for ($i=0; $i<=$#dimName; $i++ ) {
				if ($e_dimensionName =~ /$dimName[$i]/) {
					$result{'dim'}{$e_dimensionName}{'outputDimName'}=$dimName[$i];
				}				
			}
		}
	}
}

#
# Get Data Cube / Dimensions List Info
#
#
#$sql= qq{ select distinct
#	em.MeasureName,
#	edl.e_dimensionListName,
#	eod.e_dimensionOrder,
#       e_dimensionName,
#	em.MeasureDataType
#from
#        productionJob pj,
#        productionJobSpec pjs,
#        JobSpecInput jsi,
#        productionrule pr,
#        XMLTable('/dss/select/measure' passing pr.prdataselectionxml
#                columns
#                        measureName varchar(255) PATH '\@name',
#                        datacube varchar(255) path '\@from'
#                ) xt,
#        enterpriseMeasure em,
#        EnterpriseDimensionList edl,
#        FileMetadata fm,
#        EnterpriseOrderedDimension eod,
#        EnterpriseDimension ed
#where
#        pj.prJobId=$jobId and
#        pj.prodPartialJobId = pjs.prodPartialJobId and
#        pjs.prodPartialJobId = jsi.prodPartialJobId and
#        pjs.prid=pr.prid and
#        em.measureName = xt.measureName and
#        edl.e_dimensionListName = xt.dataCube and
#        em.e_dimensionListId = edl.e_dimensionListId and
#        jsi.fileId = fm.fileId and
#        edl.e_dimensionListId = eod.e_dimensionListId and
#        eod.e_dimensionId = ed.e_dimensionId
#order by
#	edl.e_dimensionListName,
#        e_DimensionOrder
#};

#print "sql=$sql\n";
#$sth = $dbh->prepare($sql);
#$sth->execute;

#while (my @data = $sth->fetchrow_array()) { 
#	my $measureName=$data[0];
#	my $e_dimensionListName=$data[1];
#	my $e_dimensionOrder=$data[2];
#	my $e_dimensionName=$data[3];
#	my $measureDataType=$data[4];
#	$result{'Cube'}{$e_dimensionListName}{'Rank'}=$e_dimensionOrder;
#	$result{'Cube'}{$e_dimensionListName}{'List'}[($e_dimensionOrder-1)]=$e_dimensionName;
#	$result{'Cube'}{$e_dimensionListName}{'Measure'}{$measureName}=$measureDataType;
#	}
#print "Result *******************************\n";
#print Dumper(\%result);

return %result;

}

# This subroutine was been commented out on 10/13/2017. It is a good starting
# point, but not a complete solution.
#
# It was written with the intention of allowing the VIIRS geoTIFFs to run even
# if one of the input images was missing.
#
# However, the intermediate net-CDF4 that is produced is not usuable in all
# cases. In an aggregation of multiple products, it deletes whole fields from
# the resulting netCDF, rather than making them all fill values, which is 
# undesirable. 
#
# It also doesn't take into consideration the possibility of filtering on a 
# measure that has not been selected.

#sub verifyDataSelection {
#	my ($ds, $input) = @_;
#	# This subroutine added by Jonathan Hansford, 9/29/2017, for ENTR-4162.
#	# Checks to make sure that all of the measures that are being selected are actually present in the input.
#	# If any are not present, a warning is logged, the measure is deleted from the ds structure, and it continues as normal.
#
#	if (exists $ds->{'select'} and exists $ds->{'select'}{'measure'}) {
#		for my $measureName (keys %{$ds->{'select'}{'measure'}}) {
#			if (exists $ds->{'select'}{'measure'}{$measureName}{'from'}) {
#				my $dimensionGroup = $ds->{'select'}{'measure'}{$measureName}{'from'};
#
#				my $measureFoundInInput = 0;
#				for my $time (keys %{$input}) {
#					if (exists $input->{$time}{'Cube'} and exists $input->{$time}{'Cube'}{$dimensionGroup}) {
#						if (exists $input->{$time}{'Cube'}{$dimensionGroup}{'Measure'} and exists $input->{$time}{'Cube'}{$dimensionGroup}{'Measure'}{$measureName}) {
#							$measureFoundInInput = 1;
#						}
#					}
#				}
#
#				if (not $measureFoundInInput) {
#					print "WARNING: The selected measure '$measureName' was not found in any input files. The measure is being ignored.\n";
#					delete $ds->{'select'}{'measure'}{$measureName};
#				}
#			} else {
#				# $ds->{'select'}{'measure'}{$measureName}{'from'} did not exist.
#				print "ERROR: DSS selection XML invalid: selected measure $measureName was not associated with any dimensions (missing 'from' XML attribute).\n";
#			}
#		}
#	}
#	return $ds;
#}

sub do_H52bin {

#my ($result, $partition, $dataCube, $measure, $input, $partitionId, $ds) = @_;
my ($Mode, $fileName, $groupName, $arrayName, $dataType, $outputFileName, $start, $stride, $count) = @_;


#print "$partition, $dataCube, $measure\n";			
#
# Extract the Arrays from source files to Binary
#
#
# Do HDF5 to Binary
# Arguments:                                                    *
#   All of the following MUST be specified IN THIS ORDER!!     *
#     - hdf file name       ex: filename.h5                     *
#     - array path          ex: /All_Data/VIIRS-MOD-GEO-TC_ALL/ *
#     - array name          ex: Latitude                        *
#     - data type           ex: float                           *
#     - binary output name  ex: fubar.bin                       *
#     - start               ex: 0,0  (if 3D: 0,0,0)             *
#     - stride              ex: 1,1  (0,0 doesn't make sense)   *
#     - count               ex: 1000,7000                       *
#


my $command="/opt/apps/nde/" . $Mode . "/pgs/dss/bin/hdf2bin";
$command .= " $fileName";
$command .= " $groupName";
$command .= " $arrayName";
$command .= " $dataType";
$command .= " $outputFileName";
$command .= " " . join(",", @{$start});
$command .= " " . join(",", @{$stride});
$command .= " " . join(",", @{$count});


# Execute the h52bin executable
print "=====================>command=$command\n";
my $rc=system("$command");	

doErrorCheck($rc,"h52bin");

# Update the result input partition with result of this function
#$result{'partition'}{$partitionId}{'Cube'}{$dataCube}{'Measure'}{$measure}{'file'}=$outputFileName;
#$result{'partition'}{$partitionId}{'Cube'}{$dataCube}{'Measure'}{$measure}{'array'}="";
#$result{'partition'}{$partitionId}{'Cube'}{$dataCube}{'Measure'}{$measure}{'group'}="";

}

sub do_NC2bin {

#my ($result, $partition, $dataCube, $measure, $input, $partitionId, $ds) = @_;
my ($Mode, $fileName, $groupName, $arrayName, $dataType, $outputFileName, $start, $stride, $count) = @_;


#print "$partition, $dataCube, $measure\n";			
#
# Extract the Arrays from source files to Binary
#
#
# Arguments:                                                    *
#   All of the following MUST be specified IN THIS ORDER!!     *
#     - hdf file name       ex: filename.h5                     *
#     - array path          ex: /All_Data/VIIRS-MOD-GEO-TC_ALL/ *
#     - array name          ex: Latitude                        *
#     - data type           ex: float                           *
#     - binary output name  ex: fubar.bin                       *
#     - start               ex: 0,0  (if 3D: 0,0,0)             *
#     - stride              ex: 1,1  (0,0 doesn't make sense)   *
#     - count               ex: 1000,7000                       *
#


my $command="/opt/apps/nde/" . $Mode . "/pgs/dss/bin/netCDF2bin";
$command .= " $fileName";
$command .= " $groupName";
$command .= " $arrayName";
$command .= " $dataType";
$command .= " $outputFileName";
$command .= " " . join(",", @{$start});
$command .= " " . join(",", @{$stride});
$command .= " " . join(",", @{$count});


# Execute the netCDF2bin executable
print "=====================>command=$command\n";
$tnc2bin=Benchmark->new;
my $rc=system("$command");
$tnc2binend=Benchmark->new;
$td=timediff($tnc2binend,$tnc2bin);
print "\tnetCDF2bin code takes: ",timestr($td),"\n";
	
doErrorCheck($rc,"netCDF2bin");

# Update the result input partition with result of this function
#$result{'partition'}{$partitionId}{'Cube'}{$dataCube}{'Measure'}{$measure}{'file'}=$outputFileName;
#$result{'partition'}{$partitionId}{'Cube'}{$dataCube}{'Measure'}{$measure}{'array'}="";
#$result{'partition'}{$partitionId}{'Cube'}{$dataCube}{'Measure'}{$measure}{'group'}="";

}


sub do_Measure2Bin {

#Input
#	result{'paritition'}{$partitionId} hash
#	dataCube Name
#	Measure Name
#
#Output
#	binary File Name
#

my ($Mode, $partition, $dataCube, $measure) = @_;

my %partitionRef=%$partition;

print "Do M2B: ****$partitionRef{'dim'}{'Scan__48'}{'size'}, $dataCube, $measure***\n";

# First, process the measure's dimensions' subSample specifications if present then use else
#             if not present then start, stride, count  are set to defaults using dimension size

my $rank = $partitionRef{'Cube'}{$dataCube}{'Rank'};
my $dimList = join(", ",@{$partitionRef{'Cube'}{$dataCube}{'List'}});
my @start='';
my @stride='';
my @count='';
for (my $i=0; $i<$rank; $i++) {
	my $dimName=$partitionRef{'Cube'}{$dataCube}{'List'}[$i];
	# print "dimName=$dimName\n ";
	$start[$i] =$partitionRef{'dim'}{$dimName}{'start'};
	$stride[$i]=$partitionRef{'dim'}{$dimName}{'stride'};
	$count[$i] =$partitionRef{'dim'}{$dimName}{'count'};
	}

#print "rank=$rank, dimList=$dimList, start=$start[0], stride=$stride[0], count=$count[0]\n";
#print "DYLAN::: \n";
#print Dumper(\%partitionRef);

my $array=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure};
#print "array=$array->{'array'}\n";

my $binFileName="";

my @fileparts = split('\.',$array->{'file'});
my $fileType = $fileparts[$#fileparts];

# Data Type Parameter

my $inputDataType = $array->{'dT'};

#print "DataType=$inputDataType\n";
#print "DYLAN*** file type is $fileType\n";

if ($fileType eq "h5") {$dataType = $dataTypeH{$inputDataType}{'hdf'}};
if ($fileType eq "nc") {$dataType = $dataTypeH{$inputDataType}{'netCDF-4'}};

#if (!defined $dataType) {
#	if ($inputDataType eq 'ubyte') {$inputDataType = 'uchar';}
#	$dataType = $inputDataType;
#} #Added if data type is already NC4 (tailored product)
#print "DYLAN*** DataType=$dataType\n";
our $bitMask = 0;
if (exists $partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'transform'} ) {
	if ($partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'transform'} eq "bit" ) {
		$bitMask = 1;
		my $bitOffset=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'bitOffset'};
		my $bitLength=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'bitLength'};
		my $tempFileName="temp.bin";
		do_H52bin($Mode, $array->{'file'}, $array->{'group'}, $array->{'array'}, $dataType, $tempFileName, \@start, \@stride, \@count), if ( $fileType eq "h5" );
		do_NC2bin($Mode, $array->{'file'}, $array->{'group'}, $array->{'array'}, $dataType, $tempFileName, \@start, \@stride, \@count), if ( $fileType eq "nc" );
		$binFileName="$partitionId.$measure.bin";
	        my $size=1;
	        for (my $i=0; $i < $partitionRef{'Cube'}{$dataCube}{'Rank'}; $i++) {
			my $dimensionName=$partitionRef{'Cube'}{$dataCube}{List}[$i];
			$size *= $partitionRef{'dim'}{$dimensionName}{'count'};
			print "i=$i, size=$size, dim=$partitionRef{'dim'}{$dimensionName}{'count'}\n";
                }
		my $command="/opt/apps/nde/" . $Mode . "/pgs/dss/bin/getBits ";
		$command.="$tempFileName $binFileName $dataType $size $bitOffset $bitLength";
		print "============================>command=$command\n";
		
		my $rc=system("$command");
		doErrorCheck($rc,"getBits");
		}
	}
else {
	# Output binary filename
	$binFileName = "$partitionId.$measure.bin";

	# Extract the Array to a binary file

	do_H52bin($Mode, $array->{'file'}, $array->{'group'}, $array->{'array'}, $dataType, $binFileName, \@start, \@stride, \@count), if ( $fileType eq "h5" );
	do_NC2bin($Mode, $array->{'file'}, $array->{'group'}, $array->{'array'}, $dataType, $binFileName, \@start, \@stride, \@count), if ( $fileType eq "nc" );
	}

$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'originalFile'} = $partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'file'};
$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'file'}=$binFileName;
#$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'array'}="";
#$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'group'}="";
}


sub do_Bin2Out {

#Input
#	result{'paritition'}{$partitionId} hash
#	dataCube Name
#	Measure Name
#	start array
#
#Output
#	binary File Name
#

my ($outputArrayName, $fillValue, $Mode, $partition, $dataCube, $measure, $start) = @_;

my %partitionRef=%$partition;

#print "parition: *************************************************\n";

#print Dumper(%partitionRef);
#print "parition: *************************************************\n";

#print "Result: *************************************************\n";
#print Dumper(%result);
#print "Result: *************************************************\n";



# 4-30-10 TJF
# add apply scale.  changes data type of anyting to 64-bit floating point, applys scale and offset
# updates this in $partitionRef

  if ($partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'scaled'} == 1) {
    if ($partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'applyScale'} == 1) {
      
      my $dataType=$dataTypeH{$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'dT'}}{'netCDF-4'};
#      if (!defined $dataType) {$dataType=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'dT'};}
      
      my $binaryFileName=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'file'};
      my $scaledFileName="scaled." . $binaryFileName; 
      my $scale=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'scale'};
      my $offset=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'offset'};

      my $size=1;
      for (my $i=0; $i < $partitionRef{'Cube'}{$dataCube}{'Rank'}; $i++) {
        my $dimensionName=$partitionRef{'Cube'}{$dataCube}{List}[$i];
        $size *= $partitionRef{'dim'}{$dimensionName}{'count'};
        print "i=$i, size=$size, dim=$partitionRef{'dim'}{$dimensionName}{'count'}\n";
      }
 
      my $command = "/opt/apps/nde/" . $Mode . "/pgs/dss/bin/scalebin ";
      $command .= "$binaryFileName $dataType $scaledFileName $size $scale $offset";
      
      print "=================>command=$command\n";
      my $rc=system("$command");
      doErrorCheck($rc,"scalebin");

      $partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'dT'} = '64-bit floating point';
      $partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'file'} = $scaledFileName;

    }
  }

 





  #$partitionRef{'Cube'}{$cubeName}{'Measure'}{$measureName}{'dT'} = '64-bit floating point';



# IF a mask exists for the measure's data cube
#    AND a fill value was specified in the measure's select XML
# THEN 
#    filter the binary file prior to copying it to the output file
#

if (exists $partitionRef{'Cube'}{$dataCube}{'mask'}) {
  my $maskFileName=$partitionRef{'Cube'}{$dataCube}{'mask'};
  my $binaryFileName=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'file'};
  my $size=1;
  for (my $i=0; $i < $partitionRef{'Cube'}{$dataCube}{'Rank'}; $i++) {
    my $dimensionName=$partitionRef{'Cube'}{$dataCube}{List}[$i];
    $size *= $partitionRef{'dim'}{$dimensionName}{'count'};
    print "i=$i, size=$size, dim=$partitionRef{'dim'}{$dimensionName}{'count'}\n";
  }

  my $dataType=$dataTypeH{$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'dT'}}{'netCDF-4'};
  $binaryFileName=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'file'};
  
  if ($fillValue ne "") {
    my $command = "/opt/apps/nde/" . $Mode . "/pgs/dss/bin/applyFilter ";
    $command .= "$maskFileName $binaryFileName $size $dataType $fillValue";
    print "====================>command=$command\n";
    
    my $rc=system("$command");
    doErrorCheck($rc,"applyFilter");
    }
}


# Parameters needed by bin2cdf4
#	binary file name
#	netCDF file name
#	output variable name
#	data type
#	start			ex: 0,0 (if 3D: 0,0,0)
#	size			ex: 100, 200

my $binaryName=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'file'};
my $netCDFName=$outfile;
#print "Need to update $netCDFName in output to reflect select list spec\n";
my $varName="$outputArrayName";
#my $dataType=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'dT'};
my $dataType=$dataTypeH{$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'dT'}}{'netCDF-4'};
#if (!defined $dataType) {
#	$dataType=$partitionRef{'Cube'}{$dataCube}{'Measure'}{$measure}{'dT'};
#	if ($dataType eq 'ubyte') {$dataType = 'uchar';}
#}


my $startParm=join(",", @{$start});

# do size parameter

my $dimensionName=$partitionRef{'Cube'}{$dataCube}{List}[0];
my $sizeParm=$partitionRef{'dim'}{$dimensionName}{'count'};
for (my $i=1; $i < $partitionRef{'Cube'}{$dataCube}{'Rank'}; $i++) {
	my $dimensionName=$partitionRef{'Cube'}{$dataCube}{List}[$i];
	$sizeParm .= ",$partitionRef{'dim'}{$dimensionName}{'count'}";
	}

# call bin2cdf4

my $command="/opt/apps/nde/" . $Mode . "/pgs/dss/bin/bin2cdf4";
$command .= " $binaryName $netCDFName $varName $dataType $startParm $sizeParm";

print "===================>command=$command\n";
#$tbin2cdf4=Benchmark->new;
my $rc = system("$command");
#$tbin2cdf4end=Benchmark->new;
#$td=timediff($tbin2cdf4end,$tbin2cdf4);
#print"\tbin2cdf4 code took: ",timestr($td),"\n";
doErrorCheck($rc,"bin2cdf4");


}

sub doErrorCheck {
	#my $rc, executable name
	my ($errorcode,$routine) = @_;
	my $rcshift = $errorcode >> 8;

	if ($errorcode ne 0 || $rcshift ne 0) {
		print "$routine rc=$errorcode\n";
		print "system rc=$rcshift\n";
		exit(1);
	}
}

