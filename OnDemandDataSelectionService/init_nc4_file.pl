#!/usr/bin/env perl

#
# name: init_nc4_file.pl
# revised: 20091127 lhf, initialize netCDF4 file
#          20091130 lhf, add CF array attributes for long_name, units, standard_name for just lat and lon
#                        using Partition 0
#          20091209 lhf, using missing_valuename/missing_value vectors, valid_min/max
#          20100305 pgm, added netCDF4 input attributes
#          20110610 lhf, CR281 update to use netcdf-4.1.1 (temporary), comply to the update to ncgen binary,
#                        create real netCDF4 files using the -k 3 option, not the strict nc3 classic model format
#		   20120221 dcp, Added identifying attributes to netCDF file: job coverage start/end, date created (all products) 
#						 orbit start/end, day/night flag, ascending/descending flag, g-ring (granule rules only - no aggregation), metadata_link (file name)
#          20120522 dcp, Added identifying attributes to netCDF file for temporal and orbital aggregations
#		   20120523 dcp, Added tests for whether dimension names match actual dimension size - if not, updates dimension name
#		   20120605 dcp, Added changes to allow for NC4 data types (enable tailoring of NC4 files)
#		   20120724 dcp, Added GranuleExact PR rule type
#		   20120822 dcp, Added capability to ncdump scale and offset from NC4 tailored products (not NUPs)
#		   20121024 dcp, Added capability to internally compress arrays using netcdf zlib library and HDF5 Shuffle algorithm (improvement over gzip)
#							Cleaned up handling of attributes written to CDL file
#	       20121205 dcp, added capability to specify output array names using xml attribute in production rule (outputArrayName=value)
#		   20130225 awc, added capability to specify compression chunk size names using xml attribute in production rule (chunkSize=value) where "value" is in "X,Y,..." chunk dimension format	
#		   20130328 awc, modified get_cf_array_attr() function to account for quality flag attributes.
#		   20130429 dcp, updated ncgen call to use environment path rather than hard coded path
#		   20131029 dcp, updated all POLYGON to MULTIPOLYGON (CR-1479 changes the filespatialarea to allow multipolygons)
#		   20140514 dcp, added capability to specify dimension name in production rule for writing to NC4 file (user request)
#		   20140924 dcp, included option to explicitely define data type for metadata attributes in CDL
#		   20150922 dcp, updated the regex for retrieving the offset and scale from IDPS files - now grabs the "-" sign [ENTR-2200]
#		   20180928 jrh, updated to be DSS on-demand
#
# What does it do?
# ----------------
# units, long_name, missing_valuename/missing_value loaded with _FillValueName and _FillValue values,
# valid_min/max loaded with RangeMin/Max values
# 
#
# What doesn't it do?
# -------------------
# no standard_name - it's a lookup, future enhancement
# no scale_factor - database has the name of the array that contains the scale factors
#

no warnings 'uninitialized';
use Math::Trig;
use Math::Trig ':great_circle';

sub create_nc4_cf {


  my ( $ds, $jobid, $dbh, $filename, $result, $odJobSpec ) = @_;
  my $debug = 0;
  # solution?
##lhf 20110624 changed byte to ubyte for unsigned 8-bit character
##dcp 20120809 added unsigned 8-bit char 
  my %translateDataType = (
  	'unsigned 16-bit integer' => 'ushort',
  	'16-bit integer' => 'short',
	'32-bit floating point' => 'float',
	'32-bit integer' => 'int',
	'unsigned 32-bit integer' => 'uint',
	'64-bit floating point' => 'double',
	'64-bit integer' => 'int64',
	'unsigned 64-bit integer' => 'uint64',
	'signed 8-bit character' => 'byte',
	'signed 8-bit char' => 'byte',
	'unsigned 8-bit character' => 'ubyte',
	'unsigned 8-bit char' => 'ubyte',
	'uchar' => 'ubyte',
	'schar' => 'byte',
	'longlong' => 'int64',
	'ulonglong' => 'uint64'
        );
  my $returnCode = 1;

  my ( $outfilenameroot ) = split /\./, $filename;
  open ( CDLFILE, ">$outfilenameroot\.cdl" ) or die "Can't open $outfilenameroot\.cdl\n";

  print CDLFILE "\nnetcdf $outfilenameroot\{\n";
  print CDLFILE "\tdimensions:\n";
  my @dimensions;
  my $numdims = 0;
  
  #print "DYLAN**********result hash:\n";
  #print Dumper(%$result);
  
  while ( my ( $key, $value ) = each( %$result ) ) {
    if ( $key eq 'dim' ) {
      while ( my ($newkey, $newvalue ) = each ( %$value ) ) {
        my ( $null, $size ) = split /\_\_/, $newkey;
        $size = $result->{ $key }{ $newkey }{ 'size' };
        my $dimName;
        $dimName = $result->{ $key }{ $newkey }{ 'outputDimName' };
        #Check to see if dimension name matches dimension size
        my $nameSize=$newkey;
        $nameSize =~ s/^.*-//;
        if ($nameSize ne $size) {
        	$newkey =~ s/-.*$/-/;
        	$newkey = $newkey . $size;        	
        } #if not, replace with proper size\
        #Check if production rule defines the dimension name (override the default)
        if (defined $dimName) {
        	$newkey = $dimName;
        }
        my $duplicate = grep { /$newkey/ } @dimensions; #Check to see if dimension already exists (may happen during tailoring)
        if ($duplicate eq 0) {
        	push ( @dimensions, "$newkey = $size" );
        	$numdims++;
        }
      }
    }
  }

  print CDLFILE "\t      ";
  for ( $i=0; $i<=$#dimensions; $i++ ) {
    print CDLFILE "$dimensions[$i]";
    print CDLFILE ", ", if ( $i < $#dimensions );
  }
  print CDLFILE ";\n";

  print CDLFILE "\tvariables:\n";

##dcp - adding identifying attributes to file

#Add file name to metadata link attribute
print CDLFILE "\t      :Metadata_Link =\"$filename\";\n";
#Generate a UUID
my $uuid = `uuidgen`;
print CDLFILE "\t      :uuid =\"$uuid\";";

#Get job start and end times from PCF file
	my $commandLine = $0;
	chomp($commandLine);
	my @list = split('/',$commandLine);
	my $Name = $list[-1];
	my $PROGRAM = $Name;
	my $job_coverage_start="";
	my $job_coverage_end="";
	my $formatter;
	
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
	    $nc4CompressionFlag = $Value, if ($Keyword_And_Argument=~m/nc4_compression_flag/i);
	    $compressionLevel = $Value, if ($Keyword_And_Argument=~m/compression_level/i);
	    $formatter = $Value, if ($Keyword_And_Argument=~m/formatter/i);
	  }
	}
	close(PCF);
	
	#Get current time for date/time of file creation	
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=gmtime;
	$year += 1900;
	
	$mon = $mon+1;
	if ($mon < 10){ $mon = "0$mon"; }
	if ($mday < 10){ $mday = "0$mday" }
	if ($hour < 10){ $hour = "0$hour"; }
	if ($min < 10){ $min = "0$min"; }
	if ($sec < 10){ $sec = "0$sec"; }
	
#Write date_created to CDL file in proper CF format
	print CDLFILE "\t      :date_created = \"$year-$mon-$mday"."T$hour".":$min".":$sec"."Z\";\n";

	my @allInputFileIds = getAllInputFileIds( $odJobSpec);

	# Write time_coverage_start, time_coverage_end, start_orbit_number, and end_orbit_number to the CDL file, in the proper format.
	my $time_coverage_sql = qq{select TO_CHAR(min(FILESTARTTIME), 'YYYY-MM-DD"T"HH24:MI:SS'), TO_CHAR(max(FILEENDTIME), 'YYYY-MM-DD"T"HH24:MI:SS'),
                                    min(FILEBEGINORBITNUM), max(FILEENDORBITNUM)
	                             from FILEMETADATA WHERE FILEID in (} . getPlaceholderStringForArray( \@allInputFileIds ) . ")";
	my $time_coverage_handle = $dbh->prepare($time_coverage_sql);
	$time_coverage_handle->execute( @allInputFileIds );

	my ($time_coverage_start, $time_coverage_end, $orbit_number_start, $orbit_number_end) = $time_coverage_handle->fetchrow_array();
	print CDLFILE "\t      :time_coverage_start = \"" . $time_coverage_start . "Z\";\n";
	print CDLFILE "\t      :time_coverage_end = \"" . $time_coverage_end . "Z\";\n";
	print CDLFILE "\t      :start_orbit_number = " . $orbit_number_start . ";\n";
	print CDLFILE "\t      :end_orbit_number = " . $orbit_number_end . ";\n";

	#Write ascending/descending information to CDL file
	my $asc_desc_indicator_sql = qq{select distinct fileascdescindicator from FILEMETADATA where FILEID in (} . getPlaceholderStringForArray( \@allInputFileIds ) . ")";
	my $asc_desc_indicator_handle = $dbh->prepare($asc_desc_indicator_sql);
	$asc_desc_indicator_handle->execute( @allInputFileIds );

	my $asc_desc_indicators = $asc_desc_indicator_handle->fetchall_arrayref();
	my $asc_desc_indicator_to_write;
	foreach my $asc_desc_flag ( @$asc_desc_indicators  ) {
		print "Got asc/desc indicator: " . $asc_desc_flag->[0] . "\n";
		if ($asc_desc_flag->[0] =~ m/0|1|2/) {
			if (defined $asc_desc_indicator_to_write) {
				# If both 0 and 1 are contained in the input files, write 2 (both)
				$asc_desc_indicator_to_write = "2";
			} else {
				$asc_desc_indicator_to_write = $asc_desc_flag->[0];
			}
		}
	}
	print CDLFILE "\t      :ascend_descend_data_flag = " . $asc_desc_indicator_to_write . ";\n";

	#Write day/night flag to CDL file
	my $day_night_flag_sql = qq{select distinct FILEDAYNIGHTFLAG from FILEMETADATA where FILEID in (} . getPlaceholderStringForArray( \@allInputFileIds ) . ")";
	my $day_night_flag_handle = $dbh->prepare($day_night_flag_sql);
	$day_night_flag_handle->execute( @allInputFileIds );

	my $day_night_flags = $day_night_flag_handle->fetchall_arrayref();
	my $day_night_flag_to_write;
	foreach my $day_night_flag ( @$day_night_flags ) {
		print "Got day/night flag: " . $day_night_flag->[0] . "\n";
		my $day_night_flag_capitalized = ucfirst(lc($day_night_flag->[0]));
		if ($day_night_flag_capitalized =~ m/(Day|Night|Both)/) {
			if (defined $day_night_flag_to_wrte) {
				$day_night_flag_to_write = "Both";
			} else {
				$day_night_flag_to_write = $day_night_flag_capitalized;
			}
		}
	}
	print CDLFILE "\t      :day_night_data_flag = \"" . $day_night_flag_to_write . "\";\n";

	#Write number of granules to CDL file
	#Specifically, writes the # of input files for the product with the most input files.
	my $number_of_granules_sql = qq{select PRODUCTID, COUNT(FILEID) from FILEMETADATA
	                                 where FILEID in (} . getPlaceholderStringForArray( \@allInputFileIds ) . qq{)
	                                  group by PRODUCTID};
	my $number_of_granules_handle = $dbh->prepare($number_of_granules_sql);
	$number_of_granules_handle->execute( @allInputFileIds );

	my $input_file_counts = $number_of_granules_handle->fetchall_arrayref();
	my $max_number_of_granules = 0;
	foreach my $input_file_count_row ( @$input_file_counts ) {
		if ($input_file_count_row->[1] > $max_number_of_granules) {
			$max_number_of_granules = $input_file_count_row->[1];
		}
	}
	if ($max_number_of_granules == 0) {
		print "ERROR: Number of input granules could not be obtained.";
		die;
	}

	print CDLFILE "\t      :number_of_granules = " . $max_number_of_granules . ";\n";

	#Write geographic metadata into the CDL file
	#First, select an input product with geographic metadata available for all granules. If there are none, geographic metadata cannot be written, and the program dies.
	my $valid_geo_sql = qq{select PRODUCTID, COUNT(FILEID) from FILEMETADATA
	                        where FILEID in (} . getPlaceholderStringForArray( \@allInputFileIds ) . qq{)
                                 and FILESPATIALAREA is not null
	                          group by PRODUCTID
                                   having COUNT(FILEID) = ?};
	print "\$valid_geo_sql = " . $valid_geo_sql . "\n";
	my $valid_geo_handle = $dbh->prepare($valid_geo_sql);
	$valid_geo_handle->execute( @allInputFileIds, $max_number_of_granules );

	my $valid_geo_result = $valid_geo_handle->fetchall_arrayref();

	if (defined $valid_geo_result) {
		my $product_id_with_geo; 
		# Always choose the minimum product ID that has valid geographic metadata, so that the form of the output product geographic metadata is consistent.
		foreach my $valid_geo_result_row ( @$valid_geo_result ) {
			if (defined($product_id_with_geo)) {
				if ($valid_geo_result_row->[0] < $product_id_with_geo) {
					$product_id_with_geo = $valid_geo_result_row->[0];
				}
			} else {
				$product_id_with_geo = $valid_geo_result_row->[0];
			}
		}
		print "INFO: Will use geographic metadata from productID " . $product_id_with_geo . ".\n";

		my $get_geo_sql = qq{select ST_AsText(FILESPATIALAREA) from FILEMETADATA
                                      where FILEID in (} . getPlaceholderStringForArray( \@allInputFileIds ) . qq{)
		                       and PRODUCTID = ?
		                        order by FILESTARTTIME asc};
		my $get_geo_handle = $dbh->prepare($get_geo_sql);
		$get_geo_handle->execute( @allInputFileIds, $product_id_with_geo );

		my $all_input_geography = $get_geo_handle->fetchall_arrayref();

		my @coordinates = ();
		foreach my $input_geography ( @$all_input_geography ) {
			print "Got geo: " . $input_geography->[0] . "\n";
			my %parsedCoords = parseMultipolygonString($input_geography->[0]);
			push @coordinates, \%parsedCoords;
		}
		
		# IDPS "G-Ring_*" metadata, by definition, may contain four or more coordinates, but for the products received by NDE, there will always be eight.
		# IDPS defines the ordering G-Ring coordinates as:
		# """
		# G-Ring points corresponding to granule boundaries are sequenced in a clockwise direction, starting with the first pixel of the last scan of a granule. 
		# """
		# However, it seems that the first pixel of last scan is not in the same location for each instrument. For VIIRS, the first point is on the right side of the granule (east if ascending); for CrIS, it is on the left (west if ascending). 
		# NUPS use most frequently use 4-sided polygons, although they can use eight, and the ordering of the points does not necessarily conform to the IDPS G-Ring definition. For example, the Active Fires geospatial_bounds POLYGON is counter-clockwise, and begins at the first scanline, leftmost pixel (west if ascending).
		# 
		# Therefore, to generate the correct geographic metadata, we need to determine the direction the satellite moving, based which of the four edges of the first granule matches up with the opposite edge from the second granule, and fill out the geographic metadata based on that.  
		#
		# ASSUMPTIONS:
		#    - The product orders its geographic metadata in a consistent way, based on the order of scans (An example that would not work is an algorithm that generates a POLYGON such that the northwest coordinate is always first, regardless of it the satellite is ascending or descending.)
		#    - The product's geographic area is roughly rectangular and is defined by a 4-sided or 8-sided polygon. (For an idea of what roughly rectangular means, look at a VIIRS granule).
		#    - If the product is using an 8-sided polygon, the first point in the list is a corner point of the rough rectangle, not the midpoint of an edge.
		#

		my @latitudeCoordinateRing;
		my @longitudeCoordinateRing;

		if ($max_number_of_granules == 1) {
			# If there is only one input granule, just copy its geo metadata.

			@latitudeCoordinateRing = @{$coordinates[0]->{'allLatitudes'}};
			@longitudeCoordinateRing = @{$coordinates[0]->{'allLongitudes'}};
		} else {
			# For the first two granules, compare each pair of opposite edges. (Compare granule 0, edge 0 w/ granule 1, edge 2; granule 0, edge 1 w/ granule 1, edge 3; etc.
			# The pair that is closest together indicates that the satellite is moving in the direction of that edge. 

			my $minDistance = distanceLL( $coordinates[0]->{'midpointLatitudes'}->[0], $coordinates[0]->{'midpointLongitudes'}->[0], $coordinates[1]->{'midpointLatitudes'}->[2], $coordinates[1]->{'midpointLongitudes'}->[2] );
			my $satelliteHeadingEdge = 0;

			for (my $i = 1; $i <= 3; $i++) {
				my $distance = distanceLL( $coordinates[0]->{'midpointLatitudes'}->[$i], $coordinates[0]->{'midpointLongitudes'}->[$i], $coordinates[1]->{'midpointLatitudes'}->[($i+2)%4], $coordinates[1]->{'midpointLongitudes'}->[($i+2)%4] );
				if ( $distance < $minDistance ) {
					$minDistance = $distance;
					$satelliteHeadingEdge = $i;
				}
			}

			print "\$satelliteHeadingEdge = " . $satelliteHeadingEdge . "\n";

			# Now, fill out the ring, starting from the edge the satellite moving towards in the last granule (i.e.: the last edge scanned).
			my $last_granule = $max_number_of_granules - 1;
			@latitudeCoordinateRing = ( $coordinates[$last_granule]->{'cornerLatitudes'}->[$satelliteHeadingEdge], $coordinates[$last_granule]->{'midpointLatitudes'}->[$satelliteHeadingEdge], $coordinates[$last_granule]->{'cornerLatitudes'}->[($satelliteHeadingEdge+1)%4] );
			@longitudeCoordinateRing = ( $coordinates[$last_granule]->{'cornerLongitudes'}->[$satelliteHeadingEdge], $coordinates[$last_granule]->{'midpointLongitudes'}->[$satelliteHeadingEdge], $coordinates[$last_granule]->{'cornerLongitudes'}->[($satelliteHeadingEdge+1)%4] );

			# Add the middle point of the next edge (the edge pointing towards the previous granule in the temporal aggregation).
			my $midpoint_granule = int($max_number_of_granules/2);
			if ($max_number_of_granules % 2 == 0) {
				push @latitudeCoordinateRing, $coordinates[$midpoint_granule]->{'cornerLatitudes'}->[($satelliteHeadingEdge+2)%4];
				push @longitudeCoordinateRing, $coordinates[$midpoint_granule]->{'cornerLongitudes'}->[($satelliteHeadingEdge+2)%4];
			} else {
				push @latitudeCoordinateRing, $coordinates[$midpoint_granule]->{'midpointLatitudes'}->[($satelliteHeadingEdge+1)%4];
				push @longitudeCoordinateRing, $coordinates[$midpoint_granule]->{'midpointLongitudes'}->[($satelliteHeadingEdge+1)%4];
			}

			# Now,  add the edge opposite the edge the satellite is moving towards from the first granule
			push @latitudeCoordinateRing, ( $coordinates[0]->{'cornerLatitudes'}->[($satelliteHeadingEdge+2)%4], $coordinates[0]->{'midpointLatitudes'}->[($satelliteHeadingEdge+2)%4], $coordinates[0]->{'cornerLatitudes'}->[($satelliteHeadingEdge+3)%4] );
			push @longitudeCoordinateRing, ( $coordinates[0]->{'cornerLongitudes'}->[($satelliteHeadingEdge+2)%4], $coordinates[0]->{'midpointLongitudes'}->[($satelliteHeadingEdge+2)%4], $coordinates[0]->{'cornerLongitudes'}->[($satelliteHeadingEdge+3)%4] );

			if ($max_number_of_granules % 2 == 0) {
				push @latitudeCoordinateRing, $coordinates[$midpoint_granule]->{'cornerLatitudes'}->[($satelliteHeadingEdge+3)%4];
				push @longitudeCoordinateRing, $coordinates[$midpoint_granule]->{'cornerLongitudes'}->[($satelliteHeadingEdge+3)%4];
			} else {
				push @latitudeCoordinateRing, $coordinates[$midpoint_granule]->{'midpointLatitudes'}->[($satelliteHeadingEdge+3)%4];
				push @longitudeCoordinateRing, $coordinates[$midpoint_granule]->{'midpointLongitudes'}->[($satelliteHeadingEdge+3)%4];
			}
		}
		
		# The last (9th) element is the same as the first
		push @latitudeCoordinateRing, $latitudeCoordinateRing[0];
		push @longitudeCoordinateRing, $longitudeCoordinateRing[0];
		
		print CDLFILE "\t      :g_ring_latitude = " . join(", ", @latitudeCoordinateRing) . ";\n";
		print CDLFILE "\t      :g_ring_longitude = " . join(", ", @longitudeCoordinateRing) . ";\n";
	} else {
		print "ERROR: Could not write geographic metadata into CDLFILE; no input product with valid geographic metadata for all granules.\n";
		die;
	}


#Write statically defined (production rule) global attributes to CDL file
	foreach my $prParameterName (keys %{$odJobSpec->{'parameters'}} ) {
		if ($prParameterName ne "formatter") {
			# Note: writing nothing as the datatype (in regular dss, comes from the algoparameterdatatype in the database - except that a datatype of 'string' is replaced with nothing.)
			print CDLFILE "\t      :" . $prParameterName . " = \"" . $odJobSpec->{'parameters'}{$prParameterName} . "\";\n";
		}
	}

  my @dataArray;
  for my $type ( sort keys %$result ) { # $type in [ 'dim', 'Cube', 'partition' ]
    ##print "##debug  type: $type\n", if ( $debug );
    if ( $type eq 'partition' ) {
      for my $partitionnumber ( sort keys %{$result->{ $type }} ) {
        # need to only do once 
        if ( $partitionnumber eq '0' ) {
          for my $subtype ( sort keys %{$result->{ $type }->{ $partitionnumber}} ) { # $subtype in [ 'dim', 'Cube' ]
            # need to only process for 'Cube' (not 'dim')
            if ( $subtype eq 'Cube' ) {
              for my $dimensionlistname ( sort keys %{$result->{ $type }->{ $partitionnumber }->{ $subtype }} ) {
                #print "##debug    dimensionlistname: $dimensionlistname\n", if ( $debug );
                for my $listattr ( sort keys %{$result->{ $type }->{ $partitionnumber }->{ $subtype }->{ $dimensionlistname }} ) {
                  #print "##debug  listattr: $listattr\n", if ( $debug );
                  if ( $listattr eq 'Measure' ) {
                    for my $item ( sort keys %{$result->{ $type }->{ $partitionnumber }->{ $subtype }->{ $dimensionlistname }->{ $listattr }} ) {
                      #print "##debug        item: $item\n", if ( $debug );
					  my $outputArrayName = '';
					  my $outputDimName;
					  if(exists $ds->{'select'} && exists $ds->{'select'}{'measure'} && exists $ds->{'select'}{'measure'}{$item} && exists $ds->{'select'}{'measure'}{$item}{'outputArrayName'}){
						$outputArrayName = $ds->{'select'}{'measure'}{$item}{'outputArrayName'};
					  }
					  if(exists $ds->{'select'} && exists $ds->{'select'}{'measure'} && exists $ds->{'select'}{'measure'}{$item} && exists $ds->{'select'}{'measure'}{$item}{'outputDimName'}){
						$outputDimName = $ds->{'select'}{'measure'}{$item}{'outputDimName'};
					  }
					  if(!(exists $ds->{'select'} && exists $ds->{'select'}{'measure'} && exists $ds->{'select'}{'measure'}{$item})){
					  	next;
					  }
					  
					  if (!$outputArrayName) {$outputArrayName = $item;}
					  my $dataType=$translateDataType{$result->{ $type }->{ $partitionnumber }->{ $subtype }->{ $dimensionlistname }->{ $listattr }->{ $item }->{'dT'}};
                      if (!defined $dataType) {$dataType=$result->{ $type }->{ $partitionnumber }->{ $subtype }->{ $dimensionlistname }->{ $listattr }->{ $item }->{'dT'};}
                      if (exists $ds->{'select'} && exists $ds->{'select'}{'measure'} && exists $ds->{'select'}{'measure'}{$item} && exists $ds->{'select'}{'measure'}{$item}{'applyScale'} &&
           					$ds->{'select'}{'measure'}{$item}{'applyScale'} eq '1') {$dataType='double';} #We are applying the scale
                      print CDLFILE "\t      $dataType" .  
                       " $outputArrayName (";
                      #print "##debug          from productId: $result->{ $type }->{ $partitionnumber }->{ $subtype }->{ $dimensionlistname }->{ $listattr }->{ $item }->{'productId'}\n", if ( $debug );
                      my @arrayDims;
                      my $sql = qq{ select E_DIMENSIONNAME
                                     from ENTERPRISEDIMENSION ed, ENTERPRISEORDEREDDIMENSION eod, 
                                      ENTERPRISEDIMENSIONLIST edl
                                       where edl.E_DIMENSIONLISTID=eod.E_DIMENSIONLISTID
                                        and eod.E_DIMENSIONID=ed.E_DIMENSIONID
                                         and edl.E_DIMENSIONLISTNAME='$dimensionlistname'
                                          order by E_DIMENSIONORDER };
        
                      my $sth = $dbh->prepare($sql);
                      $sth->execute;
                      while ( my ( $line ) = $sth->fetchrow_array() ) {
                        push ( @arrayDims, $line );
                      }
                      
                      if (defined $outputDimName) {
	                  		$outputDimName =~ s/ /, /g;
                      		print CDLFILE "$outputDimName";
	                  }
                      	# Reset size variable for each variable
	                  my @size;
	                  for ( $i=0; $i<=$#arrayDims; $i++ ) {
	                  	#Check to see if array name matches dimension size
	                  	my $nameSize = $arrayDims[$i];
	                  	$nameSize =~ s/^.*-//;
	                  	$size[$i] =$result->{'dim'}{$arrayDims[$i]}{'size'};
	                  	if ($nameSize ne $size[$i]) {   #if not, then change name to match dimension size
	                  		$arrayDims[$i] =~ s/-.*$/-/;
	                  		$arrayDims[$i] = $arrayDims[$i] . $size[$i];
	                  	}
	                  	if (!defined $outputDimName) {
	                  		print CDLFILE "$arrayDims[$i]";
	                   		print CDLFILE ", ", if ( $i < $#arrayDims );
	                  	}
                      }
                  
                      print CDLFILE ");\n";
                      
                      # Special attribute for compression - chunk size segments compression by specific dimension (chunk) for each variable
                      my $chunkSize = '';
                      if(exists $ds->{'select'} && exists $ds->{'select'}{'measure'} && exists $ds->{'select'}{'measure'}{$item} && exists $ds->{'select'}{'measure'}{$item}{'chunkSize'}){
                      	$chunkSize = $ds->{'select'}{'measure'}{$item}{'chunkSize'};
                      }
                      
                      
                      #Set arrays for internal compression if true
                      if ($nc4CompressionFlag=~m/on/i) {
                      	print "********Applying internal NC4 Compression************\n";
                      	print CDLFILE "\t      $outputArrayName:_DeflateLevel=$compressionLevel;\n";
                      	print CDLFILE "\t      $outputArrayName:_Shuffle=1;\n";
       	                
                      	# if specific chunk size (X,Y) is entered in Production Rule
                      	if($chunkSize && (($chunkSize cmp "null")!=0)) {
                      		print "Using custom chunk size... $outputArrayName:_ChunkSizes= $chunkSize\n";
                      		print CDLFILE "\t      $outputArrayName:_ChunkSizes=" . $chunkSize . ";\n";
                      	}
                      	# else use default chunk size (= array size)
                      	else {
                      		print "Using default chunk size... $outputArrayName:_ChunkSizes=" . join (',',@size) . "\n";
                      		print CDLFILE "\t      $outputArrayName:_ChunkSizes=" . join (',',@size) . ";\n";
                      	}	
                      	
                      }

                      #Add array attributes
                      push(@dataArray,$item);

                      my @cfStrings = get_cf_array_attr( $ds, $dbh, $item, 
				$result->{ $type }->{ $partitionnumber }->{ $subtype }->{ $dimensionlistname }->{ $listattr }->{ $item }->{'productId'},
				$result->{ $type }->{ $partitionnumber }->{ $subtype }->{ $dimensionlistname }->{ $listattr }->{ $item }->{'originalFile'},
				$result->{ $type }->{ $partitionnumber }->{ $subtype }->{ $dimensionlistname }->{ $listattr }->{ $item }->{'fillValue'},
				$formatter, $debug );
                      print CDLFILE @cfStrings;
                      
                    } # for measure item (arrayname)
                  } # if 'Measure'
                } # for dln attribute (Measure/List/Rank) 
              } # for dimensionlistname
            } # if partition 0 'Cube'
          } # for subtype
        } # if '0'
      } # for partitionnumber
    } # if 'partition'
  }
  #Initialize arrays
#  print CDLFILE "\tdata:\n";
#  foreach my $dataArray (@dataArray) {
#  	print CDLFILE "\t      $dataArray = _;\n";
#  }
  print CDLFILE "}\n";
  close ( CDLFILE );
  print "**********Generating NC4 CDL File***************\n";
  $returnCode = system("ncgen -o $filename -k3 -x $outfilenameroot\.cdl");

}

sub get_Offset {

  my ( $fileFormat, $originalFile, $arrayName, $start ) = @_;

	#print "offset fF=$fileFormat, oF=$originalFile, aN=$arrayName, start=$start\n";
	my $offset="";
	$start=($start * 2) + 1;
	if ($fileFormat eq 'h5') {
		# Run h5dump and parse out the effin value
		my $command="h5dump -d $arrayName -m %.10f -s $start -S 1 -c 1 -y $originalFile";
		#print "command=$command\n";
		my $rc=`$command`;
		$rc =~ s/\n/ /g;
		#print "DYLAN rc=$rc\n";
		if ($rc =~ m/DATA {\s+(-?[\d+.]+)\s+}/ ) {
			#print "1=$1\n";
			$offset=$1;
			#print "DYLAN offset: $offset\n";
		}
	} else {
		my $command="ncdump -v $arrayName -p 8 $originalFile";
		my $rc=`$command`;
		$rc =~ s/\n/ /g;
		if ($rc =~ m/data:\s+$arrayName\s+=\s+\d+.+,\s+([\d+.]+)\s/) {
			$offset=$1;
		}		
	}
	# h5dump drops the decimal for float type, so put it back.
	if (index($offset, ".") == -1) { $offset .= ".0"; }
		return($offset)
	}

sub get_Scale {

  my ( $fileFormat, $originalFile, $arrayName, $start ) = @_;
	#print "scale fF=$fileFormat, oF=$originalFile, aN=$arrayName, start=$start\n";
	my $scale="";
	$start=($start*2) + 0;
	if ($fileFormat eq 'h5') {
		# Run h5dump and parse out the effin value
		my $command="h5dump -d $arrayName -m %.10f -s $start -S 1 -c 1 -y $originalFile";
		#print "command=$command\n";
		my $rc=`$command`;
		$rc =~ s/\n/ /g;
		#print "DYLAN: rc = $rc\n";
		if ($rc =~ m/DATA {\s+(-?[\d+.]+)\s+}/ ) {
			#print "1=$1\n";
			$scale=$1;
			#print "DYLAN scale: $scale\n";
			}
	} else {
		my $command="ncdump -v $arrayName -p 8 $originalFile";
		my $rc=`$command`;
		$rc =~ s/\n/ /g;
		if ($rc =~ m/data:\s+$arrayName\s+=\s+(\d+.+),/) {
			$scale=$1;
		}		
	}
	# h5dump drops the decimal for float type, so put it back.
	if (index($scale, ".") == -1) { $scale .= ".0"; }
		return($scale)
	}
sub get_cf_array_attr {

  my ( $ds, $dbh, $measurename, $productid, $originalFile, $fillValue, $formatter, $debug ) = @_;
  my $offset="";
  my $scale="";
  my $measurenameQF=$measurename;
  my @flagName=split('\@',$measurename);
  my @cfStrings;
  my $outputArrayName = '';
  if(exists $ds->{'select'} && exists $ds->{'select'}{'measure'} && exists $ds->{'select'}{'measure'}{$measurename} && exists $ds->{'select'}{'measure'}{$measurename}{'outputArrayName'}){
  	$outputArrayName = $ds->{'select'}{'measure'}{$measurename}{'outputArrayName'};
  }
  if (!$outputArrayName) {$outputArrayName = $measurename;}
  if ($measurename =~ m/(^.*_flag\d+)@/) {$measurenameQF=$1;}

  print "##debug         $measurename is an array (measure)\n", if ( $debug );
  #my $stringtomatch = "$measurename\_\%";
	# The attributes will be in either the H5 tables or in the nc4 tables, union return whichever they are in
# lhf 04/20/2010 added groupname to h5, null to nc4
  print "\tDYLAN*** measurename is $measurename and productID is $productid\n";
  my $sql = qq{
select 'h5', H_ARRAYATTRIBUTENAME,H_ARRAYATTRIBUTEDATATYPE, H_ARRAYATTRIBUTESTRINGVALUE,
 H_ARRAYATTRIBUTEDELIMITER,H_GROUPNAME, H_ARRAYNAME
  from HDF5_ARRAYATTRIBUTE haa, HDF5_ARRAY ha, HDF5_GROUP hg,ENTERPRISEMEASURE em,
   MEASURE_H_ARRAY_XREF mhax
    where haa.H_ARRAYID=ha.h_ARRAYID
     and ha.H_GROUPID = hg.H_GROUPID
      and hg.PRODUCTID = $productid
       and em.MEASUREID = mhax.MEASUREID
        and mhax.H_ARRAYID = ha.H_ARRAYID
         and MEASURENAME = '$measurename'
          and (H_ARRAYATTRIBUTENAME in ('long_name','units','_FillValueName',
               '_FillValue', 'valid_min','valid_max', 'scale_factor', 'add_offset') 
           or H_ARRAYATTRIBUTENAME like '$measurenameQF\_%')
union
select 'nc4', N_ARRAYATTRIBUTENAME, N_ARRAYATTRIBUTEDATATYPE, N_ARRAYATTRIBUTESTRINGVALUE,
 N_ARRAYATTRIBUTEDELIMITER,'null','null'
  from nc4_ARRAYATTRIBUTE naa, nc4_ARRAY na, nc4_GROUP ng, ENTERPRISEMEASURE em, 
   MEASURE_N_ARRAY_XREF mnax
    where naa.n_ARRAYID=na.n_ARRAYID
     and na.n_GROUPID = ng.n_GROUPID
      and ng.PRODUCTID = $productid
       and em.MEASUREID = mnax.MEASUREID
        and mnax.N_ARRAYID = na.N_ARRAYID
         and MEASURENAME = '$measurename'
          and N_ARRAYATTRIBUTENAME not like '%_flag%'
  };
#          and (lower(n_ARRAYATTRIBUTENAME) in ('long_name','units','_fillvaluename',
#               '_fillvalue', 'valid_min','valid_max', 'scale_factor', 'add_offset','description','scale','offset',
#               'missing_valuename','missing_value') 
#           or n_ARRAYATTRIBUTENAME like '$measurename\_\%')
#		 };

  my $sth = $dbh->prepare( $sql );
  $sth->execute;

#***** we need to take the arrayname's attribute and give it to the measure
#**** right now we dont have the measure, we have the array right?
#*** bfdd



print $sql;
  while ( my ( $fileFormat, $aan, $aadt, $aasv, $aad, $gn, $an ) = $sth->fetchrow_array() ) 
  {
    my $cfArrayAttributeString;
    print "\n***DYLAN $measurename: $fileFormat, $aan, $aadt, $aasv, $aad, $gn\n";
    if ( $aasv ne "" ) {
      if ( lc $aan =~ m/units/i ) {
        $cfArrayAttributeString = "              $outputArrayName\:units = \"$aasv\";\n";
        $cfArrayAttributeString = "              $outputArrayName\:units = \"degrees_north\";\n", if ( uc $outputArrayName eq "LATITUDE" );
        $cfArrayAttributeString = "              $outputArrayName\:units = \"degrees_east\";\n", if ( uc $outputArrayName eq "LONGITUDE" );
      }   
      elsif ( $aan eq "_FillValueName" ) {
        $cfArrayAttributeString = "              $outputArrayName\:missing_valuename = \"";
        #my @mvn = split /,/, $aasv;
        my @mvn = split /$aad/, $aasv;
        for ( $i = 0; $i <= $#mvn; $i++ ) {
          $cfArrayAttributeString .= $mvn[$i];
          $cfArrayAttributeString .= ", ", if ( $i < $#mvn );
        }
        $cfArrayAttributeString .= "\"\;\n";
      }
      elsif ( $aan eq "_FillValue" ) {
        $cfArrayAttributeString = "              $outputArrayName\:missing_value = ";
        $cfArrayAttributeString = "              $aadt $outputArrayName\:missing_value = ", if ( defined($formatter) && !defined($fillValue) );
        #my @mv = split /,/, $aasv;
        my @mv = split /$aad/, $aasv;
        for ( $i = 0; $i <= $#mv; $i++ ) {
          $cfArrayAttributeString .= $mv[$i];
          $cfArrayAttributeString .= ", ", if ( $i < $#mv );
        }
        $cfArrayAttributeString .= "\;\n";
      }
      elsif ( lc $aan =~ m/offset/i && $aan !~ m/_flag\d+/) {
        if (exists $ds->{'select'} && exists $ds->{'select'}{'measure'} && exists $ds->{'select'}{'measure'}{$measurename} && exists $ds->{'select'}{'measure'}{$measurename}{'applyScale'} &&
            $ds->{'select'}{'measure'}{$measurename}{'applyScale'} eq '1') {
          $cfArrayAttributeString = "              $aadt $outputArrayName\:missing_value = ";
          $cfArrayAttributeString .= $fillValue . "\;\n";  	
        } else {
          $cfArrayAttributeString = "              $outputArrayName\:add_offset = ";
          $cfArrayAttributeString = "              float $outputArrayName\:add_offset = " , if ( defined($formatter) );
          #my @mv = split /,/, $aasv;
          my @mv;
          if (defined($aad) && length($aad) > 0) {
            @mv = split /$aad/, $aasv; 
          } else {
            $mv[0] = $aasv; 
          }
          for ( $i = 0; $i <= $#mv; $i++ ) {
            if ($mv[$i] =~ m/Factors/) {
            	if ($fileFormat =~ m/h5/) {
            		$offset=get_Offset($fileFormat, $originalFile, "$gn\/$mv[$i]", $i);
            	} else {
            		$offset=get_Offset($fileFormat, $originalFile, "$mv[$i]",$i);
            	}
            } else {
            	$offset = $mv[$i];
            }
            $cfArrayAttributeString .= $offset;
            $cfArrayAttributeString .= ", ", if ( $i < $#mv );
          }
         $cfArrayAttributeString .= "\;\n";
        }
      }
  
      elsif ( lc $aan =~ m/scale/i ) {
        if (exists $ds->{'select'} && exists $ds->{'select'}{'measure'} && exists $ds->{'select'}{'measure'}{$measurename} && exists $ds->{'select'}{'measure'}{$measurename}{'applyScale'} &&
           $ds->{'select'}{'measure'}{$measurename}{'applyScale'} eq '1') {
          # nop
        } else {
          $cfArrayAttributeString = "              $outputArrayName\:scale_factor = ";
          $cfArrayAttributeString = "              float $outputArrayName\:scale_factor = ", if ( defined($formatter) );
          #my @mv = split /,/, $aasv;
          my @mv;
          if (defined($aad) and length($aad) > 0) {
            @mv = split /$aad/, $aasv; 
          } else {
            $mv[0] = $aasv; 
          }
          for ( $i = 0; $i <= $#mv; $i++ ) {
            if ($mv[$i] =~ m/Factors/) {
            	if ($fileFormat =~ m/h5/) {
            		$scale=get_Scale($fileFormat, $originalFile, "$gn\/$mv[$i]", $i);
            	} else {
            		$scale=get_Scale($fileFormat, $originalFile, "$mv[$i]",$i);
            	}
            } else {
            	$scale = $mv[$i];
            }
            $cfArrayAttributeString .= $scale;
            $cfArrayAttributeString .= ", ", if ( $i < $#mv );
          }
         $cfArrayAttributeString .= "\;\n";
        }
      }
  
      elsif ( lc $aan =~ m/$measurenameQF\_/i ) {
        #print "debug statement:\ndebug statement: \$aan: $aan <===> \$aasv: $aasv <===>\$aad: $aad\ndebug statement:\ndebug statement:\n";
        if((index($aan,"bit_offset")==-1)&&(index($aan,"bit_length")==-1)){
        	#my $outputArrayNameEnd = substr($outputArrayName,length($an)+1);
        	my $aanShort = substr($aan, length($measurenameQF)+1);
        	my $outputArrayNameEnd = $outputArrayName;
	        if ($outputArrayName =~ m/_flag\d+/) {;}
	        if (defined $aad and length($aad) > 0) {
	          $cfArrayAttributeString = "              $outputArrayName\:$aanShort = ";
	          my @delimitedvalues = split /$aad/, $aasv;
	          $cfArrayAttributeString .= "\"", if ( $aan =~ m/_meanings/ );
	          for ( $i = 0; $i <= $#delimitedvalues; $i++ ) {
	            #$cfArrayAttributeString .= "\"$delimitedvalues[$i]\"", if ( $aan =~ m/_meanings/ );
	            $cfArrayAttributeString .= "$delimitedvalues[$i]", if ( ($aan =~ m/_values/) || ($aan =~ m/_meanings/));
	            $cfArrayAttributeString .= ", ", if ( $i < $#delimitedvalues );
	          }
	          $cfArrayAttributeString .= "\"", if ( $aan =~ m/_meanings/ );
	          $cfArrayAttributeString .= "\;\n";
	        } else {
	          $cfArrayAttributeString = "              $outputArrayName\:$aanShort = \"$aasv\";\n"; 
	        }
        }
      }
      else {
      	my @mv;
        print "***** jrh debug: in else under my mv\n";
	print "aad==>$aad<==, length of aad =" . length($aad) . "<==.\n";
        if (defined($aad) && length($aad) > 0 ) {
                print "***** jrh debug: \$aad is defined.\n **** jrh debug: \$aasv is $aasv\n  *** jrh debug: \$aad is $aad\n";
        	$cfArrayAttributeString = "              $outputArrayName\:$aan = ";
        	$cfArrayAttributeString .= "\"", if ( $aan =~ m/_meanings/ );
        	@mv = split /$aad/, $aasv;
            for ( $i = 0; $i <= $#mv; $i++ ) { 	
                        print "***** jrh debug: \$mv[$i] = $mv[$i]\n";
	        	$cfArrayAttributeString .= "$mv[$i]";
	        	$cfArrayAttributeString .= ", ", if ( $i < $#mv );
	       	}
	       	$cfArrayAttributeString .= "\"", if ( $aan =~ m/_meanings/ );
	       	$cfArrayAttributeString .= "\;\n";
	       	$cfArrayAttributeString = "              $outputArrayName\:valid_min = \"$aasv\";\n", if ( $aan =~ m/valid_min/i );
        	$cfArrayAttributeString = "              $outputArrayName\:valid_max = \"$aasv\";\n", if ( $aan =~ m/valid_max/i ); 
        	$cfArrayAttributeString = "              $aadt $outputArrayName\:valid_min = \"$aasv\";\n", if ( $aan =~ m/valid_min/i && defined($formatter));
        	$cfArrayAttributeString = "              $aadt $outputArrayName\:valid_max = \"$aasv\";\n", if ( $aan =~ m/valid_max/i && defined($formatter)); 
                print "***** jrh debug: \$cfArrayAttributeString = $cfArrayAttributeString\n";
        }
        else {
                print "***** jrh debug: \$aad is not defined.\n";
        	$cfArrayAttributeString = "              $outputArrayName\:$aan = \"$aasv\";\n";
        	$cfArrayAttributeString = "              $outputArrayName\:$aan = \"$aasv\";\n", if ( $aadt =~ m/string/i );
        	if ( defined($formatter) ) {
        		$aadt = 'char', if ($aadt =~ m/string/);
        		$cfArrayAttributeString = "              $aadt $outputArrayName\:$aan = \"$aasv\";\n";
        	}
        }
      }
  } else {
  	  if ( lc $aan =~ m/valid_max/i) {
      	$cfArrayAttributeString = "              $outputArrayName\:valid_max = \"180.\";\n", if ( $outputArrayName =~ m/^LONGITUDE/i );
        $cfArrayAttributeString = "              $outputArrayName\:valid_max = \"90.\";\n", if ( $outputArrayName =~ m/^LATITUDE/i );
        $cfArrayAttributeString = "              float $outputArrayName\:valid_max = \"180.\";\n", if ( $outputArrayName =~ m/^LONGITUDE/i && defined($formatter) );
        $cfArrayAttributeString = "              float $outputArrayName\:valid_max = \"90.\";\n", if ( $outputArrayName =~ m/^LATITUDE/i && defined($formatter) );
      } elsif ( lc $aan =~ m/valid_min/i) {
      	$cfArrayAttributeString = "              $outputArrayName\:valid_min = \"-180.\";\n", if ( $outputArrayName =~ m/^LONGITUDE/i );
      	$cfArrayAttributeString = "              $outputArrayName\:valid_min = \"-90.\";\n", if ( $outputArrayName =~ m/^LATITUDE/i );
      	$cfArrayAttributeString = "              float $outputArrayName\:valid_min = \"-180.\";\n", if ( $outputArrayName =~ m/^LONGITUDE/i && defined($formatter) );
      	$cfArrayAttributeString = "              float $outputArrayName\:valid_min = \"-90.\";\n", if ( $outputArrayName =~ m/^LATITUDE/i && defined($formatter) );
      } #else {
      	#if ($aan eq "_FillValue") {
      	#	$cfArrayAttributeString = "              $measurename\:$aan = 0;\n", #empty _FillValue is not allowed in nc4 cdl file
      	#} else {
      	#	$cfArrayAttributeString = "              $measurename\:$aan = \"\";\n",
      	#}      	
      #}       
  }
  push ( @cfStrings, $cfArrayAttributeString );
  }
  return @cfStrings;
}

# This subroutine expects one argument - a reference to the odJobSpec structure.
# It returns all of the input file IDs (for all products) in an array.
sub getAllInputFileIds {
	my $odJobSpec = $_[0];

	my @allInputFileIds = ();
	foreach my $inputSpec ( @{$odJobSpec->{'inputs'}} ) {
		foreach my $fileId ( @{$inputSpec->{'fileIds'}} ) {
			push @allInputFileIds, $fileId;
		}
	}
	return @allInputFileIds;
}

# This subroutine expects one array reference as an argument
# It returns a string like "?, ?, ?" - as many ?'s as elements in the array.
sub getPlaceholderStringForArray {
	my $arrayRef = $_[0];
	return join(", ", ('?') x @$arrayRef);
}

# This subroutine expects a single argument, a string of the form MULTIPOLYGON(((lon1 lat1, lon2 lat2, ... lonN latN, lon1 lat1))), where N is either 4 or 8.
# It returns a hash, containing a pair of parallel arrays, 'allLatitudes' and 'allLongitudes', which containing all 4 or 8 elements of the multipolygon, 
# *and* another pair of parallel arrays, 'cornerLatitudes' and 'cornerLongitudes', containing only the four corner points of the multipolygon.
# The final lon1 lat1 pair is not added to ethese arrays, so each coordinate pair is distinct. 
sub parseMultipolygonString {
	my $multipolygonString = $_[0];

	$multipolygonString =~ s/MULTIPOLYGON\(\(\(\s*//g;
	$multipolygonString =~ s/\s*\)\)\)//g;

	my @multipolygonCoordinatePairs = split(",", $multipolygonString);
	
	my @latitudes = ();
	my @longitudes = ();

	foreach my $pair ( @multipolygonCoordinatePairs ) {
		my ($longitude, $latitude) = split(" ", $pair);
		if (scalar(@latitudes) == 0 or $latitudes[0] ne $latitude or $longitudes[0] ne $longitude) {
			push @longitudes, $longitude;
			push @latitudes, $latitude;
		}
	}

	my @cornerLatitudes;
	my @cornerLongitudes;

	my @midpointLatitudes;
	my @midpointLongitudes;

	if (scalar(@latitudes) == 4) {
		@cornerLatitudes = @latitudes;
		@cornerLongitudes = @longitudes;

		@midpointLatitudes = ( ($latitudes[0] + $latitudes[1]) / 2, ($latitudes[1] + $latitudes[2]) / 2, ($latitudes[2] + $latitudes[3]) / 2, ($latitudes[3] + $latitudes[0]) / 2);
		@midpointLongitudes = ( ($longitudes[0] + $longitudes[1]) / 2, ($longitudes[1] + $longitudes[2]) / 2, ($longitudes[2] + $longitudes[3]) / 2, ($longitudes[3] + $longitudes[0]) / 2);
	} elsif (scalar(@latitudes) == 8) {
		@cornerLatitudes = @latitudes[0, 2, 4, 6];
		@cornerLongitudes = @longitudes[0, 2, 4, 6];

		@midpointLatitudes = @latitudes[1, 3, 5, 7];
		@midpointLongitudes = @longitudes[1, 3, 5, 7];
	} else {
		print "ERROR: In sub parseMultipolygonString: cannot handle " . scalar(@latitudes) . "-sided polygon.\n";
		print "DEBUG: Multipolygon was: " . $_[0] . "\n";
		die;
	}
	
	my %result = ();
	$result{'allLatitudes'} = [@latitudes];
	$result{'allLongitudes'} = [@longitudes];
	$result{'cornerLatitudes'} = [@cornerLatitudes];
	$result{'cornerLongitudes'} = [@cornerLongitudes];
	$result{'midpointLatitudes'} = [@midpointLatitudes];
	$result{'midpointLongitudes'} = [@midpointLongitudes];
	return %result;
}

sub distanceLL {
	my ($lat1, $lon1, $lat2, $lon2) = @_;

	my $phi1 = deg2rad(90 - $lat1);
	my $phi2 = deg2rad(90 - $lat2);

	my $theta1 = deg2rad($lon1);
	my $theta2 = deg2rad($lon2);

	my $central_angle = great_circle_distance($theta1, $phi1, $theta2, $phi2); # In radians.
	print "distanceLL( " . $lat1 . ", " . $lon1 . "; " . $lat2 . ", " . $lon2 . " ) = " . $central_angle . "\n";

	return $central_angle;
}

1;
