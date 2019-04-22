#!/usr/bin/env python2.7
"""
@author: dcp
"""

#Dictionary for mapping proj4 projections to netCDF Climate Forecast convention metadata (gridded products)
# http://cfconventions.org/Data/cf-conventions/cf-conventions-1.6/build/cf-conventions.html#appendix-grid-mappings
grid_map_dictionary = {
                       'lcc' : {
                                'grid_mapping_name': 'lambert_conformal_conic',
                                'grid_parameters': {
                                                    'lat_1': 'standard_parallel_1', 
                                                    'lat_2': 'standard_parallel_2',
                                                    'lon_0': 'longitude_of_central_meridian',
                                                    'lat_0': 'latitude_of_projection_origin',
                                                    'x_0': 'false_easting',
                                                    'y_0': 'false_northing'
                                                    }
                                },
                       'merc' : {
                                 'grid_mapping_name': 'mercator',
                                 'grid_parameters': {
                                                     'lon_0': 'longitude_of_projection_origin',
                                                     'lat_1': 'standard_parallel_1',
                                                     'lat_ts': 'latitude_of_true_scale',
                                                     'k_0': 'scale_factor_at_projection_orgin',
                                                     'x_0': 'false_easting',
                                                     'y_0': 'false_northing'
                                                     }
                                 },
                       'stere' : {
                                  'grid_mapping_name': 'polar_stereographic',
                                  'grid_parameters': {
                                                      'lon_0': 'straight_vertical_longitude_from_pole',
                                                      'lat_0': 'latitude_of_projection_origin',
                                                      'lat_ts': 'latitude_of_true_scale',
                                                      'x_0': 'false_easting',
                                                      'y_0': 'false_northing'
                                                      }
                                  },
                       'eqc' : { #Note that there is no currently defined CF convention for platte caree as of CF v1.6
                                'grid_mapping_name': 'equidistant_cylindrical', 
                                'grid_parameters': {
                                                    'lon_0': 'longitude_of_central_meridian',
                                                    'lat_0': 'latitude_of_projection_origin',
                                                    'lat_ts': 'latitude_of_true_scale',
                                                    'x_0': 'false_easting',
                                                    'y_0': 'false_northing'
                                                    }
                                }
                       }

#Data ranges for each numpy data type.
dataRange = {
    'int8': (-1*(2**7), 2**7-1),
    'uint8': (0, 2**8-1),
    'int16': (-1*(2**15), 2**15-1),
    'uint16': (0, 2**16-1),
    'int32': (-1*(2**31), 2**31-1),
    'uint32': (0, 2**32-1),
    'int64': (-1*(2**63), 2**63-1),
    'uint64': (0, 2**64-1),
    'float32': (None, None),
    'float': (None, None),
    'float64': (None, None)
    }
