#!/usr/bin/env python

import sys
import xml.etree.ElementTree
import json

def select_json_to_dictionary(json_string):
	return json.loads(json_string)

def select_dictionary_to_tailoring_XML(select_statement):
	"""
	Returns the XML representation of the given select statement, as a string.
	"""
	dss_element = xml.etree.ElementTree.Element('dss')
	select_element = xml.etree.ElementTree.SubElement(dss_element, 'select')

	if 'measures' in select_statement:
		for measure_object in select_statement['measures']:
			xml.etree.ElementTree.SubElement(select_element, 'measure', measure_object)
            
	if 'where' in select_statement:
		where_statement = select_statement['where']
		where_element = xml.etree.ElementTree.SubElement(select_element, 'where')
        
		if 'subSample' in where_statement:
			subSample_statement = where_statement['subSample']
			subSample_element = xml.etree.ElementTree.SubElement(where_element, 'subSample')
            
			if 'dimensions' in subSample_statement:
				for dimension_object in subSample_statement['dimensions']:
					xml.etree.ElementTree.SubElement(subSample_element, 'dimension', dimension_object)
                    
		if 'filter' in where_statement:
			filter_statement = where_statement['filter']
			filter_element = xml.etree.ElementTree.SubElement(where_element, 'filter')
            
			if 'args' in filter_statement:
				for arg_object in filter_statement['args']:
					xml.etree.ElementTree.SubElement(filter_element, 'arg', arg_object)

	return xml.etree.ElementTree.tostring(dss_element)

# __main__:
if len(sys.argv) != 2:
	print "USAGE: %s <json>" % sys.argv[0]
	print "Got: %s" % (' --- '.join(sys.argv))
else:
	print select_dictionary_to_tailoring_XML(select_json_to_dictionary(sys.argv[1]))
