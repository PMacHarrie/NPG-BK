"""
    Author: Peter MacHarrie; SOLERS INC
    Contact: hieu.phung@solers.com
    Last modified: 2019-01-02
    Python Version: 3.6
"""

import json
import h5py
import sys
import numpy


jsonVar = {
    "attributes" : {},
    "datasets"   : {},
    "datatypes"  : {}
}

def insertDynKeyGroup(node, keys, value):
#   print "keys=", keys,
#   print "value=", value
    if len(keys) == 1:
        key=keys[0]
        node[key]=value
    else:
        insertDynKeyGroup(node[keys[0]], keys[1:], value)
    
def insertDynKeyObject(node, keys, objectType, name, value):
#   print "obj keys=", keys, "name=", name, "type=", objectType,
#   print "obj value=", value
    if len(keys) == 1:
        key=keys[0]
        if objectType in node[keys[0]]:
            node[key][objectType][name]=value
        else:
            node[key][objectType]={}
            node[key][objectType][name]=value
    else:
        insertDynKeyObject(node[keys[0]], keys[1:], objectType, name, value)
    
def visit_h5obj(name, node):

    tmpJ = {}

#   print "name=", name, "node=", node
    groups = node.name.split("/")[1:]
    if isinstance(node, h5py.Group):
#       print "This is a group"
#       print "groupName=", groups
#       print "len gn=", len(groups)
#       jsonVar[grpRef] = { "attributes": {}, "datasets" : {}, "datatypes" : {} }
        insertDynKeyGroup(jsonVar, groups, {})

    if isinstance(node, h5py.Dataset):
        groups = node.name.split("/")[1:-1]
        tmpJ = { 
            "datatype" : str(node.dtype),
            "group" : str(node.parent.name),
            "size"  : int(node.size),
            "shape"  : node.shape,
        }
#       print "DataSet=", tmpJ
        names = node.name.split("/")
#       print "names=", names[-1]
        name = names[-1]
        insertDynKeyObject(jsonVar, groups, 'datasets', name, tmpJ)       
#       jsonVar[grpRef]['datasets'][names[-1]] = tmpJ

#    print ("type=", tmpJ)
#    if 'size' in tmpJ:
#        print ("size-", tmpJ['size'])

    for key, val in node.attrs.items():
#       print "****Attr ****", tmpJ
#       print "****Attr **** dtype=", val.dtype
#       print "****Attr **** shape=", val.shape
#       print "****Attr **** size=", val.size
        tmpVal = []
        for i in val:
#           print "val=", i[0]
            if not (numpy.issubdtype(numpy.bytes_, i.dtype) or numpy.issubdtype(numpy.str, i.dtype)):
#               print "val as Float=", i[0].astype('float')
                tmpVal.append( i[0].astype('float') )
            else:
                tmpVal.append(i[0].decode())
        tmp = ""
        if val.size == 1:
            tmp = tmpVal[0],
        else:
            tmp =  tmpVal,
#       jsonVar['attributes'][key] = tmp[0]
        insertDynKeyObject(jsonVar, groups, 'attributes', key, tmp[0])
#       print "****Attr ****", tmpJ


def h52json(fileName):
#f = h5py.File('GITCO_npp_d20180702_t1009061_e1010303_b34604_c20180702120236129973_niic_int.h5', 'r')
#f = h5py.File(sys.argv[1], 'r')
    f = h5py.File(fileName, 'r')

# Do root attributes first
    for key, val in f.attrs.items():
#   print "key=", key, "value=", val
     tmpVal = []
     for i in val:
#        print ("i=",i, "key=", key, "dtype=", i.dtype)
        if not (numpy.issubdtype(numpy.bytes_, i.dtype) or numpy.issubdtype(numpy.str, i.dtype)):
            tmpVal.append( i[0].astype('float') )
        else:
            tmpVal.append(i[0].decode())
     tmp = ""
     if val.size == 1:
        tmp = tmpVal[0],
     else:
        tmp = tmpVal.decode(),
    
     jsonVar['attributes'][key] = tmp[0]

    f.visititems(visit_h5obj)

# set edmCore metadata attributes

#    jsonVar['edmCore']['platformNames'].append(jsonVar['attributes']["Platform_Short_Name"])
#print (jsonVar)
#    print (json.dumps(jsonVar))
    return (jsonVar)
