
import h5py
import numpy
import sys
import os


if len(sys.argv) != 2:
    print("Usage: %s <file>" % sys.argv[0])
    raise Exception("Usage: %s <file>" % sys.argv[0])

file = sys.argv[1]

path_to_file = file
if '/' not in file and 'incomingdir' in os.environ:
    path_to_file = os.environ['incomingdir'] + '/' + file

if not os.path.exists(path_to_file):
    print("Unable to find file: " + path_to_file)
    raise Exception("Unable to find file: " + path_to_file)
	
f = h5py.File(path_to_file, 'r')

obsStartTime = f.attrs.get('ObservationStartDateTime');
obsEndTime = f.attrs.get('ObservationEndDateTime');

f.close()

if obsStartTime is None:
	raise Exception("Unable to retrieve ObservationStartDateTime")
if obsEndTime is None:
	raise Exception("Unable to retrieve ObservationEndDateTime")
	
obsStartTimeStr = obsStartTime[0].decode()
obsEndTimeStr = obsEndTime[0].decode()

# print(obsStartTimeStr)
# print(obsEndTimeStr)

obsStartTimeStr = obsStartTimeStr.replace('T', ' ').replace('Z', '')
obsEndTimeStr = obsEndTimeStr.replace('T', ' ').replace('Z', '')
  
print(obsStartTimeStr + '&' + obsEndTimeStr)
