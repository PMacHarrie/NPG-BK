#!/bin/bash

#echo $$
python jumpEc2.py $$
sshCmd=$(cat "jumpEc2."$$)
rm "jumpEc2."$$
#echo $sshCmd
eval $sshCmd

