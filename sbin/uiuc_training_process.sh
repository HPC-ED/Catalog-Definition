#!/bin/bash

# PRE-REQUISIE: a python3 in the environement with 

echo "** Retrieving UIUC sample metadata from XSEDE APIs"

curl -s -o data/uiuc_training.json \
  "https://info.xsede.org/wh1/resource-api/v3/resource_search/?affiliation=uiuc.edu&resource_groups=Streamed%20Events,Live%20Events&format=json"
ls -l data/uiuc_training.json

curl -s -o data/uiuc_training_local.json \
  "https://info.xsede.org/wh1/resource-api/v3/local_search/?affiliation=uiuc.edu&localtypes=resource&format=json"
ls -l data/uiuc_training_local.json

echo "** Loading UIUC sample metadata"
echo python3 ./PROD/bin/uiuc_training_load.py -c conf/uiuc_training_load.conf -s file:data/uiuc_training_local.json -l debug
python3 ./PROD/bin/uiuc_training_load.py -c conf/uiuc_training_load.conf -s file:data/uiuc_training_local.json -l debug
