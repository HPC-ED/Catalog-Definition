#!/bin/bash -x

curl -o uiuc_training.json \
  "https://info.xsede.org/wh1/resource-api/v3/resource_search/?affiliation=uiuc.edu&resource_groups=Streamed%20Events,Live%20Events&format=json"

curl -o uiuc_training_local.json \
  "https://info.xsede.org/wh1/resource-api/v3/local_search/?affiliation=uiuc.edu&localtypes=resource&format=json"

ls -l uiuc_training*
