#!/bin/bash

sudo mkdir -p /efs
sudo mount -t nfs -o nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2 fs-e9a39da0.efs.us-east-1.amazonaws.com:/ /efs

aws s3 cp s3://eqtl-example-bucket/dotRenviron /home/ubuntu/.Renviron

