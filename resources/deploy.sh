#!/bin/sh

NODE_LIST=("bc-intel1" "bc-intel2")
DIRECTORY=/usr/lib/hadoop/lib

for HOST in "${NODE_LIST[@]}"
 do
  echo "scp ../target/glusterfs-0.20.2-0.1.jar ${HOST}:/${DIRECTORY}"
  scp ./target/glusterfs-0.20.2-0.1.jar "${HOST}:/${DIRECTORY}"
 
 done
