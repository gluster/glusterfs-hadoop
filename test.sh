#!/bin/sh

# this could be conditionally set.  it may be set higher up as part of the global env variables.
rm -rf target/
export GLUSTER_MOUNT=/mnt/glusterfs
export HCFS_FILE_SYSTEM_CONNECTOR=org.apache.hadoop.fs.test.connector.glusterfs.GlusterFileSystemTestConnector
export HCFS_CLASSNAME=org.apache.hadoop.fs.glusterfs.GlusterFileSystem
rm -rf target/

mvn package

export HCFS_CLASSNAME=org.apache.hadoop.fs.glusterfs.GlusterFileSystemCRC

mvn package

unset HCFS_FILE_SYSTEM_CONNECTOR
unset HCFS_CLASSNAME


