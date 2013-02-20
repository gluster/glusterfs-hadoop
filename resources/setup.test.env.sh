#* Copyright (c) 2011 Gluster, Inc. <http://www.gluster.com>
#  This file is part of GlusterFS.
#
#  Licensed under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License. You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#  implied. See the License for the specific language governing
#  permissions and limitations under the License.
#
# This script creates a gluster volume using block files mounted on loopback as bricks.
# The puprose is to allow scripted create and delete of gluster volumes without needing to alter
# disk partition or volume structure.

print_help() {
    echo "Usage: $0 [OPTION]..."
    echo ""
    echo "Options:"
    echo " -w, --work <directory>   		working directory for gluster block device bricks"
    echo " -v, --volume <gluster volume>	gluster volume to create"
    echo " -h, --help               		show this message"
    echo ""
    echo "Note: bricks and gluster volume will not persist on reboot.  Please edit fstab manually if you wish."
}

# parse options
while [ "${1+isset}" ]; do
    case "$1" in
        -v|--volume)
            GLUSTER_VOLUME=$2
            shift 2
            ;;
        -w|--work)
            WORK_DIR=$2
            shift 2
            ;;
        -h|--help)
            print_help
            exit
            ;;
        *)
            echo "Error: Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

if  [ -z ${GLUSTER_VOLUME} ] 
then
    echo "I am error.  No volume specified."
    echo ""
    print_help
	exit 1;
fi

if  [ -z ${WORK_DIR} ] 
then
    echo "I am error. No temp directory set."
    echo ""
    print_help
	exit 1;
fi

NUMBER_OF_BRICKS=3

mkdir -p ${WORK_DIR}/rpm
mkdir -p ${WORK_DIR}/bricks
mkdir -p ${WORK_DIR}/blocks



# Grap and extract the RPM
#cd ${WORK_DIR}/rpm
#wget ${GLUSTER_URL}
#rpm2cpio *.rpm | cpio -idmv

#create some loopback bricks

createGlusterVolume(){
	LOOP_BRICKS=( )
	
	# TODO: Need to run a force on this command so it doesn't prompt

	GLUSTER_VOLUME_CMD="gluster volume create ${GLUSTER_VOLUME} "
	HOSTNAME=`hostname`
	
	for ((  i = 0 ;  i <= ${NUMBER_OF_BRICKS};  i++  ))
	 do
	# Create an empty block device to use as loop back filesystem
		BLOCK=${WORK_DIR}/blocks/block${i}
		BRICK=${WORK_DIR}/bricks/brick${i}
		
		dd if=/dev/zero of=${BLOCK} bs=1024 count=30720
		
	# find a free loopback device
	# LOOP_BRICKS[${i}]=${WORK_DIR}/bricks/brick${i}
		LB_BRICK=`losetup -f`
		LOOP_BRICKS[${i}]="${LB_BRICK}"
		
		
		echo "creating loopback file system on loopback: ${LB_BRICK} block: ${BLOCK}"
		losetup ${LB_BRICK} ${BLOCK}
		echo "Making loopback block brick on ${LB_BRICK}"
	# mkfs.xfs -m 1 -v ${BLOCK}
		mkfs -t ext4 -m 1 -v ${LB_BRICK}
		
		
		mkdir -p ${BRICK}
		mount -t ext4 $LB_BRICK  $BRICK
		GLUSTER_VOLUME_CMD="${GLUSTER_VOLUME_CMD} ${HOSTNAME}:${BRICK}" 
	 done

	# Run the gluster command to create the volume

	
	echo "running: ${GLUSTER_VOLUME_CMD}"
	$GLUSTER_VOLUME_CMD
	gluster volume start ${GLUSTER_VOLUME}
}

createCleanupScript(){
	# clean up after ourselfs
	# create an unmount script for the bricks
        echo "#!/bin/sh" > ${WORK_DIR}/cleanup.sh
        chmod +x ${WORK_DIR}/cleanup.sh

	echo "gluster volume stop ${GLUSTER_VOLUME}" >> ${WORK_DIR}/cleanup.sh
	echo "gluster volume delete ${GLUSTER_VOLUME}" >> ${WORK_DIR}/cleanup.sh
	# Unmount the bricks and loopback devices

	for ((  i = 0 ;  i <= ${NUMBER_OF_BRICKS};  i++  ))
	 do
	    # Create an empty block device to use as loop back filesystem
		BLOCK=${WORK_DIR}/blocks/block${i}
		BRICK=${WORK_DIR}/bricks/brick${i}
		echo "umount $BRICK" >> ${WORK_DIR}/cleanup.sh
		echo "losetup -d ${LOOP_BRICKS[$i]}" >> ${WORK_DIR}/cleanup.sh
		echo "rm -rf $BLOCK" >> ${WORK_DIR}/cleanup.sh
		echo "rm -rf $BRICK" >> ${WORK_DIR}/cleanup.sh
		
	 done
	
	echo "rm -rf ${WORK_DIR}" >> ${WORK_DIR}/cleanup.sh
}

createGlusterVolume
createCleanupScript
echo "Cleanup script: ${WORK_DIR}/cleanup.sh"
