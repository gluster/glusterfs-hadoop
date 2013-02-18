#!/bin/sh
GLUSTER_URL=http://download.gluster.org/pub/gluster/glusterfs/3.3/3.3.1/EPEL.repo/epel-6/x86_64/glusterfs-server-3.3.1-1.el6.x86_64.rpm
TMP_DIR=/tmp/gluster-test
NUMBER_OF_BRICKS=3
GLUSTER_VOLUME=hadooptest

mkdir -p ${TMP_DIR}/rpm
mkdir -p ${TMP_DIR}/bricks
mkdir -p ${TMP_DIR}/blocks



# Grap and extract the RPM
#cd ${TMP_DIR}/rpm
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
		BLOCK=${TMP_DIR}/blocks/block${i}
		BRICK=${TMP_DIR}/bricks/brick${i}
		
		dd if=/dev/zero of=${BLOCK} bs=1024 count=30720
		
	# find a free loopback device
	# LOOP_BRICKS[${i}]=${TMP_DIR}/bricks/brick${i}
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
        echo "#!/bin/sh" > ${TMP_DIR}/cleanup.sh
        chmod +x ${TMP_DIR}/cleanup.sh

	echo "gluster volume stop ${GLUSTER_VOLUME}" >> ${TMP_DIR}/cleanup.sh
	echo "gluster volume delete ${GLUSTER_VOLUME}" >> ${TMP_DIR}/cleanup.sh
	# Unmount the bricks and loopback devices

	for ((  i = 0 ;  i <= ${NUMBER_OF_BRICKS};  i++  ))
	 do
	    # Create an empty block device to use as loop back filesystem
		BLOCK=${TMP_DIR}/blocks/block${i}
		BRICK=${TMP_DIR}/bricks/brick${i}
		echo "umount $BRICK" >> ${TMP_DIR}/cleanup.sh
		echo "losetup -d ${LOOP_BRICKS[$i]}" >> ${TMP_DIR}/cleanup.sh
		echo "rm -rf $BLOCK" >> ${TMP_DIR}/cleanup.sh
		echo "rm -rf $BRICK" >> ${TMP_DIR}/cleanup.sh
		
	 done
	
	echo "rm -rf ${TMP_DIR}" >> ${TMP_DIR}/cleanup.sh
}

createGlusterVolume
createCleanupScript
echo "Cleanup script: ${TMP_DIR}/cleanup.sh"
