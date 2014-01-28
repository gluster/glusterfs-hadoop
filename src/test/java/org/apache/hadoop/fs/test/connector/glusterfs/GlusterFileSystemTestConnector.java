package org.apache.hadoop.fs.test.connector.glusterfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.glusterfs.GlusterVolume;
import org.apache.hadoop.fs.test.connector.HcfsTestConnector;

/**
 * A HCFS test connector specifically for instantiation and testing Glusterfs.
 */
public class GlusterFileSystemTestConnector extends HcfsTestConnector{

    public Configuration createConfiguration(){
		Configuration c = super.createConfiguration();
		c.set("fs.glusterfs.mount",System.getProperty("GLUSTER_MOUNT"));
		c.set("fs.glusterfs.impl","org.apache.hadoop.fs.local.GlusterFs");
		//dont apply umask, that way permissions tests for mkdir w/ 777 pass.
		c.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY,"000");
		
		//So that sorted implementation of testListStatus passes if it runs.
		//Note that in newer FSMainOperations tests, testListStatus doesnt require.
		c.set("fs.glusterfs.list_status_sorted", "true");
		
		c.set("fs.default.name","glusterfs:///");
		c.setInt("io.file.buffer.size",GlusterVolume.OVERRIDE_WRITE_BUFFER_SIZE );
		return c;
	}

}
