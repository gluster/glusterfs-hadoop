package org.apache.hadoop.fs.test.connector.glusterfs;

import org.apache.hadoop.conf.Configuration;
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
		c.set("fs.default.name","glusterfs:///");
		c.setInt("io.file.buffer.size",GlusterVolume.OVERRIDE_WRITE_BUFFER_SIZE );
		return c;
	}

}
