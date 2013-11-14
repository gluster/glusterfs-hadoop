package org.apache.hadoop.fs.test.connector.glusterfs;

import org.apache.hadoop.conf.Configuration;
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
		return c;
	}

}
