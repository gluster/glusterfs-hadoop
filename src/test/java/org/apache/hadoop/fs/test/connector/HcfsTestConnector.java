package org.apache.hadoop.fs.test.connector;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/*
 * Generic HCFS file system test connector.
 * This test connector takes a fully qualified o.a.h.f.FileSystem implementor class 
 * as an environment variable.
 * 
 */
public class HcfsTestConnector implements HcfsTestConnectorInterface{
	 
	public Configuration createConfiguration(){
		return new Configuration();
	}
	   
    public FileSystem create(String HcfsClassName) throws IOException{
    	try {
    		FileSystem hcfs = (FileSystem)Class.forName(HcfsClassName).newInstance();
			hcfs.initialize(hcfs.getUri(), createConfiguration());
			return hcfs;
		} catch (Exception e) {
			throw new RuntimeException("Cannont instatiate HCFS. Error:\n " + e);
		} 
	}

	public FileSystem create() throws IOException {
		return create(System.getProperty("HCFS_CLASSNAME"));
	}
}
