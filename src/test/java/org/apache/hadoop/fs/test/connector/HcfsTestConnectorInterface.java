package org.apache.hadoop.fs.test.connector;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;


/* generic interface for creating HCFS file sytems for testing purposes */

public interface HcfsTestConnectorInterface {
	
	/* return a fully configured instantiated file system for testing */
	public  FileSystem create() throws IOException;
	
	/* returns a configuration file with properties for a given FS */
	public Configuration createConfiguration();

}
