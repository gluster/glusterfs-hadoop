package org.gluster.test;

import java.io.File;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * A meta-test: confirms that the configuration sample file has essential parameters.
 */
public class TestConfiguration {

	@Test
	public void testGlusterProperties() throws Throwable{
		try{
			Configuration c = new Configuration(true);
			c.addResource(new File("conf/core-site.xml").getAbsoluteFile().toURI().toURL());
			
			System.out.println(c);
			Assert.assertNotNull(c.get("fs.default.name"));
			Assert.assertNotNull(c.get("fs.glusterfs.volname"));
			System.out.println(c.get("fs.default.name") + " , " + c.get("fs.glusterfs.volname"));
		}
		catch(Throwable t){
			t.printStackTrace();
			System.out.println("Missing glusterconfig.properties file in test root directory.");
			throw t;
		}
	}
}

