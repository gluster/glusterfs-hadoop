package org.gluster.test;

import java.net.URL;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * A meta-test: confirms that we correctly loaded gluster properties.
 */
public class TestConfiguration {
	
	@Before
	public void setup(){
		Configuration.addDefaultResource("/conf/core-site.xml");
	}

	@Test
	public void testGlusterProperties() throws Throwable{
		try{
			Configuration c = new Configuration();
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

