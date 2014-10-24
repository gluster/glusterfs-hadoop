package org.apache.hadoop.fs.test;

import java.io.IOException;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.glusterfs.*;
/**
 *  This class is for tests which are gluster specific.
 */ 
public class GlusterVolumeTest {
   
    Logger log = LoggerFactory.getLogger(GlusterVolumeTest.class);
    static Configuration  config = null;
 
    @org.junit.Test
    public void testNullURI() { 
	Assert.assertNotNull(new org.apache.hadoop.fs.glusterfs.GlusterVolume().getUriOrCreate(null));
    }
    
}
