package org.gluster.test;

import java.io.File;

import junit.framework.Assert;

import org.apache.hadoop.fs.glusterfs.GlusterFUSEInputStream;
import org.apache.hadoop.fs.glusterfs.GlusterFUSEOutputStream;
import org.junit.Before;
import org.junit.Test;

public class TestGlusterFuseInputStream{
	String infile ;

	@Before
	public void create() throws Exception{
		//setup: no need for gluster specific path, since its just reading from local path
	    infile=File.createTempFile("TestGlusterFuseInputStream"+System.currentTimeMillis(),"txt").getAbsolutePath();
		final GlusterFUSEOutputStream stream = new GlusterFUSEOutputStream(infile,true);
		stream.write("hello there, certainly, there is some data in this stream".getBytes());
		stream.close();
	}
	
    @Test
    public void testDoubleClose() throws Exception{
    	 //test
         GlusterFUSEInputStream gfi= new GlusterFUSEInputStream (new File(infile), null, "localhost") ; 
         
         //assert that Position is updated (necessary for hbase to function properly)
         gfi.seek(2);
         Assert.assertEquals(2,gfi.getPos());

         gfi.close();
     
         //cleanup
         new File(infile).delete();
    }
}
