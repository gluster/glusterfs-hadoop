package org.gluster.test;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.glusterfs.GlusterFileSystem;
import org.junit.After;
import org.junit.Before;


/**
 * Unit test for simple App.
 */
public class TestGlusterMount{
	
	protected File tempDirectory;
	protected String glusterVolume="hadoop-gluster";
    protected GlusterFileSystem gfs;

    @Before
	public void before() throws Exception{
		glusterVolume = System.getProperty("gluster-volume");		
		tempDirectory =  File.createTempFile("temp", Long.toString(System.nanoTime()));
		
		if(glusterVolume==null){
			glusterVolume="hadoop-gluster";
		}
		
		tempDirectory.mkdirs();
		tempDirectory.deleteOnExit();
		
        gfs = new GlusterFileSystem();
        Configuration conf = new Configuration();
        
        /* retrieve the local machines hostname */
        InetAddress addr = null;
		try {
			addr = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        String hostname = addr.getHostName();	
        File temp = new File(tempDirectory, "hadoop-temp");
    	File mount = new File(tempDirectory, "mount");
    
    	temp.mkdirs();
		mount.mkdirs();
		temp.delete();
		temp.mkdir();
		mount.delete();
		mount.mkdir();
		
       
        conf.set("fs.glusterfs.volname", glusterVolume);
        conf.set("fs.glusterfs.mount",mount.getAbsolutePath());
        conf.set("fs.glusterfs.server",hostname);
        conf.set("quick.slave.io", "true");
        
        try {
			gfs.initialize(temp.toURI(), conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
	}
	
	@org.junit.Test
	public void testTextWrite(){
        String testString = "Is there anyone out there?";
        String readChars = null;
        
        
        try {
        	FSDataOutputStream dfsOut = null;
        	dfsOut = gfs.create(new Path("test1.txt"));
			dfsOut.writeUTF(testString);
			dfsOut.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        try {
        	FSDataInputStream dfsin = null;
           
			dfsin = gfs.open(new Path("test1.txt"));
        	readChars = dfsin.readUTF();
        	dfsin.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        assertEquals(testString, readChars);
    }

	
	
	
}
