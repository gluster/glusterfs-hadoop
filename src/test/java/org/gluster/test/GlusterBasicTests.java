package org.gluster.test;


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


/**
 * Unit test for simple App.
 */
public class GlusterBasicTests{
    
    public void tesMount() throws Exception{
        GlusterFileSystem gfs = new GlusterFileSystem();
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
        File temp = null;
    	File mount = null;
        try {
			temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
			temp.mkdirs();
			mount = File.createTempFile("temp", "mount");
			mount.mkdirs();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        conf.set("fs.glusterfs.volname", "hadooptest");
        conf.set("fs.glusterfs.mount",mount.getAbsolutePath());
        conf.set("fs.glusterfs.server",hostname);
        conf.set("quick.slave.io", "true");
        
        try {
			gfs.initialize(temp.toURI(), conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        String testString = "Is there anyone out there?";
        try {
        	FSDataOutputStream dfsOut = null;
        	dfsOut = gfs.create(new Path("test1.txt"));
			dfsOut.writeUTF(testString);
			dfsOut.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        String readChars = null;
        try {
        	FSDataInputStream dfsin = null;
            gfs.initialize(temp.toURI(), conf);
			dfsin = gfs.open(new Path("test1.txt"));
        	readChars = dfsin.readUTF();
        	dfsin.close();
        } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
       
        
        
        
    }
}
