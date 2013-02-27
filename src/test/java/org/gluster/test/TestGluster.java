/**
 *
 * Copyright (c) 2011 Gluster, Inc. <http://www.gluster.com>
 * This file is part of GlusterFS.
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 *
 * Base test class for GlusterFS + hadoop testing.  
 * Requires existing/working gluster volume named "hadoop-gluster".
 * 
 * The default volume name can be overridden with env variable gluster-volume
 *
 */


package org.gluster.test;


import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.glusterfs.GlusterFileSystem;
import org.apache.tools.ant.util.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;


/**
 * Unit test for simple Gluster FS + Hadoop shim test.
 * 
 */
public class TestGluster{
	
	protected static File tempDirectory;
	
    protected static GlusterFileSystem gfs;
	private static File temp;
	private static File mount;

	@AfterClass
	public static void after() throws IOException{
		gfs.close();
		FileUtils.delete(tempDirectory);
	}
	
    @BeforeClass
	public static void before() throws Exception{
    	Configuration.addDefaultResource("conf/core-site.xml");
    	Configuration c = new Configuration();
    	
		tempDirectory =  new File(System.getProperty("java.io.tmpdir"), "gluster");

		tempDirectory.mkdirs();
		tempDirectory.delete();
		tempDirectory.mkdir();
		
		
		gfs = new GlusterFileSystem();
        Configuration conf = new Configuration();
        
        Assert.assertNotNull(c.get("fs.glusterfs.volname"));
        Assert.assertNotNull(c.get("fs.default.name"));

        /**
        if(glusterHost==null || "".compareTo(glusterHost)==0){
        	InetAddress addr = null;
            
            addr = InetAddress.getLocalHost();
            
            glusterHost = addr.getHostName();   
        }
        **/
        
        temp = new File(tempDirectory, "hadoop-temp");
    	mount = new File(tempDirectory, "mount");
    	temp.mkdir();
    	mount.mkdir();

        conf.set("fs.glusterfs.mount",mount.getAbsolutePath());
        conf.set("quick.slave.io", "true");
        
        gfs.initialize(temp.toURI(), conf);
	}
	
	@org.junit.Test
	public void testTextWriteAndRead() throws Exception{
	   
        String testString = "Is there anyone out there?";
        String readChars = null;
        
        
        FSDataOutputStream dfsOut = null;
        dfsOut = gfs.create(new Path("test1.txt"));
		dfsOut.writeUTF(testString);
		dfsOut.close();
        
        
        FSDataInputStream dfsin = null;
           
		dfsin = gfs.open(new Path("test1.txt"));
        readChars = dfsin.readUTF();
        dfsin.close();
        
        assertEquals(testString, readChars);
        
        gfs.delete(new Path("test1.txt"), true);
        
        assertFalse(gfs.exists(new Path("test1")));
    }
	@org.junit.Test
	public void testDirs() throws Exception {
	   
        Path subDir1 = new Path("dir.1");
        Path baseDir = new Path("testDirs1");
        // make the dir
        gfs.mkdirs(baseDir);
        assertTrue(gfs.isDirectory(baseDir));
       // gfs.setWorkingDirectory(baseDir);

        gfs.mkdirs(subDir1);
        assertTrue(gfs.isDirectory(subDir1));

        assertFalse(gfs.exists(new Path("test1")));
        assertFalse(gfs.isDirectory(new Path("test/dir.2")));

        
        FileStatus[] p = gfs.listStatus(baseDir);
        assertEquals(p.length, 1);

        gfs.delete(baseDir, true);
        assertFalse(gfs.exists(baseDir));
        
        gfs.delete(subDir1, true);
        assertFalse(gfs.exists(subDir1));
        
    }
	
	@org.junit.Test
	 public void testFiles() throws Exception {
	   
	        Path subDir1 = new Path("dir.1");
	        Path baseDir = new Path("testDirs1");
	        Path file1 = new Path("dir.1/foo.1");
	        Path file2 = new Path("dir.1/foo.2");

	        
	        gfs.mkdirs(baseDir);
	        assertTrue(gfs.isDirectory(baseDir));
	        gfs.setWorkingDirectory(baseDir);

	        gfs.mkdirs(subDir1);

	        FSDataOutputStream s1 = gfs.create(file1, true, 4096, (short) 1, (long) 4096, null);
	        FSDataOutputStream s2 = gfs.create(file2, true, 4096, (short) 1, (long) 4096, null);

	        s1.close();
	        s2.close();

	        FileStatus[] p = gfs.listStatus(subDir1);
	        assertEquals(p.length, 2);

	        gfs.delete(file1, true);
	        p = gfs.listStatus(subDir1);
	        assertEquals(p.length, 1);

	        gfs.delete(file2, true);
	        p = gfs.listStatus(subDir1);
	        assertEquals(p.length, 0);

	        gfs.delete(baseDir, true);
	        assertFalse(gfs.exists(baseDir));
	    }

	 public void testFileIO() throws Exception {
	     
	        Path subDir1 = new Path("dir.1");
	        Path file1 = new Path("dir.1/foo.1");
	        Path baseDir = new Path("testDirs1");
	        
	        gfs.mkdirs(baseDir);
	        assertTrue(gfs.isDirectory(baseDir));
	        gfs.setWorkingDirectory(baseDir);

	        gfs.mkdirs(subDir1);

	        FSDataOutputStream s1 = gfs.create(file1, true, 4096, (short) 1, (long) 4096, null);

	        int bufsz = 4096;
	        byte[] data = new byte[bufsz];

	        for (int i = 0; i < data.length; i++)
	            data[i] = (byte) (i % 16);

	        // write 4 bytes and read them back; read API should return a byte per call
	        s1.write(32);
	        s1.write(32);
	        s1.write(32);
	        s1.write(32);
	        // write some data
	        s1.write(data, 0, data.length);
	        // flush out the changes
	        s1.close();

	        // Read the stuff back and verify it is correct
	        FSDataInputStream s2 = gfs.open(file1, 4096);
	        int v;

	        v = s2.read();
	        assertEquals(v, 32);
	        v = s2.read();
	        assertEquals(v, 32);
	        v = s2.read();
	        assertEquals(v, 32);
	        v = s2.read();
	        assertEquals(v, 32);

	        assertEquals(s2.available(), data.length);

	        byte[] buf = new byte[bufsz];
	        s2.read(buf, 0, buf.length);
	        for (int i = 0; i < data.length; i++)
	            assertEquals(data[i], buf[i]);

	        assertEquals(s2.available(), 0);

	        s2.close();

	        gfs.delete(file1, true);
	        assertFalse(gfs.exists(file1));        
	        gfs.delete(subDir1, true);
	        assertFalse(gfs.exists(subDir1));        
	        gfs.delete(baseDir, true);
	        assertFalse(gfs.exists(baseDir));        
	    }
	
	
	
}
