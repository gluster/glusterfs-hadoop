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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
	protected static String glusterVolume= System.getProperty("gluster-volume");
	protected static String glusterHost=System.getProperty("gluster-host"); 
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
    	/* the user can over ride the default gluster volume used for test with ENV var */
        glusterVolume= System.getProperty("GLUSTER_VOLUME");
        if(glusterVolume==null || glusterVolume.equals("")){
    		System.out.println("WARNING: HOST NOT DEFINED IN ENVIRONMENT! See README");
        	glusterVolume="HadoopVol";
		}
        
        glusterHost= System.getProperty("GLUSTER_HOST"); 
    	if(glusterHost==null || glusterHost.equals("")){
    		System.out.println("WARNING: HOST NOT DEFINED IN ENVIRONMENT! See README");
    		InetAddress addr = InetAddress.getLocalHost();
    		glusterHost = addr.getHostName();
    	}
        
        System.out.println("Testing against host=" + glusterHost);
        System.out.println("Testing against volume=" + glusterVolume);
        
		tempDirectory =  new File(System.getProperty("java.io.tmpdir"), "gluster");
		tempDirectory.mkdirs();
		tempDirectory.delete();
		tempDirectory.mkdir();
		
		if(glusterVolume==null){
			glusterVolume="hadoop-gluster";
		}
		
		gfs = new GlusterFileSystem();
        
        /* retrieve the local machines hostname */
        if(glusterHost==null || "".compareTo(glusterHost)==0){
            InetAddress addr = null;
            
            addr = InetAddress.getLocalHost();
            
            glusterHost = addr.getHostName();   
        }
        
        System.out.println("Confirmed that configuration properties from gluster were found , now creating dirs");
		
		gfs = new GlusterFileSystem();
        temp = new File(tempDirectory, "hadoop-temp");
    	mount = new File(tempDirectory, "mount");
    	temp.mkdir();
    	mount.mkdir();
 
    	System.out.println("Now initializing GlusterFS !");

    	Configuration conf = new Configuration();
    	conf.set("fs.glusterfs.volname", glusterVolume);
        conf.set("fs.glusterfs.mount", mount.getAbsolutePath());
        conf.set("fs.glusterfs.server", glusterHost);
        conf.set("fs.default.name", "glusterfs://"+glusterHost+":9000");
        conf.set("quick.slave.io", "true");
        System.out.println("server " + conf.get("fs.glusterfs.server"));
        gfs.initialize(temp.toURI(), conf);
	}
    
    
    
    @org.junit.Test
    public void testTolerantMkdirs() throws Exception{
        System.out.println("Testing tollerance of mkdirs(a/b/c/d) then mkdirs(a/b/c)");
        Path longPath = new Path("a/b/c/d");
        assertFalse(gfs.exists(longPath));
        gfs.mkdirs(longPath);
        assertTrue(gfs.exists(longPath));
        gfs.mkdirs(new Path("a"));
        assertTrue(gfs.exists(longPath));
        assertTrue(gfs.exists(new Path("a")));
        gfs.mkdirs(new Path("a/b"));
        assertTrue(gfs.exists(longPath));
        assertTrue(gfs.exists(new Path("a/b")));
        gfs.mkdirs(new Path("a/b/c"));
        assertTrue(gfs.exists(longPath));
        assertTrue(gfs.exists(new Path("a/b/c")));
        
        /* delete the directories */
        
        gfs.delete(longPath);
        assertFalse(gfs.exists(longPath));
       
        
        
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
	public void testZDirs() throws Exception {
        final Path subDir1 = new Path("td_dir.1");
        final Path baseDir = new Path("td_testDirs1");
        final Path test1 = new Path("td_test1");
        final Path test2 = new Path("td_test/dir.2");
        
		System.out.println("Assert that " + baseDir + " doesnt exist yet " + gfs.exists(baseDir));
        assertFalse(gfs.exists(baseDir));
		assertFalse(gfs.isDirectory(baseDir));
        
        // make the dir
        gfs.mkdirs(baseDir);

		System.out.println("Assert that " + baseDir + " exists under gfs");
        assertTrue(gfs.isDirectory(baseDir));
       // gfs.setWorkingDirectory(baseDir);

        gfs.mkdirs(subDir1);

        System.out.println("Assert that subDir1 " + subDir1 + " exists under gfs");
        assertTrue(gfs.isDirectory(subDir1));

        System.out.println("Assert that test1 " + test1 + " exists under gfs");
        assertFalse(gfs.exists(test1));

        System.out.println("Assert that test2 " + test2 + " is file under gfs");
        assertFalse(gfs.isDirectory(test2));

        
        gfs.create(new Path(baseDir,"dummyfile"));
        FileStatus[] p = gfs.listStatus(baseDir);
        System.out.println("Assert that baseDir " + baseDir+ " has 1 file in it " + p.length);
        assertEquals(p.length, 1);

        gfs.delete(baseDir, true);
        System.out.println("Assert that basedir  " +baseDir+" is nonexistent");
        assertFalse(gfs.exists(baseDir));

        gfs.delete(subDir1, true);
        System.out.println("Assert that subDir  " +subDir1+" is nonexistent");
        assertFalse(gfs.exists(subDir1));

        System.out.println("done.");
 
        gfs.delete(baseDir);
        gfs.delete(test1);
        gfs.delete(test2);
	}
	
	@org.junit.Test
	 public void testFiles() throws Exception {
	   
	        Path subDir1 = new Path("tf_dir.1");
	        Path baseDir = new Path("tf_testDirs1");
	        Path file1 = new Path("tf_dir.1/foo.1");
	        Path file2 = new Path("tf_dir.1/foo.2");

	        
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
	        
	        gfs.delete(subDir1);
	        gfs.delete(file1);
	        gfs.delete(file2);
	}

	 public void testFileIO() throws Exception {
	     
	        Path subDir1 = new Path("tfio_dir.1");
	        Path file1 = new Path("tfio_dir.1/foo.1");
	        Path baseDir = new Path("tfio_testDirs1");
	        
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
	        
	        gfs.delete(subDir1);
	        gfs.delete(file1);
	        gfs.delete(baseDir);
	    }
	
	
	
}
