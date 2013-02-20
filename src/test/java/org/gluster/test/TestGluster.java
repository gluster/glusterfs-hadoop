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
import org.apache.tools.ant.util.FileUtils;
import org.junit.After;
import org.junit.Before;


/**
 * Unit test for simple Gluster FS + Hadoop shim test.
 * 
 */
public class TestGluster{
	
	protected File tempDirectory;
	protected String glusterVolume="hadoop-gluster";
    protected GlusterFileSystem gfs;
	private File temp;
	private File mount;

	@After
	public void after(){
		FileUtils.delete(tempDirectory);
	}
	
    @Before
	public void before() throws Exception{
    	/* the user can over ride the default gluster volume used for test with ENV var */
		glusterVolume = System.getProperty("gluster-volume");		
		tempDirectory =  new File(System.getProperty("java.io.tmpdir"), "gluster");

		tempDirectory.mkdirs();
		tempDirectory.delete();
		tempDirectory.mkdir();
		
		if(glusterVolume==null){
			glusterVolume="hadoop-gluster";
		}
		
		
		
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
        this.temp = new File(tempDirectory, "hadoop-temp");
    	this.mount = new File(tempDirectory, "mount");
    	temp.mkdir();
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
	public void testTextWriteAndRead(){
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
