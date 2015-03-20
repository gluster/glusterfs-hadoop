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
 */

package org.apache.hadoop.fs.glusterfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.RegEx;

import org.apache.hadoop.fs.BlockLocation;

public class GlusterFSXattr{

   private String getFattrCmdBase = null;
   private String filename = null;
   private String xattrValue = null;
   
   public GlusterFSXattr(String fileName) {
      this(fileName,  "sudo getfattr -m . -n trusted.glusterfs.pathinfo");
   }
  
   public GlusterFSXattr(String fileName, String getAttr){
	   getFattrCmdBase=getAttr;
	   this.filename = fileName;
   }
    
   public void reset(){
	   xattrValue=null;  
   }
   
   public static String shellToString(String shellCommand) throws IOException{
	 
       Process p=Runtime.getRuntime().exec(shellCommand);
       BufferedReader brInput=null;
       String s=null;
       
       brInput=new BufferedReader(new InputStreamReader(p.getInputStream()));

       String value="";
       while ((s=brInput.readLine())!=null)
       	value+=s;
       
       return value;
   }
   
    /* Caches the xattr value.  Must call reset() to re-query */
    public String execGetFattr() throws IOException{
    	if(xattrValue==null){
	        xattrValue=shellToString(this.getFattrCmdBase + " " + filename);
	        
	        /* strip off 'trusted.glusterfs.pathinfo="  and the last quote*/
	    	xattrValue = xattrValue.substring(28,xattrValue.length()-1);
    	}
       
    	return xattrValue;
    }

    
    
	public BlockLocation[] getPathInfo(long start, long len) {
		String xattr = null;
		//Pattern blockReg = Pattern.compile("<POSIX([^)]*):([^:]*)");
		Pattern blockReg = Pattern.compile("<POSIX(.*):((.*)):");
		ArrayList<BlockLocation> blockLocations = new ArrayList<BlockLocation>();
		try {
			xattr = execGetFattr();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Matcher matcher = blockReg.matcher(xattr);
		ArrayList<String> list = new ArrayList<String>();
		while(matcher.find()){
			String host = matcher.group(2);
			list.add(host);
			/*
			for(int i=0;i<matcher.groupCount();i++)
		    	System.out.println(matcher.group(i));
			*/
		}
		String hosts[] = list.toArray(new String[list.size()]);
		BlockLocation b = new BlockLocation(null, hosts, start, len);
		b.setCachedHosts(hosts);
		blockLocations.add(b);

		return blockLocations.toArray(new BlockLocation[blockLocations.size()]);
	}


    
}