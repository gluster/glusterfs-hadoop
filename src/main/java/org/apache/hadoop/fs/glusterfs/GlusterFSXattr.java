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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    	}
       
    	return xattrValue;
    }
    
	public BlockLocation[] getPathInfo(long start, long len) {
		String xattr = null;
		Pattern blockReg = Pattern.compile("<POSIX\\(.*?\\):(.*?):.*?>");
		try {
			xattr = execGetFattr();
		} catch (IOException e) {
			// problem executing getfattr command, fail gracefully.
		}
		
		Matcher matcher = blockReg.matcher(xattr);
		ArrayList<String> list = new ArrayList<String>();
		while(matcher.find()){
			list.add(matcher.group(1));
		}
		
		/* no pathinfo found*/
		if(list.size() == 0)
			return null;
			
		String hosts[] = list.toArray(new String[list.size()]);
		return new BlockLocation[]{ new BlockLocation(null, hosts, start, len) };

	}
    
}