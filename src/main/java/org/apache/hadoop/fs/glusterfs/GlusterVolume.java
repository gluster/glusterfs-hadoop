/**
 *
 * Copyright (c) 2011 Red Hat, Inc. <http://www.redhat.com>
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
 * Extends the RawLocalFileSystem to add support for Gluster Volumes. 
 * 
 */

package org.apache.hadoop.fs.glusterfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlusterVolume extends RawLocalFileSystem{


    static final Logger log = LoggerFactory.getLogger(GlusterFileSystemCRC.class);
    public static final URI NAME = URI.create("glusterfs:///");
    
    protected String root=null;
    
    protected static GlusterFSXattr attr = null;
    
    public GlusterVolume(){}
    
    public GlusterVolume(Configuration conf){
        this();
        this.setConf(conf);
    }
    public URI getUri() { return NAME; }
    
    public void setConf(Configuration conf){
        log.info("Initializing gluster volume..");
        super.setConf(conf);
        String getfattrcmd = null;
        if(conf!=null){
         
            try{
                root=conf.get("fs.glusterfs.mount", null);
                getfattrcmd = conf.get("fs.glusterfs.getfattrcmd", null);
                if(getfattrcmd!=null){
                	attr = new GlusterFSXattr(getfattrcmd);
                }else{
                	attr = new GlusterFSXattr();
                }
                String jtSysDir = conf.get("mapreduce.jobtracker.system.dir", null);
                Path mapredSysDirectory = null;
                
                if(jtSysDir!=null)
                    mapredSysDirectory = new Path(jtSysDir);
                else{
                    mapredSysDirectory = new Path(conf.get("mapred.system.dir", "glusterfs:///mapred/system"));
                }
                
                if(!exists(mapredSysDirectory)){
                    mkdirs(mapredSysDirectory);
                }
                
                /* ensure the initial working directory exists */
                Path workingDirectory = getInitialWorkingDirectory();
                mkdirs(workingDirectory);
                
                String buffy = conf.get("io.file.buffer.size", null);
                if(buffy==null || "".compareTo(buffy)==0){
                	conf.set("io.file.buffer.size", Integer.toString(1024 * 128));
                }
                
                //volName=conf.get("fs.glusterfs.volname", null);
                //remoteGFSServer=conf.get("fs.glusterfs.server", null);
                
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }
        
    }
    
    public File pathToFile(Path path) {
      checkPath(path);
      if (!path.isAbsolute()) {
        path = new Path(getWorkingDirectory(), path);
      }
      return new File(root + path.toUri().getPath());
    }
  
    @Override
	protected Path getInitialWorkingDirectory() {
		/* apache's unit tests use a default working direcotry like this: */
       return new Path(this.NAME + "user/" + System.getProperty("user.name"));
        /* The super impl returns the users home directory in unix */
		//return super.getInitialWorkingDirectory();
	}

	public Path fileToPath(File path) {
        return new Path(NAME.toString() + path.toURI().getRawPath().substring(root.length()));
    }

	public boolean rename(Path src, Path dst) throws IOException {
		File dest = pathToFile(dst);
		
		/* two HCFS semantics java.io.File doesn't honor */
		if(dest.exists() && dest.isFile() || !(new File(dest.getParent()).exists())) return false;
		
		if (!dest.exists() && pathToFile(src).renameTo(dest)) {
	      return true;
	    }
	    return FileUtil.copy(this, src, this, dst, true, getConf());
	}
	  /**
	   * Delete the given path to a file or directory.
	   * @param p the path to delete
	   * @param recursive to delete sub-directories
	   * @return true if the file or directory and all its contents were deleted
	   * @throws IOException if p is non-empty and recursive is false 
	   */
	@Override
	public boolean delete(Path p, boolean recursive) throws IOException {
	    File f = pathToFile(p);
	    if(!f.exists()){
	    	/* HCFS semantics expect 'false' if attempted file deletion on non existent file */
	    	return false;
	    }else if (f.isFile()) {
	      return f.delete();
	    } else if (!recursive && f.isDirectory() && 
	        (FileUtil.listFiles(f).length != 0)) {
	      throw new IOException("Directory " + f.toString() + " is not empty");
	    }
	    return FileUtil.fullyDelete(f);
	}
	  
    public FileStatus[] listStatus(Path f) throws IOException {
        File localf = pathToFile(f);
        FileStatus[] results;

        if (!localf.exists()) {
          throw new FileNotFoundException("File " + f + " does not exist");
        }
        if (localf.isFile()) {
          return new FileStatus[] {
            new GlusterFileStatus(localf, getDefaultBlockSize(), this) };
        }

        File[] names = localf.listFiles();
        if (names == null) {
          return null;
        }
        results = new FileStatus[names.length];
        int j = 0;
        for (int i = 0; i < names.length; i++) {
          try {
            results[j] = getFileStatus(fileToPath(names[i]));
            j++;
          } catch (FileNotFoundException e) {
            // ignore the files not found since the dir list may have have changed
            // since the names[] list was generated.
          }
        }
        if (j == names.length) {
          return results;
        }
        return Arrays.copyOf(results, j);
    }
    
    public FileStatus getFileStatus(Path f) throws IOException {
        File path = pathToFile(f);
        if (path.exists()) {
          return new GlusterFileStatus(pathToFile(f), getDefaultBlockSize(), this);
        } else {
          throw new FileNotFoundException( "File " + f + " does not exist.");
        }
      }
    
    public long getBlockSize(Path path) throws IOException{
        long blkSz;
        File f=pathToFile(path);

        blkSz=attr.getBlockSize(f.getPath());
        if(blkSz==0)
            blkSz=getLength(path);

        return blkSz;
    }
   
    public BlockLocation[] getFileBlockLocations(FileStatus file,long start,long len) throws IOException{
        File f=pathToFile(file.getPath());
        BlockLocation[] result=null;

        result=attr.getPathInfo(f.getPath(), start, len);
        if(result==null){
            log.info("Problem getting destination host for file "+f.getPath());
            return null;
        }

        return result;
    }
    
    public String toString(){
        return "Gluster Volume mounted at: " + root;
    }

}
