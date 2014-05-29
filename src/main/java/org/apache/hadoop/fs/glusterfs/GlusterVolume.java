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
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlusterVolume extends RawLocalFileSystem{

    static final Logger log = LoggerFactory.getLogger(GlusterVolume.class);

    /**
     * General reason for these constants is to help us decide
     * when to override the specified buffer size.  See implementation 
     * of logic below, which might change overtime.
     */
    public static final int OVERRIDE_WRITE_BUFFER_SIZE = 1024 * 4;
    public static final int OPTIMAL_WRITE_BUFFER_SIZE = 1024 * 128;
    public static final int DEFAULT_BLOCK_SIZE = 64 * 1024 * 1024;
    
    protected URI NAME = null;
 
    protected Hashtable<String,String> volumes=new Hashtable<String,String>();
    protected String default_volume = null;
    
    
    protected static GlusterFSXattr attr = null;
    
    public GlusterVolume(){}
    
    public GlusterVolume(Configuration conf){
        this();
        this.setConf(conf);
    }
    
    public URI getUri() { return NAME; }
    
    public void initialize(URI uri, Configuration conf) throws IOException {
        /* we only really care about the URI up to the path, so strip other things off */
        String auth = uri.getAuthority();
        if(auth==null)
            auth = "";
        this.NAME = URI.create(uri.getScheme() + "://" + auth + "/") ;
        super.initialize(this.NAME, conf);
    }
    
    /*
     *  This expands URIs to include 'default' values.  
     *  For instance, glusterfs:///path would expand to glusterfs://defaultvolume/path
     */
    protected URI canonicalizeUri(URI uri) {
        String auth = uri.getAuthority();
        if(auth==null)
            auth = default_volume;
        return URI.create(uri.getScheme() + "://" + auth + "/" + uri.getPath()) ;
    }
    
    /* check if a path is on the same volume as this instance */
    public boolean sameVolume(Path p){
        URI thisUri = this.NAME;
        URI thatUri = p.toUri();
        if(!thatUri.getScheme().equalsIgnoreCase(thisUri.getScheme())) return false;
        if((thatUri.getAuthority()==null && thisUri.getAuthority()==null)) return true;
        return (thatUri.getAuthority()!=null && thatUri.getAuthority().equalsIgnoreCase(thisUri.getAuthority()));
        
    }
    
    public void setConf(Configuration conf){
        log.info("Initializing gluster volume..");
        super.setConf(conf);
        String getfattrcmd = null;
        if(conf!=null){
         
            try{
                String[] v=conf.get("fs.glusterfs.volumes", "").split(",");
                default_volume = v[0];
                for(int i=0;i<v.length;i++){
                    String vol = conf.get("fs.glusterfs.volume.fuse." + v[i] , null);
                    
                    if(vol==null){
                        log.error("Could not find property: fs.glusterfs.fuse." + v[i]);
                        throw new RuntimeException("Could not find mount point for volume: "+ v[i]);
                    }
                    volumes.put(v[i],vol);
                    log.info("Gluster volume: " + v[i] + " at : " + volumes.get(v[i]));
                }
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
                
                if(sameVolume(mapredSysDirectory) && !exists(mapredSysDirectory) ){
                    mkdirs(mapredSysDirectory);
                }
                //Working directory setup
                
                Path workingDirectory = getInitialWorkingDirectory();
                if(!sameVolume(workingDirectory)){
                    workingDirectory = new Path("/");
                }else if( !exists(workingDirectory)){
                    mkdirs(workingDirectory);
                }
                setWorkingDirectory(workingDirectory);
                
                
                log.info("Working directory is : "+ getWorkingDirectory());

                /**
                 * Write Buffering
                 */
                Integer userBufferSize=conf.getInt("io.file.buffer.size", -1);
                if(userBufferSize == OVERRIDE_WRITE_BUFFER_SIZE || userBufferSize == -1) {
                	conf.setInt("io.file.buffer.size", OPTIMAL_WRITE_BUFFER_SIZE);
                }
                log.info("Write buffer size : " +conf.getInt("io.file.buffer.size",-1)) ;
                
                /**
                 * Default block size
                 */
                
                Long defaultBlockSize=conf.getLong("fs.local.block.size", -1);
                
                if(defaultBlockSize == -1) {
                    conf.setInt("fs.local.block.size", DEFAULT_BLOCK_SIZE);
                }
                log.info("Default block size : " +conf.getInt("fs.local.block.size",-1)) ;
                
            }
            catch (Exception e){
                throw new RuntimeException(e);
            }
        }
        
    }
    
    public File pathToFile(Path path) {
      
      if(path==null) return null;
      
      checkPath(path);
      
      if (!path.isAbsolute()) {
        path = new Path(getWorkingDirectory(), path);
      }
      
      String volume = path.toUri().getAuthority();
      String scheme = path.toUri().getScheme();
      
      if(scheme==null || "".equals(scheme)){
         return pathToFile(path.makeQualified(this));
        
      }else if(volume==null){
          volume = default_volume;
      }

      return new File(this.volumes.get(volume) + "/" + path.toUri().getPath());
    }
    
    protected Path getInitialWorkingDirectory() {
		/* initial home directory is always on the default volume */
       return new Path("glusterfs:///user/" + System.getProperty("user.name"));
	}
    
	public Path fileToPath(File path) {
	    Enumeration<String> all = volumes.keys();
	    String rawPath = path.getAbsolutePath();
	    
	    String volume = null;
	    String root = null;
	    
	    while(volume==null && all.hasMoreElements()){
	        String nextVolume = all.nextElement();
	        String nextPath = volumes.get(nextVolume);
	        if(rawPath.startsWith(nextPath)){
	            volume = nextVolume;
	            root = nextPath;
	        }
	    }
	    
	    if(default_volume.equalsIgnoreCase(volume))
	        volume = "";
	    return new Path("glusterfs://" + volume + "/" + rawPath.substring(root.length()));
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
	public boolean mkdirs(Path f) throws IOException {
	      if(f == null) {
	        throw new IllegalArgumentException("mkdirs path arg is null");
	      }
	      
	      f = f.makeQualified(this);
	      
	      return super.mkdirs(f);
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
        	  log.info("ignoring invisible path :  " + names[i]);
          }
        }
        if (j == names.length) {
          return results;
        }
        return Arrays.copyOf(results, j);
    }
    
    public FileStatus getFileStatus(Path f) throws IOException {
        
        File path = null;
        
        try{
               path =  pathToFile(f);
        }catch(IllegalArgumentException ex){
            throw new FileNotFoundException( "File " + f + " does not exist on this volume." + ex);
        }
        
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
    
    public void setOwner(Path p, String username, String groupname)
            throws IOException {
    	super.setOwner(p,username,groupname);
    	
    }
    
    public void setPermission(Path p, FsPermission permission)
            throws IOException {
    	super.setPermission(p,permission);
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
        return "Gluster volume: " + this.NAME;
    }

}
