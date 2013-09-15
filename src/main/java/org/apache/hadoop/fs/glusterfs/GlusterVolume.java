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
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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
                
                //volName=conf.get("fs.glusterfs.volname", null);
                //remoteGFSServer=conf.get("fs.glusterfs.server", null);
                
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }
        
    }
    
    public Path getWorkingDirectory() {
        return new Path(NAME.toString() );
    }
    
    public File pathToFile(Path path) {
        String pathString = path.toUri().getRawPath();
     
        if(pathString.startsWith(Path.SEPARATOR)){
            pathString = pathString.substring(1);
        }
        
        return new File(root + Path.SEPARATOR + pathString);
    }
    
    public Path fileToPath(File path) {
        return new Path(NAME.toString() + path.toURI().getRawPath().substring(root.length()));
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
    

    /**
     * An FS DataInputStream that guards against the underlying exception
     */
   	@Override
   	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
   		return new WrappedFSDataInputStream(super.open(f, bufferSize));
   	}

   	
    public static class WrappedFSDataInputStream extends FSDataInputStream{
		public WrappedFSDataInputStream(FSDataInputStream in) throws IOException {
			super(in);
		}
		long lastPos=0;
		@Override
		public long getPos() throws IOException {
			try{
				//Try to call the underlying get pos, but if it fails,
				//just return the last valid read.  This should be 
				//correct if the underlying call fails, it will be because
				//the stream has been closed (assuming that the last read ended 
				//at the final position in the stream, and no reads occured after that).
				lastPos= ((BufferedFSInputStream)in).getPos();
			}
			catch(Throwable t){
				log.error("Wrapped exception in getPos() : We will return the last valid pos." + t.getMessage());
			}
			return lastPos;
		}
    }

}
