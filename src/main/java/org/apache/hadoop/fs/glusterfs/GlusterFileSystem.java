/**
 *
 * Copyright (c) 2013 Red Hat, Inc. <http://www.redhat.com>
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

/**
 * Implements the Hadoop FileSystem Interface to allow applications to store
 * files on GlusterFS and run Map/Reduce jobs on the data.  This code does NOT perform a CRC 
 * on files.
 * 
 * gluster file systems are specified with the glusterfs:// prefix.
 * 
 * 
 */
package org.apache.hadoop.fs.glusterfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlusterFileSystem extends FilterFileSystem{

    protected static final Logger log=LoggerFactory.getLogger(GlusterFileSystem.class);
   
    public GlusterFileSystem(){
        super(new GlusterVolume());
        Version v=new Version();
        log.info("Initializing GlusterFS,  CRC disabled.");
        log.info("GIT INFO="+v);
        log.info("GIT_TAG="+v.getTag());
    }

    /** Convert a path to a File. */
    public File pathToFile(Path path){
        return ((GlusterVolume) fs).pathToFile(path);
    }

    /**
     * Get file status.
     */
    public boolean exists(Path f) throws IOException{
        File path=pathToFile(f);
        if(path.exists()){
            return true;
        }else{
            return false;
        }
	}

	public FSDataOutputStream createNonRecursive(Path file,
			FsPermission permission, boolean overwrite, int bufferSize,
			short replication, long blockSize, Progressable progress)
			throws IOException {
		Path parent = file.getParent();
		if (parent == null || exists(parent)) {
			return create(file, permission, overwrite, bufferSize, replication,
					blockSize, progress);
		} else {
			throw new IOException("Parent " + parent + " does not exist");
		}
	}
	
    public void setConf(Configuration conf){
        log.info("Configuring GlusterFS");
        if(conf!=null) conf.addResource("glusterfs-site.xml");
        super.setConf(conf);
    }

    /*
     * if GlusterFileSystem is the default filesystem, real local URLs come back
     * without a file:/ scheme name (BUG!). the glusterfs file system is
     * assumed. force a schema.
     */

    public void copyFromLocalFile(boolean delSrc,Path src,Path dst) throws IOException{
        FileSystem srcFs=new Path("file:/"+src.toString()).getFileSystem(getConf());
        FileSystem dstFs=dst.getFileSystem(getConf());
        FileUtil.copy(srcFs, src, dstFs, dst, delSrc, getConf());
    }

    public void copyToLocalFile(boolean delSrc,Path src,Path dst) throws IOException{
        FileSystem srcFs=src.getFileSystem(getConf());
        FileSystem dstFs=new Path("file:/"+dst.toString()).getFileSystem(getConf());
        FileUtil.copy(srcFs, src, dstFs, dst, delSrc, getConf());
    }

    @Override
    public boolean mkdirs(Path f) throws IOException {
        return mkdirs(f, FsPermission.getDirDefault().applyUMask(FsPermission.getUMask(getConf())));
    }

    public String toString(){
        return "Gluster File System, no CRC.";
    }
}
