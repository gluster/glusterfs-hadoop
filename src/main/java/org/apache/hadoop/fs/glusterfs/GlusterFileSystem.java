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
 * files on GlusterFS and run Map/Reduce jobs on the data.
 * 
 * gluster file systems are specified with the glusterfs:// prefix.
 * 
 * 
 */
package org.apache.hadoop.fs.glusterfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GlusterFileSystem extends LocalFileSystem{
    
    protected static final Logger log = LoggerFactory.getLogger(GlusterFileSystem.class);
    
    public GlusterFileSystem(){
        super(new GlusterVolume());
    }
    
    public void setConf(Configuration conf){
        log.info("Initializing GlusterFS");
        super.setConf(conf);
    }

    /*
     * if GlusterFileSystem is the default filesystem, real local URLs come back without a file:/ scheme name (BUG!). the glusterfs
     * file system is assumed.  force a schema. 
     */
    @Override
    public void copyFromLocalFile(boolean delSrc, Path src, Path dst)  throws IOException {
        FileSystem srcFs = new Path("file:/" + src.toString()).getFileSystem(getConf());
        FileSystem dstFs = dst.getFileSystem(getConf());
        FileUtil.copy(srcFs, src, dstFs, dst, delSrc, getConf());
    }

    @Override
    public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
      FileSystem srcFs = src.getFileSystem(getConf());
      FileSystem dstFs = new Path("file:/" + dst.toString()).getFileSystem(getConf());
      FileUtil.copy(srcFs, src, dstFs, dst, delSrc, getConf());
    }
    
    public String toString(){
        return "Gluster File System";
    }
}
