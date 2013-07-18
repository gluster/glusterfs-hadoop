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
 * Implements the Hadoop FileSystem Interface to allow applications to store
 * files on GlusterFS and run Map/Reduce jobs on the data.
 * 
 * 
 */

package org.apache.hadoop.fs.glusterfs;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
/*
 * Copied from org.apache.fs.RawLocalFileSystem.RawFileStatus
 */
public class GlusterFileStatus extends FileStatus{
    /*
     * We can add extra fields here. It breaks at least CopyFiles.FilePair(). We
     * recognize if the information is already loaded by check if
     * onwer.equals("").
     */
    protected GlusterVolume fs;

    private boolean isPermissionLoaded(){
        return !super.getOwner().equals("");
    }

    
    /*
     * This constructor is the only difference than the RawLocalFileStatus impl. RawLocalFileSystem converts a raw file path to path with the same prefix. ends up with a double /mnt/glusterfs.
     */
    GlusterFileStatus(File f, long defaultBlockSize, GlusterVolume fs){
        super(f.length(), f.isDirectory(), 1, defaultBlockSize, f.lastModified(), fs.fileToPath(f));
        this.fs=fs;
    }

    @Override
    public FsPermission getPermission(){
        if(!isPermissionLoaded()){
            loadPermissionInfo();
        }
        return super.getPermission();
    }

    @Override
    public String getOwner(){
        if(!isPermissionLoaded()){
            loadPermissionInfo();
        }
        return super.getOwner();
    }

    @Override
    public String getGroup(){
        if(!isPermissionLoaded()){
            loadPermissionInfo();
        }
        return super.getGroup();
    }

    // / loads permissions, owner, and group from `ls -ld`
    private void loadPermissionInfo(){
        IOException e=null;
        try{
            StringTokenizer t=new StringTokenizer(Util.execCommand(fs.pathToFile(getPath()), Shell.getGET_PERMISSION_COMMAND()));
            // expected format
            // -rw------- 1 username groupname ...
            String permission=t.nextToken();
            if(permission.length()>10){ // files with ACLs might have a '+'
                permission=permission.substring(0, 10);
            }
            setPermission(FsPermission.valueOf(permission));
            t.nextToken();
            setOwner(t.nextToken());
            setGroup(t.nextToken());
        }catch (Shell.ExitCodeException ioe){
            if(ioe.getExitCode()!=1){
                e=ioe;
            }else{
                setPermission(null);
                setOwner(null);
                setGroup(null);
            }
        }catch (IOException ioe){
            e=ioe;
        }finally{
            if(e!=null){
                throw new RuntimeException("Error while running command to get "+"file permissions : "+StringUtils.stringifyException(e));
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException{
        if(!isPermissionLoaded()){
            loadPermissionInfo();
        }
        super.write(out);
    }

}
