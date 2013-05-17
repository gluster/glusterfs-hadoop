/**
 *
 * Copyright (c) 2011 Red Hat Inc <http://www.redhat.com>
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
 * Hard-core debugging shim which uses the GlusterFileSystem super class for the heavy lifting, but logs every call
 * to /tmp/glusterfs-N.log  
 * 
 * This Shim can be interchanged with the GlusterFileSystem to debug calls.
 * 
 * Each JVM instance of GlusterDebugFileSystem will generate a new log, incremented on what's already there.
 * 
 * To use, specify this class as your file system in hadoop:
 * 
 * <property>
 *    <name>fs.glusterfs.impl</name>
 *   <value>org.apache.hadoop.fs.glusterfs.GlusterDebugFileSystem</value>
 * </property>
 * 
 * Authoer: bchilds@redhat.com
 * 
 *
 */

package org.apache.hadoop.fs.glusterfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class GlusterDebugFileSystem extends GlusterFileSystem{

    private static File logFile=null;
    private static final String LOG_PREFIX="/tmp/glusterfs";
    private static boolean showStackTrace = true;
    
    public static synchronized String getStackTrace(int stripTopElements){
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        String traceString = "";
        // remove the specified top elements of the stack trace (to avoid debug methods in the trace). the +1 is for this methods call.
        for(int i=stripTopElements+1;i<trace.length;i++){
            traceString += "\t[" + trace[i].getFileName() + "] " +  trace[i].getClassName() +  "." + trace[i].getMethodName() +  "() line:" + trace[i].getLineNumber() + "\n";
        }
        
        return traceString;
    }
    
    public static synchronized void logMachine(String text){

        DateFormat dateFormat=new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date=new Date();
        if(logFile==null){
            for(int i=0;i<1000000;i++){
                logFile=new File(LOG_PREFIX+"-"+i+".log");
                if(!logFile.exists()){
                    try{
                        logFile.createNewFile();
                        break;
                    }catch (IOException e){
                    }
                }
            }
        }

        try{
            PrintWriter out=new PrintWriter(new BufferedWriter(new FileWriter(logFile, true)));
            out.write("(" + dateFormat.format(date)+") : "+text+"\n");
            String stackTrace = GlusterDebugFileSystem.getStackTrace(3);
            if(showStackTrace) out.write(stackTrace);
            out.close();
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }

    }
    public void initialize(URI uri,Configuration conf) throws IOException{
        GlusterDebugFileSystem.logMachine("Init of the GlusterDebugFileSystem....");
        GlusterDebugFileSystem.logMachine("init(" + uri + "," + conf);
        super.initialize(uri,conf);
    }
    
    public URI getUri(){
        GlusterDebugFileSystem.logMachine("getUri()");
        return super.getUri();
    }

    public String getName(){
        GlusterDebugFileSystem.logMachine("getName()");
        return super.getName();
    }

    public Path getWorkingDirectory(){
        GlusterDebugFileSystem.logMachine("getWorkingDirectory()");
        return super.getWorkingDirectory();
    }

    public Path getHomeDirectory(){
        GlusterDebugFileSystem.logMachine("getHomeDirectory()");
        return super.getHomeDirectory();
    }

    public void setWorkingDirectory(Path dir){
        GlusterDebugFileSystem.logMachine("setWorkingDirectory("+dir+")");
        super.setWorkingDirectory(dir);
    }

    public boolean exists(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("exists("+path+")");
        return super.exists(path);
    }

    public boolean mkdirs(Path path,FsPermission permission) throws IOException{
        GlusterDebugFileSystem.logMachine("mkdirs("+path+","+permission+")");
        return super.mkdirs(path, permission);
    }

    @Deprecated
    public boolean isDirectory(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("isDirectory("+path+")");
        return super.isDirectory(path);
    }

    public boolean isFile(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("isFile("+path+")");
        return super.isFile(path);
    }

    public Path[] listPaths(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("listPaths("+path+")");
        return super.listPaths(path);
    }

    public FileStatus[] listStatus(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("listStatus("+path+")");
        return super.listStatus(path);

    }

    public FileStatus getFileStatusFromFileString(String path) throws IOException{
        GlusterDebugFileSystem.logMachine("getFileStatusFromFileString("+path+")");
        return super.getFileStatusFromFileString(path);
    }

    public void setPermission(Path p,FsPermission permission){
        GlusterDebugFileSystem.logMachine("setPermission("+p+","+permission+")");
        super.setPermission(p, permission);
    }

    public FileStatus getFileStatus(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("getFileStatus("+path+")");
        return super.getFileStatus(path);
    }

    public FSDataOutputStream create(Path path,FsPermission permission,boolean overwrite,int bufferSize,short replication,long blockSize,Progressable progress) throws IOException{
        GlusterDebugFileSystem.logMachine("create("+path+","+permission+","+(overwrite ? "true" : "false")+","+bufferSize+","+replication+","+blockSize+",progress"+"):");
        return super.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

    public FSDataInputStream open(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("open("+path+")");
        return super.open(path);
    }

    public FSDataInputStream open(Path path,int bufferSize) throws IOException{
        GlusterDebugFileSystem.logMachine("open("+path+","+bufferSize+")");
        return super.open(path, bufferSize);
    }

    public FSDataOutputStream append(Path f,int bufferSize,Progressable progress) throws IOException{
        GlusterDebugFileSystem.logMachine("append("+f+","+bufferSize+",progress)");
        return super.append(f, bufferSize, progress);
    }

    public boolean rename(Path src,Path dst) throws IOException{
        GlusterDebugFileSystem.logMachine("rename("+src+","+dst+")");
        return super.rename(src, dst);
    }

    public boolean delete(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("delete("+path+")");
        return super.delete(path);
    }

    public boolean delete(Path path,boolean recursive) throws IOException{
        GlusterDebugFileSystem.logMachine("delete("+path+(recursive ? ",true" : ",false")+"): ");
        return super.delete(path, recursive);
    }

    @Deprecated
    public long getLength(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("getLength("+path+") ");
        return super.getLength(path);
    }

    public short getDefaultReplication(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("getDefaultReplication("+path+") ");
        return super.getDefaultReplication(path);
    }

    public long getBlockSize(Path path) throws IOException{
        GlusterDebugFileSystem.logMachine("getBlockSize("+path+") ");
        return super.getBlockSize(path);
    }

    public long getDefaultBlockSize(){
        GlusterDebugFileSystem.logMachine("getDefaultBlockSize() ");
        return super.getDefaultBlockSize();
    }

    public BlockLocation[] getFileBlockLocations(FileStatus file,long start,long len) throws IOException{
        GlusterDebugFileSystem.logMachine("getFileBlockLocations("+file+","+start+","+len+")");
        return super.getFileBlockLocations(file, start, len);
    }

    public void setOwner(Path p,String username,String groupname) throws IOException{
        GlusterDebugFileSystem.logMachine("setOwner("+p+","+username+","+groupname+")");
        super.setOwner(p, username, groupname);
    }

    public void copyFromLocalFile(boolean delSrc,Path src,Path dst) throws IOException{
        GlusterDebugFileSystem.logMachine("copyFromLocalFile("+delSrc+","+src+","+dst+")");
        super.copyFromLocalFile(delSrc, src, dst);
    }

    public void copyToLocalFile(boolean delSrc,Path src,Path dst) throws IOException{
        GlusterDebugFileSystem.logMachine("copyFromLocalFile("+delSrc+","+src+","+dst+")");
        super.copyToLocalFile(delSrc, src, dst);
    }

    public Path startLocalOutput(Path fsOutputFile,Path tmpLocalFile) throws IOException{
        GlusterDebugFileSystem.logMachine("startLocalOutput("+fsOutputFile+","+tmpLocalFile+")");
        return super.startLocalOutput(fsOutputFile, tmpLocalFile);
    }

    public void completeLocalOutput(Path fsOutputFile,Path tmpLocalFile) throws IOException{
        GlusterDebugFileSystem.logMachine("completeLocalOutput("+fsOutputFile+","+tmpLocalFile+")");
        super.completeLocalOutput(fsOutputFile, tmpLocalFile);
    }

}
