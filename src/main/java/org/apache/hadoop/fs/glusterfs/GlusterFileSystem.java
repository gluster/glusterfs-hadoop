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

/**
 * Implements the Hadoop FileSystem Interface to allow applications to store
 * files on GlusterFS and run Map/Reduce jobs on the data.
 */

package org.apache.hadoop.fs.glusterfs;

import java.io.*;
import java.net.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.util.TreeMap;

import java.nio.channels.FileLock;



/*
 * This package provides interface for hadoop jobs (incl. Map/Reduce)
 * to access files in GlusterFS backed file system via FUSE mount
 * @TODO: Evaluate LocalFileSystem and RawLocalFileSystem as possible delegate file systems to remove & refactor this code.





 * 
 */
public class GlusterFileSystem extends FileSystem {

	private FileSystem glusterFs = null;
	private URI uri = null;
	private Path workingDir = null;
	private String glusterMount = null;
	private boolean mounted = false;

	/* for quick IO */

	private boolean quickSlaveIO = false;

	/* extended attribute class */
	private GlusterFSXattr xattr = null;
	/* hostname of this machine */
	private static String hostname;
	private RandomAccessFile fuseFileLock;
	public GlusterFileSystem() {

	}

	public URI getUri() {
		return uri;
	}

	public boolean FUSEMount(String volname, String server, String mount)
			throws IOException, InterruptedException {
		boolean ret = true;
		int retVal = 0;
		Process p = null;
	


		final String mountCmd = "mount -t glusterfs " + server + ":" + "/" + volname + " "+ mount;
		System.out.println("Running: " + mountCmd);

		try {
			p = Runtime.getRuntime().exec(mountCmd);
			retVal = p.waitFor();
			if (retVal != 0)
				ret = false;
		}
		catch (IOException e) {
			e.printStackTrace();
			System.out.println("Error calling mount, Continuing.., hopefully its already been mounted.");
		}
		return ret;
	}
	/** Initialization is done when we first load a new FileSystem **/
	public void initialize(URI uri, Configuration conf) throws IOException {
		boolean ret = false;
		String volName = null;
		String remoteGFSServer = null, needQuickRead = null;
		boolean autoMount = true;
		File fuseFile;

		if (this.mounted)
			return;

		System.out.println("Initializing GlusterFS");

		try {
			volName = conf.get("fs.glusterfs.volname", null);
			glusterMount = conf.get("fs.glusterfs.mount", null);
			remoteGFSServer = conf.get("fs.glusterfs.server", null);
			needQuickRead = conf.get("quick.slave.io", null);
			autoMount = conf.getBoolean("fs.glusterfs.automount", true);

	


			if ((volName.length() == 0) || (remoteGFSServer.length() == 0) 
					|| (glusterMount.length() == 0)){
				throw new RuntimeException(
						"Not enough info to mount FUSE: volname="+volName 
								+ " glustermount=" + glusterMount);
			}

	
	
	


			        if (autoMount) {
			        	ret = FUSEMount(volName, remoteGFSServer, glusterMount);
			        	if (!ret) {
			        		throw new RuntimeException("Initialize: Failed to mount GlusterFS ");
			        	}
			        }
			        if((needQuickRead.length() != 0)
			        		&& (needQuickRead.equalsIgnoreCase("yes") 
							|| needQuickRead.equalsIgnoreCase("on") || needQuickRead
								.equals("1"))){
				this.quickSlaveIO = true;
	}
	fuseFile = new File("/var/tmp/" + glusterMount.replace('/', '_'));
	/* create the file if it doesn't exist.  we're going to lock on access to the file, not creation */
	if(!fuseFile.exists()) fuseFile.createNewFile();
		fuseFile.deleteOnExit();
	/* create the channel for the lock on the file */
	fuseFileLock = new RandomAccessFile(fuseFile,"rw");
	this.mounted = true;
	this.glusterFs = FileSystem.getLocal(conf);
	this.workingDir = new Path(glusterMount);
	this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());

	this.xattr = new GlusterFSXattr();
	InetAddress addr = InetAddress.getLocalHost();
	GlusterFileSystem.hostname = addr.getHostName();
	setConf(conf);
	}catch (Exception e) {
	throw new RuntimeException("Unable to initialize GlusterFS " + e.getMessage());
	}
	}

	
	
	@Deprecated
	public String getName() {
		return getUri().toString();
	}

	public Path getWorkingDirectory() {
		return this.workingDir;
	}

	public Path getHomeDirectory() {
		return this.workingDir;
	}

	/** 
	 * Localizes a DFS path.
	 * 
	 * @param path a DFS path to localize. 
	 * @return a system specific location of the file. 
	 * 
	 */
	public Path makeAbsolute(Path path) {
		String pth = path.toUri().getPath();
		if (pth.startsWith(workingDir.toUri().getPath())) {
			return path;
		}

		return new Path(workingDir + "/" + pth);
	}

	public void setWorkingDirectory(Path dir) {
		this.workingDir = makeAbsolute(dir);
	}

	/** 
	 * Checks existence of a path or file in the DFS.
	 * 
	 * @param path a DFS path
	 * @return if the directory or file exists or not.
	 */
	public boolean exists(Path path) throws IOException {
		FileLock fuseLock=null;
		synchronized(fuseFileLock){
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	Path absolute = makeAbsolute(path);
	File f = new File(absolute.toUri().getPath());

	return f.exists();
	}finally{
	fuseLock.release();
	}
	}
	}

	/** 
	 * Idempotent directory creation. While not atomic, this call is tolerant of 
	 * partial directory existence. 
	 * 
	 * @param path DFS Path to create.
	 * @param permission permissions for the new path (broken ATM.  paths get the permisions of current process).
	 * @return success or failure.
	 * 
	 */
	public boolean mkdirs(Path path, FsPermission permission) throws IOException {
	/* had to unspin recursion for the locking */
	String split[] = path.toString().split(Path.SEPARATOR);
	String current = "";
	boolean success = true;
	FileLock fuseLock=null;
	synchronized(fuseFileLock){
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	for(int i=0;i<split.length && success;i++){
	current += split[i] + Path.SEPARATOR;
	Path absolute = makeAbsolute(new Path(current));
	File p2f = new File(absolute.toUri().getPath());
	p2f.mkdirs();
	success = p2f.exists();
	}
	}finally{
	fuseLock.release();  
	}
	}
	   
	return success;
	}

	@Deprecated
	public boolean isDirectory(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
	FileLock fuseLock=null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	File f = new File(absolute.toUri().getPath());
	return f.isDirectory();
	}finally{
	fuseLock.release();  
	}
	}
	}

	public boolean isFile(Path path) throws IOException {
		return !isDirectory(path);
	}

	/**
	 * Returns the children of a given DFS path.
	 * 
	 * @param path a path within the DFS
	 * @return an array of child paths for the given parent.
	 * @throws IOException
	 */
	public Path[] listPaths(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());
		String relPath = path.toUri().getPath();
		String[] fileList = null;
		Path[] filePath = null;
		int fileCnt = 0;
		FileLock fuseLock=null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	fileList = f.list();
	}finally{
	fuseLock.release();  
	}
	}
	
	filePath = new Path[fileList.length];

	for (; fileCnt < fileList.length; fileCnt++) {
	filePath[fileCnt] = new Path(relPath + "/" + fileList[fileCnt]);
	}

	return filePath;
	}
   
	/**
	 * Lists a DFS path's children FileStatus.  FileStatus
	 * contain META information about a resource
	 * 
	 * @param path a DFS path
	 * @return an array of FileStatus[] 
	 */
	public FileStatus[] listStatus(Path path) throws IOException {
	return listStatus(path, true);
	}
	
	public FileStatus[] listStatus(Path path, boolean sync) throws IOException {
	int fileCnt = 0;
	Path absolute = makeAbsolute(path);
	String relpath = path.toUri().getPath();
	String[] strFileList = null;
	FileStatus[] fileStatus = null;
	boolean exists = false;
	boolean isFile = false;  
	File f = new File(absolute.toUri().getPath());
	FileLock fuseLock=null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	exists = f.exists();
	if(!exists)
	return null;
	
	isFile = f.isFile();
	if(!isFile)
	strFileList = f.list();
	}finally{
	fuseLock.release();   
	}
	}
	
	if (isFile)
	return new FileStatus[] { getFileStatus(path) };

	if (relpath.charAt(relpath.length() - 1) != '/')
	relpath += "/";

	fileStatus = new FileStatus[strFileList.length];

	for (; fileCnt < strFileList.length; fileCnt++) {
	fileStatus[fileCnt] = getFileStatusFromFileString(relpath
	+ strFileList[fileCnt]);
	}

	return fileStatus;
	}

	public FileStatus getFileStatusFromFileString(String path) throws IOException {
	Path nPath = new Path(path);
	return getFileStatus(nPath);
	}

	public FileStatus getFileStatus(Path path) throws IOException {
	Path absolute = makeAbsolute(path);
	boolean isDirectory = false;
	long lastModified = -1;
	long length = -1;
	FileLock fuseLock=null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	File f = new File(absolute.toUri().getPath());
	  
	if (!f.exists()){
	throw new FileNotFoundException("File " + path + " does not exist.");
	}
	
	isDirectory = f.isDirectory();
	lastModified = f.lastModified();
	length = f.length();
	}finally{
	fuseLock.release();    
	}
	}
	   
	FileStatus fs;
	// simple version - should work . we'll see.
	if (isDirectory){
	fs = new FileStatus(0, true, 1, 0, lastModified, path.makeQualified(this)) {
	public String getOwner() {
	return "root";
	}
	};
	}else{
	fs = new FileStatus(length, false, 0, getDefaultBlockSize(), lastModified, path.makeQualified(this)) {
	public String getOwner() {
	return "root";
	}
	};
	}
	return fs;
	}

	/**
	 * creates a new file in glusterfs namespace. internally the file descriptor
	 * is an instance of OutputStream class.
	 */
	 public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
	
	Path absolute = makeAbsolute(path);
	Path parent = null;
	File f = null;
	String filePath = null;
	FileLock fuseLock=null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	f = new File(absolute.toUri().getPath());
	filePath = f.getPath();
	if ( f.exists()) {
		if (overwrite){
			f.delete();
	}
	else{
		throw new IOException(f.getPath() + " already exist");
	}
	}
	}
	finally{
		fuseLock.release(); 
	}
	}
	
	parent = path.getParent();
	mkdirs(parent);

	   return new FSDataOutputStream(new GlusterFUSEOutputStream(filePath, false));
	}
	
	/**
	 * open the file in read mode (internally the file descriptor is an instance
	 * of InputStream class).
	 * 
	 * if quick read mode is set then read the file by by-passing FUSE if we are
	 * on same slave where the file exist
	 
	 * @param path DFS path to open.
	 */
	
	public FSDataInputStream open(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());
		FileLock fuseLock=null;
		TreeMap<Integer, GlusterFSBrickClass> hnts = null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	if (!f.exists())
	throw new IOException("File " + f.getPath() + " does not exist.");
	
	if (quickSlaveIO)
	hnts = GlusterFSXattr.quickIOPossible(f.getPath(), 0, f.length());
	
	}finally{
	fuseLock.release();  
	}
	}
	
	   return  new FSDataInputStream(new GlusterFUSEInputStream(f, hnts, hostname));
	}

	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		return open(path);
	}

	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		throw new IOException("append not supported (as yet).");
	}

	public boolean rename(Path src, Path dst) throws IOException {
		Path absoluteSrc = makeAbsolute(src);
		Path absoluteDst = makeAbsolute(dst);

		File fSrc = new File(absoluteSrc.toUri().getPath());
		File fDst = new File(absoluteDst.toUri().getPath());
	FileLock fuseLock=null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	if (fDst.isDirectory()) {
	fDst = null;
	String newPath = absoluteDst.toUri().getPath() + "/"
	+ fSrc.getName();
	fDst = new File(newPath);
	}
	   return fSrc.renameTo(fDst);
	}finally{
	fuseLock.release();  
	}
	}
	}

	@Deprecated
	public boolean delete(Path path) throws IOException {
	return delete(path, true);
	}

	public boolean delete(Path path, boolean recursive) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());
	FileLock fuseLock=null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	if (f.isFile())
	return f.delete();
	}finally{
	fuseLock.release();  
	}
	}
	
	FileStatus[] dirEntries = listStatus(absolute,false);
	if ((!recursive) && (dirEntries != null) && (dirEntries.length != 0)){
	throw new IOException("Directory " + path.toString()
	+ " is not empty");
	}
	
	if (dirEntries != null){
	for (int i = 0; i < dirEntries.length; i++){
	delete(new Path(absolute, dirEntries[i].getPath()), recursive);
	}
	}
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	return f.delete();
	}finally{
	fuseLock.release();   
	}
	}
	}

	@Deprecated
	public long getLength(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());
	FileLock fuseLock=null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	if (!f.exists())
	throw new IOException(f.getPath() + " does not exist.");
	return f.length();
	}finally{
	fuseLock.release();
	}
	}
	}

	@Deprecated
	public short getReplication(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());
		FileLock fuseLock=null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	if (!f.exists()){
	throw new IOException(f.getPath() + " does not exist.");
	}
	
	return xattr.getReplication(f.getPath());
	}finally{
	fuseLock.release();  
	}
	}
	}

	public short getDefaultReplication(Path path) throws IOException {
	return getReplication(path);
	}

	public boolean setReplication(Path path, short replication) throws IOException {
	return true;
	}

	public long getBlockSize(Path path) throws IOException {
	long blkSz;
	Path absolute = makeAbsolute(path);
	File f = new File(absolute.toUri().getPath());
	FileLock fuseLock=null;
	
	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	blkSz = GlusterFSXattr.getBlockSize(f.getPath());
	}finally{
	fuseLock.release();  
	}
	}
	
	if (blkSz == 0)
	blkSz = getLength(path);

		return blkSz;
	}

	public long getDefaultBlockSize() {
		return 1 << 26; /* default's from hdfs, kfs */
	}

	@Deprecated
	public void lock(Path path, boolean shared) throws IOException {
	}

	@Deprecated
	public void release(Path path) throws IOException {
	}

	public BlockLocation[] getFileBlockLocations(FileStatus file, long start, 
			long len) throws IOException {
	Path absolute = makeAbsolute(file.getPath());
	File f = new File(absolute.toUri().getPath());
	FileLock fuseLock=null;

	synchronized (fuseFileLock) {
	try{
	fuseLock = fuseFileLock.getChannel().lock();
	return GlusterFSXattr.getPathInfo(f.getPath(), start, len);
	
	}finally{
	fuseLock.release();  
	}
	}
	}

	// getFileBlockLocations (FileStatus, long, long) is called by hadoop
	public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException {
	return null;
	}

	public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
	FileUtil.copy(glusterFs, src, this, dst, delSrc, getConf());
	}

	public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
	FileUtil.copy(this, src, glusterFs, dst, delSrc, getConf());
	}
  
	public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
	return tmpLocalFile;
	}

	public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException {
	moveFromLocalFile(tmpLocalFile, fsOutputFile);
	}
	
	public void finalize(){
	if (this.fuseFileLock !=null){
	try {
	this.fuseFileLock.getChannel().close();
	this.fuseFileLock.close();
	} catch (IOException e) {
	   // too bad
	}
	}
	}
	
}
