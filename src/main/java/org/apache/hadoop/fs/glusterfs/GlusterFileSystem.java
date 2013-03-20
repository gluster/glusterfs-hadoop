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
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import java.util.regex.*;

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

/*
 * This package provides interface for hadoop jobs (incl. Map/Reduce)
 * to access files in GlusterFS backed file system via FUSE mount
 */


/*
 * 
 * TODO: Evaluate LocalFileSystem and RawLocalFileSystem as possible delegate file systems to remove & refactor this code.
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

	private int debug = 0;
	private boolean selfLock = false;
	public static final String LOCK_FILE_EXTENSION = ".gfs.lock";
	public static final int LOCK_WAIT = 60 * 1000; // time to sleep while waiting for the lock to release
	
	
	
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
		String mountCmd = null;

		mountCmd = "mount -t glusterfs " + server + ":" + "/" + volname + " "+ mount;
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
			//throw new RuntimeException("Problem mounting FUSE mount on: "+ mount);
		}

		return ret;
	}

	public void initialize(URI uri, Configuration conf) throws IOException {
		boolean ret = false;
		String volName = null;
		String remoteGFSServer = null;
		String needQuickRead = null;
		boolean autoMount = true;

		if (this.mounted)
			return;

		System.out.println("Initializing GlusterFS");

		try {
			volName = conf.get("fs.glusterfs.volname", null);
			glusterMount = conf.get("fs.glusterfs.mount", null);
			remoteGFSServer = conf.get("fs.glusterfs.server", null);
			needQuickRead = conf.get("quick.slave.io", null);
			autoMount = conf.getBoolean("fs.glusterfs.automount", true);
			
			/* Figure out the debug level (as integer).  0 is none. */
			String debugString = conf.get("fs.glusterfs.debug.level", null);
			if(debugString==null){
				debug = 0;
			}else{
				debug = Integer.parseInt(debugString);
			}
				
			//selfLock = Boolean.parseBoolean(conf.get("fs.glusterfs.safe.lock", "false"));
			selfLock = true;
			if ((volName.length() == 0) || (remoteGFSServer.length() == 0)
					|| (glusterMount.length() == 0))
				throw new RuntimeException("Not enough info to mount FUSE: volname="+volName + " glustermount=" + glusterMount);


			if (autoMount) {
				ret = FUSEMount(volName, remoteGFSServer, glusterMount);
				if (!ret) {
					throw new RuntimeException("Initialize: Failed to mount GlusterFS ");
				}
			}

			if((needQuickRead.length() != 0)
					&& (needQuickRead.equalsIgnoreCase("yes")
							|| needQuickRead.equalsIgnoreCase("on") || needQuickRead
								.equals("1")))
				this.quickSlaveIO = true;

			this.mounted = true;
			this.glusterFs = FileSystem.getLocal(conf);
			this.workingDir = new Path(glusterMount);
			this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());

			this.xattr = new GlusterFSXattr();

			InetAddress addr = InetAddress.getLocalHost();
			this.hostname = addr.getHostName();

			setConf(conf);
		} 
		catch (Exception e) {
			e.printStackTrace();
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

	public boolean exists(Path f) throws IOException {
		  return getFileStatus(f) != null;
	}
	public boolean mkdirs(Path f,  FsPermission permission) throws IOException {
	    return mkdirs(f,true,permission);
	}

	/*
	 * Code copied from:
	 * @see org.apache.hadoop.fs.RawLocalFileSystem#mkdirs(org.apache.hadoop.fs.Path)
	 * as incremental fix towards a re-write. of this class to remove duplicity.
	 * 
	 */
	public boolean mkdirs(Path f, boolean lock, FsPermission permission) throws IOException {
        
        if(f==null) return true;
            
        Path parent = f.getParent();
        Path absolute = makeAbsolute(f);
        File p2f = new File(absolute.toUri().getPath());
        try{
        	if(lock) lock(f);	
        	return (f == null || mkdirs(parent,lock,permission)) && (p2f.mkdir() || p2f.isDirectory());
        }finally{
        	if(lock) release(f);
        }
    }

	@Deprecated
	public boolean isDirectory(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		try{
			lock(path);
			File f = new File(absolute.toUri().getPath());
			return f.isDirectory();
		}finally{
			release(path);
		}
	}

	public boolean isFile(Path path) throws IOException {
		return !isDirectory(path);
	}

	public Path[] listPaths(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());
		String relPath = path.toUri().getPath();
		String[] fileList = null;
		Path[] filePath = null;
		int fileCnt = 0;
		try{
			lock(path);
			fileList = f.list();
		}finally{
			release(path);
		}
		
		filePath = new Path[fileList.length];

		for (; fileCnt < fileList.length; fileCnt++) {
			filePath[fileCnt] = new Path(relPath + "/" + fileList[fileCnt]);
		}

		return filePath;
	}

	public FileStatus[] listStatus(Path path) throws IOException {
		int fileCnt = 0;
		Path absolute = makeAbsolute(path);
		String relpath = path.toUri().getPath();
		String[] strFileList = null;
		FileStatus[] fileStatus = null;
		File f = new File(absolute.toUri().getPath());
		try{
			lock(path);
			if (!f.exists()) {
				return null;
			}
		}finally{
			release(path);
		}

		if (f.isFile())
			return new FileStatus[] { getFileStatus(path) };

		if (relpath.charAt(relpath.length() - 1) != '/')
			relpath += "/";

		strFileList = f.list();

		fileStatus = new FileStatus[strFileList.length];

		for (; fileCnt < strFileList.length; fileCnt++) {
			fileStatus[fileCnt] = getFileStatusFromFileString(relpath
					+ strFileList[fileCnt]);
		}

		return fileStatus;
	}

	public FileStatus getFileStatusFromFileString(String path)
			throws IOException {
		Path nPath = new Path(path);
		return getFileStatus(nPath);
	}

	public FileStatus getFileStatus(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());
		try{
			lock(path);
			if (!f.exists()){
				throw new FileNotFoundException("File " + f.getPath()
					+ " does not exist.");
			}
			FileStatus fs;
		// simple version - should work . we'll see.
			if (f.isDirectory()){
				fs = new FileStatus(0, true, 1, 0, f.lastModified(),
					path.makeQualified(this)) {
					public String getOwner() {
						return "root";
					}
				};
			}else{
				fs = new FileStatus(f.length(), false, 0, getDefaultBlockSize(),
						f.lastModified(), path.makeQualified(this)) {
					public String getOwner() {
						return "root";
					}
				};
			}
			return fs;
		}finally{
			release(path);
		}
	}

	/*
	 * creates a new file in glusterfs namespace. internally the file descriptor
	 * is an instance of OutputStream class.
	 */
	public FSDataOutputStream create(Path path, FsPermission permission,
	        boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		
	    Path absolute = makeAbsolute(path);
		Path parent = null;
		File f = null;
		File fParent = null;
		FSDataOutputStream glusterFileStream = null;

		f = new File(absolute.toUri().getPath());
		try{
			lock(path);
			if (f.exists()) {
				if (overwrite)
					f.delete();
				else
					throw new IOException(f.getPath() + " already exist");
			}
			parent = path.getParent();
		}finally{
			release(path);
		}
		
		mkdirs(parent);
		try{
			lock(path);
			glusterFileStream = new FSDataOutputStream(new GlusterFUSEOutputStream(f.getPath(), false));
			return glusterFileStream;
		}finally{
			release(path);
		}
	}

	/*
	 * open the file in read mode (internally the file descriptor is an instance
	 * of InputStream class).
	 * 
	 * if quick read mode is set then read the file by by-passing FUSE if we are
	 * on same slave where the file exist
	 */
	public FSDataInputStream open(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());
		FSDataInputStream glusterFileStream = null;
		TreeMap<Integer, GlusterFSBrickClass> hnts = null;
		try{
			lock(path);
			if (!f.exists())
				throw new IOException("File " + f.getPath() + " does not exist.");
	
			if (quickSlaveIO)
				hnts = xattr.quickIOPossible(f.getPath(), 0, f.length());
	
			return new FSDataInputStream(new GlusterFUSEInputStream(f, hnts, hostname));

		}finally{
			release(path);
		}
	}

	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		return open(path);
	}

	public FSDataOutputStream append(Path f, int bufferSize,
			Progressable progress) throws IOException {
		throw new IOException("append not supported (as yet).");
	}

	public boolean rename(Path src, Path dst) throws IOException {
		try{
			lock(src);
			lock(dst);
			
			Path absoluteSrc = makeAbsolute(src);
			Path absoluteDst = makeAbsolute(dst);
	
			File fSrc = new File(absoluteSrc.toUri().getPath());
			File fDst = new File(absoluteDst.toUri().getPath());
	
			if (fDst.isDirectory()) {
				fDst = null;
				String newPath = absoluteDst.toUri().getPath() + "/"
						+ fSrc.getName();
				fDst = new File(newPath);
			}
			return fSrc.renameTo(fDst);
		}finally{
			release(src);
			release(dst);
		}
	}

	@Deprecated
	public boolean delete(Path path) throws IOException {
		return delete(path, true);
	}

	public boolean delete(Path path, boolean recursive) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());
		try{
			
			lock(path);
			if (f.isFile())
			return f.delete();
		}finally{
			release(path);
		}
		
		FileStatus[] dirEntries = listStatus(absolute);
		if ((!recursive) && (dirEntries != null) && (dirEntries.length != 0))
			throw new IOException("Directory " + path.toString()
					+ " is not empty");

		if (dirEntries != null)
			for (int i = 0; i < dirEntries.length; i++)
				delete(new Path(absolute, dirEntries[i].getPath()), recursive);

		return f.delete();
	}

	@Deprecated
	public long getLength(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		
		File f = new File(absolute.toUri().getPath());
		try{
			lock(path);
			if (!f.exists())
				throw new IOException(f.getPath() + " does not exist.");

			return f.length();
		}finally{
			release(path);
		}
	}

	@Deprecated
	public short getReplication(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		
		File f = new File(absolute.toUri().getPath());
		try{
			lock(path);
			if (!f.exists())
				throw new IOException(f.getPath() + " does not exist.");

			return xattr.getReplication(f.getPath());
		}finally{
			release(path);
		}
		
	}

	public short getDefaultReplication(Path path) throws IOException {
		return getReplication(path);
	}

	public boolean setReplication(Path path, short replication)
			throws IOException {
		return true;
	}

	public long getBlockSize(Path path) throws IOException {
		long blkSz;
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());
		try{
			lock(path);
			blkSz = xattr.getBlockSize(f.getPath());
		}finally{
			release(path);
		}
		
		if (blkSz == 0)
			blkSz = getLength(path);

		return blkSz;
	}

	public long getDefaultBlockSize() {
		return 1 << 26; /* default's from hdfs, kfs */
	}
	
	private boolean isLocked(Path path){
		/* 
		 * logic for checking if a file is locked.  First check if the file is locked, 
		 * then see if any of its parent directories are locked 
		 */
	    if("".equals(path.getName())){
	        Path absolute = makeAbsolute(new Path(""));
	        File f = new File(absolute.toUri().getPath() + LOCK_FILE_EXTENSION);
	    }
	    
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath() + LOCK_FILE_EXTENSION);
		
		if(f.exists()) return true;

		Path parent = path.getParent();
	   
	    
	    return isLocked(parent);
		
	}
	@Deprecated
	public void lock(Path path, boolean shared) throws IOException {
		lock(path);
	}
	
	public void lock(Path path) throws IOException {
	    if(debug > 0) System.out.println("lock(" + path + ")");
		/* Sure its deprecated, but lets use it anyways! */
		while(isLocked(path)){
			try{
				Thread.sleep(GlusterFileSystem.LOCK_WAIT);
			}catch(InterruptedException ex){
				throw new IOException("Error while waiting for lock on file: " + path + " Exception:" + ex.getMessage());
			}
		}
		/* root locking */
		if("".equals(path.getName())){
            File f = new File("root" + LOCK_FILE_EXTENSION);
            f.createNewFile();
        }
        
		/* directory + file locking */
		mkdirs(path.getParent(), false, null);
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath() + LOCK_FILE_EXTENSION);
		f.createNewFile();
	}

	@Deprecated
	public void release(Path path) throws IOException {
	    if(debug > 0) System.out.println("release(" + path + ")");
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath() + LOCK_FILE_EXTENSION);
		f.delete();
		
	}

	public BlockLocation[] getFileBlockLocations(FileStatus file, long start,
			long len) throws IOException {

		Path absolute = makeAbsolute(file.getPath());
		File f = new File(absolute.toUri().getPath());
		BlockLocation[] result = null;

		if (file == null)
			return null;
		try{
			lock(new Path(f.getPath()));
			result = xattr.getPathInfo(f.getPath(), start, len);
		
		}finally{
			release(new Path(f.getPath()));
		}
		
		if (result == null) {
			System.out.println("Problem getting destination host for file "
					+ f.getPath());
			return null;
		}

		return result;
	}

	// getFileBlockLocations (FileStatus, long, long) is called by hadoop
	public BlockLocation[] getFileBlockLocations(Path p, long start, long len)
			throws IOException {
		return null;
	}

	public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
			throws IOException {
		try{
			lock(dst);
			FileUtil.copy(glusterFs, src, this, dst, delSrc, getConf());
		}finally{
			release(dst);
		}
	}

	public void copyToLocalFile(boolean delSrc, Path src, Path dst)
			throws IOException {
		try{
			lock(src);
			FileUtil.copy(this, src, glusterFs, dst, delSrc, getConf());
		}finally{
			release(src);
		}
	}

	public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
			throws IOException {
		return tmpLocalFile;
	}

	public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile)
			throws IOException {
		moveFromLocalFile(tmpLocalFile, fsOutputFile);
	}
}
