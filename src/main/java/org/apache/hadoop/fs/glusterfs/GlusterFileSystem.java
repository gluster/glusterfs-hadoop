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

import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.HadoopVersionAnnotation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.StringUtils;

/*
 * This package provides interface for hadoop jobs (incl. Map/Reduce)
 * to access files in GlusterFS backed file system via FUSE mount
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
		String s = null;
		String mountCmd = null;

		mountCmd = "mount -t glusterfs " + server + ":" + "/" + volname + " "
				+ mount;
		System.out.println(mountCmd);
		try {
			p = Runtime.getRuntime().exec(mountCmd);
			retVal = p.waitFor();
			if (retVal != 0)
				ret = false;
		} 
		catch (IOException e) {
			System.out.println("Problem mounting FUSE mount on: " + mount);
			throw new RuntimeException(e);
		}
		return ret;
	}

	public void initialize(URI uri, Configuration conf) throws IOException {
		boolean ret = false;
		String volName = null;
		String remoteGFSServer = null;
		String needQuickRead = null;

		if (this.mounted)
			return;

		System.out.println("Initializing GlusterFS");

		try {
			volName = conf.get("fs.glusterfs.volname", "");
			glusterMount = conf.get("fs.glusterfs.mount", "");
			remoteGFSServer = conf.get("fs.glusterfs.server", "");
			needQuickRead = conf.get("quick.slave.io", "");

			/*
			 * bail out if we do not have enough information to do a FUSE mount
			 */
			if ((volName.length() == 0) || (remoteGFSServer.length() == 0)
					|| (glusterMount.length() == 0))
				throw new RuntimeException(
						"Not enough info for FUSE Mount : volname=" + volName
								+ ",server=" + remoteGFSServer
								+ ",glustermount=" + glusterMount);

			ret = FUSEMount(volName, remoteGFSServer, glusterMount);
			if (!ret) {
				System.out.println("Failed to initialize GlusterFS");
				System.exit(-1);
			}

			if ((needQuickRead.length() != 0)
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

		} catch (Exception e) {
			System.out.println("Unable to initialize GlusterFS");
			throw new RuntimeException(e);
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

	public boolean exists(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());

		return f.exists();
	}

	public boolean mkdirs(Path path, FsPermission permission)
			throws IOException {
		boolean created = false;
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());

		if (f.exists()) {
			System.out.println("Directory " + f.getPath() + " already exist");
			return true;
		}

		return f.mkdirs();
	}

	@Deprecated
	public boolean isDirectory(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());

		return f.isDirectory();
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

		fileList = f.list();

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

		if (!f.exists()) {
			return null;
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


	/**
	 * Adopted from The Existing raw local file status already implements the equivalent of the FUSE requirements.
	 */
	 public class RawLocalFileStatus extends FileStatus {
	    /* We can add extra fields here. It breaks at least CopyFiles.FilePair().
	     * We recognize if the information is already loaded by check if
	     * onwer.equals("").
	     */
	    private boolean isPermissionLoaded() {
	      return !super.getOwner().equals(""); 
	    }
	    
	    RawLocalFileStatus(File f, long defaultBlockSize, FileSystem fs) {
	      super(f.length(), f.isDirectory(), 1, defaultBlockSize,
	            f.lastModified(), new Path(f.getPath()).makeQualified(fs));
	    }
	    
	    @Override
	    public FsPermission getPermission() {
	      if (!isPermissionLoaded()) {
	        loadPermissionInfo();
	      }
	      return super.getPermission();
	    }

	    @Override
	    public String getOwner() {
	      if (!isPermissionLoaded()) {
	        loadPermissionInfo();
	      }
	      return super.getOwner();
	    }

	    @Override
	    public String getGroup() {
	      if (!isPermissionLoaded()) {
	        loadPermissionInfo();
	      }
	      return super.getGroup();
	    }
	    
	    /// loads permissions, owner, and group from `ls -ld`
	    private void loadPermissionInfo() {
	      IOException e = null;
	      try {
	        StringTokenizer t = new StringTokenizer(
	        		org.apache.hadoop.util.Shell.execCommand("ls","-ld",getPath().toUri().getPath()));
	        //expected format
	        //-rw-------    1 username groupname ...
	        String permission = t.nextToken();
	        if (permission.length() > 10) { //files with ACLs might have a '+'
	          permission = permission.substring(0, 10);
	        }
	        setPermission(FsPermission.valueOf(permission));
	        t.nextToken();
	        setOwner(t.nextToken());
	        setGroup(t.nextToken());
	      } 
	      catch (Throwable ioe) {
	    	  throw new RuntimeException(ioe);
	      } 
	    }

	    @Override
	    public void write(DataOutput out) throws IOException {
	      if (!isPermissionLoaded()) {
	        loadPermissionInfo();
	      }
	      super.write(out);
	    }
	  }
	
	private class FUSEFileStatus extends RawLocalFileStatus{
		File theFile;
		Path path;
		boolean isdir;
		short blockReplication;
		public FUSEFileStatus(File f, boolean isdir, short block_replication,
				long blocksize, Path path) {
			// if its a dir, 0 length
			super(f, blocksize, GlusterFileSystem.this);
			/**	super(isdir ? 0 : f.length(), isdir, block_replication, blocksize,
					f.lastModified(), path);
					**/
			this.path=path;
			this.theFile = f;
			this.blockReplication=block_replication;
		}
		
		@Override
		public short getReplication(){
			return blockReplication;
		}
		@Override
		public boolean isDir(){
			return isdir;
		}
		@Override
		public Path getPath(){
			return path;
		}
	}

	public FileStatus getFileStatus(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		final File f = new File(absolute.toUri().getPath());

		if (!f.exists())
			throw new FileNotFoundException("File " + f.getPath()
					+ " does not exist.");
		FileStatus fs;

		// simple version - should work . we'll see.
		// TODO COMPARE these w/ original signatures - do we retain the correct
		// default args?
		if (f.isDirectory())
			fs = new FUSEFileStatus(f, true, (short) 1, 0, path.makeQualified(this));
		else
			fs = new FUSEFileStatus(f, false, (short) 0, getDefaultBlockSize(),
					path.makeQualified(this));
		return fs;

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

		if (f.exists()) {
			if (overwrite)
				f.delete();
			else
				throw new IOException(f.getPath() + " already exist");
		}

		parent = path.getParent();
		fParent = new File((makeAbsolute(parent)).toUri().getPath());
		if ((parent != null) && (fParent != null) && (!fParent.exists()))
			if (!fParent.mkdirs())
				throw new IOException("cannot create parent directory: "
						+ fParent.getPath());

		glusterFileStream = new FSDataOutputStream(new GlusterFUSEOutputStream(
				f.getPath(), false));

		return glusterFileStream;
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

		if (!f.exists())
			throw new IOException("File " + f.getPath() + " does not exist.");

		if (quickSlaveIO)
			hnts = xattr.quickIOPossible(f.getPath(), 0, f.length());

		glusterFileStream = new FSDataInputStream(new GlusterFUSEInputStream(f,
				hnts, hostname));
		return glusterFileStream;
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

		if (fDst.isDirectory()) {
			fDst = null;
			String newPath = absoluteDst.toUri().getPath() + "/"
					+ fSrc.getName();
			fDst = new File(newPath);
		}
		return fSrc.renameTo(fDst);
	}

	@Deprecated
	public boolean delete(Path path) throws IOException {
		return delete(path, true);
	}

	public boolean delete(Path path, boolean recursive) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());

		if (f.isFile())
			return f.delete();

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

		if (!f.exists())
			throw new IOException(f.getPath() + " does not exist.");

		return f.length();
	}

	@Deprecated
	public short getReplication(Path path) throws IOException {
		Path absolute = makeAbsolute(path);
		File f = new File(absolute.toUri().getPath());

		if (!f.exists())
			throw new IOException(f.getPath() + " does not exist.");

		return xattr.getReplication(f.getPath());
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

		blkSz = xattr.getBlockSize(f.getPath());
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
		BlockLocation[] result = null;

		if (file == null)
			return null;

		result = xattr.getPathInfo(f.getPath(), start, len);
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
		FileUtil.copy(glusterFs, src, this, dst, delSrc, getConf());
	}

	public void copyToLocalFile(boolean delSrc, Path src, Path dst)
			throws IOException {
		FileUtil.copy(this, src, glusterFs, dst, delSrc, getConf());
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