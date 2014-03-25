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
 * Extends the RawLocalFileSystem to add support for Gluster Volumes. 
 * 
 */

package org.apache.hadoop.fs.libgfsio;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.gluster.fs.GlusterClient;
import org.gluster.fs.GlusterFile;
import org.gluster.fs.GlusterVolume;
import org.gluster.fs.IGlusterInputStream;
import org.gluster.fs.IGlusterOutputStream;

public class GlusterfsVolume extends FileSystem {

    static final URI NAME = URI.create("glusterfs:///");
    private Path workingDir;
    public static final int OVERRIDE_WRITE_BUFFER_SIZE = 1024 * 4;
    public static final int OPTIMAL_WRITE_BUFFER_SIZE = 1024 * 128;

    public GlusterVolume vol;
    public GlusterClient client;

    public GlusterfsVolume() {}

    public GlusterfsVolume(Configuration conf) {
        this.setConf(conf);

    }

    private Path makeAbsolute(Path f){
        if (f.isAbsolute()) {
            return f;
        } else {
            return new Path(workingDir, f);
        }
    }

    
    public URI getUri(){
        return NAME;
    }

    
   
    public void setConf(Configuration conf){

        if (conf == null)
            return;

        super.setConf(conf);

        String volume = conf.get("fs.glusterfs.volume", "gv0");
        String server = conf.get("fs.glusterfs.server", "localhost");

        client = new GlusterClient(server);

        try {
            vol = client.connect(volume);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Path workingDirectory = getInitialWorkingDirectory();
        try {
            mkdirs(workingDirectory);
        } catch (IOException e) {
            e.printStackTrace();
        }
        setWorkingDirectory(workingDirectory);
        System.out.println("Working directory is : " + getWorkingDirectory());

        /**
         * Write Buffering
         */
        Integer userBufferSize = conf.getInt("io.file.buffer.size", -1);
        if (userBufferSize == OVERRIDE_WRITE_BUFFER_SIZE || userBufferSize == -1) {
            conf.setInt("io.file.buffer.size", OPTIMAL_WRITE_BUFFER_SIZE);
        }

    }

    public String pathOnly(Path path){
        
        return makeQualified(path).toUri().getPath();
    }

    
  
    public void initialize(URI uri, Configuration conf) throws IOException{
        super.initialize(uri, conf);
        setConf(conf);
    }

    class TrackingInputStreamWrapper extends InputStream {

        IGlusterInputStream ios = null;
        long bytesRead = 0;

        public TrackingInputStreamWrapper(IGlusterInputStream ios) throws IOException {
            this.ios = ios;
        }

        public IGlusterInputStream getChannel(){
            return this.ios;
        }

        
        public int read() throws IOException{
            int result = ios.read();
            if (result != -1) {
                bytesRead += result;
                statistics.incrementBytesRead(1);
            }
            return result;
        }

        
        public int read(byte[] data) throws IOException{
            int result = ios.read(data,0,data.length);
            if (result != -1) {
                bytesRead += result;
                statistics.incrementBytesRead(result);
            }
            return result;
        }

        
        public int read(byte[] data, int offset, int length) throws IOException{
            int result = ios.read(data, offset, length);
          
            if (result != -1) {
                bytesRead += result;
                statistics.incrementBytesRead(result);
            }
            return result;
        }
    }

    /*******************************************************
     * For open()'s FSInputStream.
     *******************************************************/
    class GlussterFileInputStream extends FSInputStream {
        private TrackingInputStreamWrapper fis;
        private long bytesReadThisStream = 0;
        private String fileName = null;
        IGlusterInputStream gis;
        
        public GlussterFileInputStream(Path f) throws IOException {
            gis = vol.open(pathOnly(f)).bufferedInputStream();
            fileName = f.toString();
            this.fis = new TrackingInputStreamWrapper(gis);
        }

        
        public void seek(long pos) throws IOException{
            gis.seek(pos);
        }

        
        public long getPos() throws IOException{
            return gis.offset();
        }

        
        public boolean seekToNewSource(long targetPos) throws IOException{
            seek(targetPos);
            return true;
        }

        
        public int available() throws IOException{
            return fis.available();
        }

        
        public void close() throws IOException{
            fis.close();
        }

        
        public boolean markSupported(){
            return ((InputStream) gis).markSupported();
        }

        
        public int read() throws IOException{
            bytesReadThisStream++;
            return fis.read();
        }

        
        public int read(byte[] b, int off, int len) throws IOException{
            int read = fis.read(b, off, len);
            bytesReadThisStream += read;
            return read;
        }

        
        public int read(long position, byte[] b, int off, int len) throws IOException{
            seek(position);
            int read = fis.getChannel().read(b, off, len);
            return read;
        }

        
        public long skip(long n) throws IOException{
            return fis.skip(n);
        }

    }

    
    public FSDataInputStream open(Path f, int bufferSize) throws IOException{
        if (!exists(f)) {
            throw new FileNotFoundException(f.toString());
        }
        f = makeQualified(f);
        return new FSDataInputStream(new BufferedFSInputStream(new GlussterFileInputStream(f), bufferSize));
    }

    /*********************************************************
     * For create()'s FSOutputStream.
     *********************************************************/
    class GlusterFileOutputStream extends OutputStream {
        private IGlusterOutputStream fos;

        private GlusterFileOutputStream(Path f, boolean append) throws IOException {
            GlusterFile file = vol.open(pathOnly(f));
            try {
                if (!file.exists()) {
                    file.createNewFile();
                }
            } catch (Exception ex) {
                throw new IOException("Error creating " + f + ":\n" + ex);
            }

            this.fos = file.outputStream();
            if (append) {
                this.fos.position(file.length() - 1);
            }
        }

        
        public void close() throws IOException{
            fos.close();
        }

        
        public void flush() throws IOException{
            fos.flush();
        }

        
        public void write(byte[] b, int off, int len) throws IOException{
            fos.write(b, off, len);
        }

        
        public void write(int b) throws IOException{
            fos.write(b);
        }
    }

    public BlockLocation[] getFileBlockLocations(FileStatus file,long start,long len) throws IOException{
        GlusterFile f = vol.open(pathOnly(file.getPath()));
        BlockLocation[] result=null;
        GlusterfsXattr at = new GlusterfsXattr();
        return at.getPathInfo(f, start, len);

    }
    
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException{
        f = makeQualified(f);
        if (!exists(f)) {
            throw new FileNotFoundException("File " + f + " not found");
        }
        if (getFileStatus(f).isDirectory()) {
            throw new IOException("Cannot append to a diretory (=" + f + " )");
        }
        return new FSDataOutputStream(new BufferedOutputStream(new GlusterFileOutputStream(f, true), bufferSize), statistics);
    }

    
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        f = makeQualified(f);
        return create(f, overwrite, true, bufferSize, replication, blockSize, progress);
    }

    private FSDataOutputStream create(Path f, boolean overwrite, boolean createParent, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        f = makeQualified(f);
        if (exists(f) && !overwrite) {
            throw new IOException("File already exists: " + f);
        }

        Path parent = f.getParent();
        if (parent != null && !mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent.toString());
        }
        return new FSDataOutputStream(new BufferedOutputStream(new GlusterFileOutputStream(f, false), bufferSize), statistics);
    }

    
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        f = makeQualified(f);
        
        if (exists(f) && !flags.contains(CreateFlag.OVERWRITE)) {
            throw new IOException("File already exists: " + f);
        }
        return new FSDataOutputStream(new BufferedOutputStream(new GlusterFileOutputStream(f, false), bufferSize), statistics);
    }

    
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        f = makeQualified(f);
        FSDataOutputStream out = create(f, overwrite, bufferSize, replication, blockSize, progress);
        setPermission(f, permission);
        return out;
    }

    
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        f = makeQualified(f);
        FSDataOutputStream out = create(f, overwrite, false, bufferSize, replication, blockSize, progress);
        setPermission(f, permission);
        return out;
    }

    
    public boolean rename(Path src, Path dst) throws IOException{
         if (vol.open(pathOnly(src)).renameTo(vol.open(pathOnly(dst)))) {
            return true;
        }
        return FileUtil.copy(this, src, this, dst, true, getConf());
    }

    /**
     * Delete the given path to a file or directory.
     * 
     * @param p
     *            the path to delete
     * @param recursive
     *            to delete sub-directories
     * @return true if the file or directory and all its contents were deleted
     * @throws IOException
     *             if p is non-empty and recursive is false
     */
    
    public boolean delete(Path p, boolean recursive) throws IOException{
        p = makeQualified(p);
        GlusterFile file = vol.open(pathOnly(p));
        return file.delete(recursive);
    }

    
    public FileStatus[] listStatus(Path f) throws IOException{
        f = makeQualified(f);
        
        GlusterFile localf = vol.open(pathOnly(f));
        FileStatus[] results;

        if (!localf.exists()) {
            throw new FileNotFoundException("File " + f + " does not exist");
        }
        if (localf.isFile()) {
            return new FileStatus[] { new GlusterFileStatus(localf, this) };
        }

        GlusterFile[] names = localf.listFiles();
        if (names == null) {
            return null;
        }
        results = new FileStatus[names.length];
        int j = 0;
        for (int i = 0; i < names.length; i++) {
            try {
                results[j] = getFileStatus(new Path(names[i].getPath()));
                j++;
            } catch (FileNotFoundException e) {
                // ignore the files not found since the dir list may have have
                // changed
                // since the names[] list was generated.
            }
        }
        if (j == names.length) {
            return results;
        }
        return Arrays.copyOf(results, j);
    }

    /**
     * Creates the specified directory hierarchy. Does not treat existence as an
     * error.
     */
   
    
    public boolean mkdirs(Path f) throws IOException{
        f = makeQualified(f);
        
        if (f == null) {
            throw new IllegalArgumentException("mkdirs path arg is null");
        }
        Path parent = f.getParent();
        GlusterFile p2f = vol.open(pathOnly(f));
        if (parent != null) {
            GlusterFile parent2f = vol.open(pathOnly(parent));
            if (parent2f != null && parent2f.exists() && !parent2f.isDirectory()) {
                throw new FileAlreadyExistsException("Parent path is not a directory: " + parent);
            }
        }
        return (parent == null || mkdirs(parent)) && (p2f.mkdir() || p2f.isDirectory());
    }

    
    public boolean mkdirs(Path f, FsPermission permission) throws IOException{
        boolean b = mkdirs(f);
        if (b) {
            setPermission(f, permission);
        }
        return b;
    }

    
    protected boolean primitiveMkdir(Path f, FsPermission absolutePermission) throws IOException{
        boolean b = mkdirs(f);
        setPermission(f, absolutePermission);
        return b;
    }

    
    public Path getHomeDirectory(){
        return this.makeQualified(new Path(System.getProperty("user.home")));
    }

    /**
     * Set the working directory to the given directory.
     */
    
    public void setWorkingDirectory(Path newDir){
        workingDir = makeAbsolute(newDir);
        checkPath(workingDir);

    }

    
    public Path getWorkingDirectory(){
        return workingDir;
    }

    
    protected Path getInitialWorkingDirectory(){
        return new Path(GlusterfsVolume.NAME + "user/" + System.getProperty("user.name"));
    }

    
    public FsStatus getStatus(Path p) throws IOException{
        p = makeQualified(p);
        // assume for now that we're only dealing with one volume.
        // GlusterFile partition = vol.open(pathOnly(p == null ? new Path("/") :
        // p));
        // File provides getUsableSpace() and getFreeSpace()
        // File provides no API to obtain used space, assume used = total - free
        return new GlusterFsStatus(vol);
    }

    
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException{
        return fsOutputFile;
    }

    // It's in the right place - nothing to do.
    
    public void completeLocalOutput(Path fsWorkingFile, Path tmpLocalFile) throws IOException{
    }

    
    public void close() throws IOException{
        super.close();
    }

    
    public String toString(){
        return "GlusterFs Volume - glide:" + vol.getName();
    }

    
    public FileStatus getFileStatus(Path f) throws IOException{
        f = makeQualified(f);
        
        GlusterFile file = vol.open(pathOnly(f));

        if (file.exists()) {
            return new GlusterFileStatus(vol.open(pathOnly(f)), this);
        } else {
            throw new FileNotFoundException("File " + f + " does not exist.");
        }
    }

    
    public void setOwner(Path p, String username, String groupname) throws IOException{
        p = makeQualified(p);
        if (username == null && groupname == null) {
            throw new IOException("username == null && groupname == null");
        }
        GlusterFile gf = vol.open(pathOnly(p));
        long gid = -1;
        long uid = -1;

        if (username == null) {
            uid = gf.getUid();
        }

        if (groupname == null) {
            gid = gf.getGid();
        }

    }

    
    public void setPermission(Path p, FsPermission permission) throws IOException{
        p = makeQualified(p);
        GlusterFile gf = vol.open(pathOnly(p));
        gf.chmod(permission.toShort());
    }

}
