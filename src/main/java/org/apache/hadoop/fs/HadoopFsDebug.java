package org.apache.hadoop.fs;

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
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Options.Rename;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Progressable;

public class HadoopFsDebug extends FileSystem{

    FileSystem proxy;
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
            String stackTrace = getStackTrace(3);
            if(showStackTrace) out.write(stackTrace);
            out.close();
        }catch (FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }

    }
    
    public HadoopFsDebug(){}

    public HadoopFsDebug(String fileSystemClass){
        try {
            proxy = (FileSystem)Class.forName(fileSystemClass).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    

    public void setConf(Configuration conf){
        if(proxy==null){
            String fileSystem = conf.get("fs.hadoop.debug", "org.apache.hadoop.fs.libgfsio.GlusterFileSystem");
            try {
                proxy = (FileSystem)Class.forName(fileSystem).newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
        proxy.setConf(conf);
        
    }

   
    public URI getUri(){
        logMachine("getUri()");
        return proxy.getUri();
    }
   
    public FSDataInputStream open(Path f, int bufferSize) throws IOException{
        logMachine("open(" +  f +  bufferSize + ")");
        return proxy.open(f,bufferSize);
    }
   
    public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        logMachine("create(" + f + "," + permission + "," + overwrite + "," + bufferSize+ "," + replication+ "," + blockSize+ ",progress)");
        return proxy.create(f,permission,overwrite,bufferSize,replication,blockSize,progress);
    }
   
    public FSDataOutputStream append(Path f, int bufferSize) throws IOException{
        logMachine("append(" + f + "," + bufferSize +")");
        return proxy.append(f, bufferSize);
    }

    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException{
        logMachine("append(" + f + "," + bufferSize +",progress)");
        return proxy.append(f,bufferSize,progress);
    }

   
    public boolean rename(Path src, Path dst) throws IOException{
        logMachine("rename(" + src +","+  dst + ")");
        return proxy.rename(src,dst);
    }

   
    public boolean delete(Path f, boolean recursive) throws IOException{
        logMachine("delete(" + f  + (recursive?"true":"false") + ")");
        return proxy.delete(f,recursive);
    }

   
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException{
        logMachine(" listStatus(" + f  + ")");
        return proxy.listStatus(f);
    }

   
    public void setWorkingDirectory(Path new_dir){
        logMachine("setWorkingDirectory(" + new_dir  + ")");
       proxy.setWorkingDirectory(new_dir);
        
    }

   
    public Path getWorkingDirectory(){
        logMachine("getWorkingDirectory()");
      return proxy.getWorkingDirectory();
    }

   
    public boolean mkdirs(Path f, FsPermission permission) throws IOException{
        logMachine("mkdirs(" + f + "," +  permission + ")" );
        return proxy.mkdirs(f,permission);
    }

   
    public FileStatus getFileStatus(Path f) throws IOException{
        logMachine("getFileStatus(" + f  + ")" );
        return proxy.getFileStatus(f);
    }

   
    public void initialize(URI name, Configuration conf) throws IOException{
        logMachine("initialize(" + name  + "," + conf + ")" );
        proxy.initialize(name, conf);
    }

   
    public String getScheme(){
        logMachine("getScheme()" );
        return proxy.getScheme();
    }
   
    public String getCanonicalServiceName(){
        logMachine("getCanonicalServiceName()" );
        return proxy.getCanonicalServiceName();
    }
   
    public String getName(){
        logMachine("getName()" );
        return proxy.getName();
    }
   
    public Path makeQualified(Path path){
        logMachine("makeQualified(" + path + ")" );
        return proxy.makeQualified(path);
    }
   
    public Token<?> getDelegationToken(String renewer) throws IOException{
        logMachine("getDelegationToken(" + renewer + ")" );
        return proxy.getDelegationToken(renewer);
    }

    public Token<?>[] addDelegationTokens(String renewer, Credentials credentials) throws IOException{
        logMachine("addDelegationTokens(" + renewer + "," + credentials +  ")" );
        return proxy.addDelegationTokens(renewer, credentials);
    }

    public FileSystem[] getChildFileSystems(){
        logMachine("getChildFileSystems()" );
        return proxy.getChildFileSystems();
    }

   
    protected void checkPath(Path path){
        logMachine("checkPath(" + path + ")" );
        proxy.checkPath(path);
    }

   
    public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException{
        logMachine("getFileBlockLocations(" + file + "," + start + "," + len + ")" );
        return proxy.getFileBlockLocations(file, start, len);
    }
   
    public BlockLocation[] getFileBlockLocations(Path p, long start, long len) throws IOException{
        logMachine("getFileBlockLocations(" + p + "," + start + "," + len + ")" );
        return proxy.getFileBlockLocations(p, start, len);
    }

   
    public FsServerDefaults getServerDefaults() throws IOException{
        logMachine("getServerDefaults()" );
        return proxy.getServerDefaults();
    }

   
    public FsServerDefaults getServerDefaults(Path p) throws IOException{
        return proxy.getServerDefaults(p);
    }

   
    public Path resolvePath(Path p) throws IOException{
        return proxy.resolvePath(p);
    }

   
    public FSDataInputStream open(Path f) throws IOException{
        return proxy.open(f);
    }

   
    public FSDataOutputStream create(Path f) throws IOException{
        return proxy.create(f);
    }

   
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException{
        return proxy.create(f, overwrite);
    }

   
    public FSDataOutputStream create(Path f, Progressable progress) throws IOException{
        return proxy.create(f, progress);
    }

   
    public FSDataOutputStream create(Path f, short replication) throws IOException{
        return proxy.create(f, replication);
    }

   
    public FSDataOutputStream create(Path f, short replication, Progressable progress) throws IOException{
        return proxy.create(f, replication, progress);
    }

   
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize) throws IOException{
        return proxy.create(f, overwrite, bufferSize);
    }

   
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress) throws IOException{
        return proxy.create(f, overwrite, bufferSize, progress);
    }

   
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize) throws IOException{
        return proxy.create(f, overwrite, bufferSize, replication, blockSize);
    }

   
    public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        return proxy.create(f, overwrite, bufferSize, replication, blockSize, progress);
    }

   
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        return proxy.create(f, permission, flags, bufferSize, replication, blockSize, progress);
    }

   
    public FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt) throws IOException{
        return proxy.create(f, permission, flags, bufferSize, replication, blockSize, progress, checksumOpt);
    }

   
    protected FSDataOutputStream primitiveCreate(Path f, FsPermission absolutePermission, EnumSet<CreateFlag> flag, int bufferSize, short replication, long blockSize, Progressable progress, ChecksumOpt checksumOpt) throws IOException{
        return proxy.primitiveCreate(f, absolutePermission, flag, bufferSize, replication, blockSize, progress, checksumOpt);
    }

   
    protected boolean primitiveMkdir(Path f, FsPermission absolutePermission) throws IOException{
        return proxy.primitiveMkdir(f, absolutePermission);
    }

   
    protected void primitiveMkdir(Path f, FsPermission absolutePermission, boolean createParent) throws IOException{
        proxy.primitiveMkdir(f, absolutePermission, createParent);
    }

   
    public FSDataOutputStream createNonRecursive(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        return proxy.createNonRecursive(f, overwrite, bufferSize, replication, blockSize, progress);
    }

   
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        return proxy.createNonRecursive(f, permission, overwrite, bufferSize, replication, blockSize, progress);
    }

   
    public FSDataOutputStream createNonRecursive(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException{
        return proxy.createNonRecursive(f, permission, flags, bufferSize, replication, blockSize, progress);
    }

   
    public boolean createNewFile(Path f) throws IOException{
        return proxy.createNewFile(f);
    }

   
    public FSDataOutputStream append(Path f) throws IOException{
        return proxy.append(f);
    }

   
    public void concat(Path trg, Path[] psrcs) throws IOException{
        proxy.concat(trg, psrcs);
    }

   
    public short getReplication(Path src) throws IOException{
        return proxy.getReplication(src);
    }

   
    public boolean setReplication(Path src, short replication) throws IOException{
        return proxy.setReplication(src, replication);
    }

   
    protected void rename(Path src, Path dst, Rename... options) throws IOException{
        proxy.rename(src, dst, options);
    }

   
    public boolean delete(Path f) throws IOException{
        return proxy.delete(f);
    }

   
    public boolean deleteOnExit(Path f) throws IOException{
        return proxy.deleteOnExit(f);
    }

   
    public boolean cancelDeleteOnExit(Path f){
        return proxy.cancelDeleteOnExit(f);
    }
   
    protected void processDeleteOnExit(){
        proxy.processDeleteOnExit();
    }

   
    public boolean exists(Path f) throws IOException{
        return proxy.exists(f);
    }

   
    public boolean isDirectory(Path f) throws IOException{
        return proxy.isDirectory(f);
    }

   
    public boolean isFile(Path f) throws IOException{
        return proxy.isFile(f);
    }

   
    public long getLength(Path f) throws IOException{
        return proxy.getLength(f);
    }

   
    public ContentSummary getContentSummary(Path f) throws IOException{
        return proxy.getContentSummary(f);
    }

   
    public RemoteIterator<Path> listCorruptFileBlocks(Path path) throws IOException{
        return proxy.listCorruptFileBlocks(path);
    }

   
    public FileStatus[] listStatus(Path f, PathFilter filter) throws FileNotFoundException, IOException{
        return proxy.listStatus(f, filter);
    }

   
    public FileStatus[] listStatus(Path[] files) throws FileNotFoundException, IOException{
        return proxy.listStatus(files);
    }

   
    public FileStatus[] listStatus(Path[] files, PathFilter filter) throws FileNotFoundException, IOException{
        return proxy.listStatus(files, filter);
    }

   
    public FileStatus[] globStatus(Path pathPattern) throws IOException{
        return proxy.globStatus(pathPattern);
    }

   
    public FileStatus[] globStatus(Path pathPattern, PathFilter filter) throws IOException{
        return proxy.globStatus(pathPattern, filter);
    }

   
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f) throws FileNotFoundException, IOException{
        return proxy.listLocatedStatus(f);
    }

   
    protected RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f, PathFilter filter) throws FileNotFoundException, IOException{
        return proxy.listLocatedStatus(f, filter);
    }

   
    public RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive) throws FileNotFoundException, IOException{
        return proxy.listFiles(f, recursive);
    }

   
    public Path getHomeDirectory(){
        return proxy.getHomeDirectory();
    }

   
    protected Path getInitialWorkingDirectory(){
        return proxy.getInitialWorkingDirectory();
    }

   
    public boolean mkdirs(Path f) throws IOException{
        return proxy.mkdirs(f);
    }

   
    public void copyFromLocalFile(Path src, Path dst) throws IOException{
        proxy.copyFromLocalFile(src, dst);
    }

   
    public void moveFromLocalFile(Path[] srcs, Path dst) throws IOException{
        proxy.moveFromLocalFile(srcs, dst);
    }

   
    public void moveFromLocalFile(Path src, Path dst) throws IOException{
        proxy.moveFromLocalFile(src, dst);
    }

    public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException{
        proxy.copyFromLocalFile(delSrc, src, dst);
    }

   
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException{
        proxy.copyFromLocalFile(delSrc, overwrite, srcs, dst);
    }

   
    public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException{
        proxy.copyFromLocalFile(delSrc, overwrite, src, dst);
    }

   
    public void copyToLocalFile(Path src, Path dst) throws IOException{
        proxy.copyToLocalFile(src, dst);
    }

   
    public void moveToLocalFile(Path src, Path dst) throws IOException{
        proxy.moveToLocalFile(src, dst);
    }

   
    public void copyToLocalFile(boolean delSrc, Path src, Path dst) throws IOException{
        proxy.copyToLocalFile(delSrc, src, dst);
    }

   
    public void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem) throws IOException{
        proxy.copyToLocalFile(delSrc, src, dst, useRawLocalFileSystem);
    }

   
    public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException{
        return proxy.startLocalOutput(fsOutputFile, tmpLocalFile);
    }

   
    public void completeLocalOutput(Path fsOutputFile, Path tmpLocalFile) throws IOException{
        proxy.completeLocalOutput(fsOutputFile, tmpLocalFile);
    }

   
    public void close() throws IOException{
        proxy.close();
    }

   
    public long getUsed() throws IOException{
        return proxy.getUsed();
    }

   
    public long getBlockSize(Path f) throws IOException{
        return proxy.getBlockSize(f);
    }

   
    public long getDefaultBlockSize(){
        return proxy.getDefaultBlockSize();
    }

   
    public long getDefaultBlockSize(Path f){
        return proxy.getDefaultBlockSize(f);
    }

   
    public short getDefaultReplication(){
        return proxy.getDefaultReplication();
    }

   
    public short getDefaultReplication(Path path){
        return proxy.getDefaultReplication(path);
    }

   
    public FileChecksum getFileChecksum(Path f) throws IOException{
        return proxy.getFileChecksum(f);
    }

   
    public void setVerifyChecksum(boolean verifyChecksum){
        proxy.setVerifyChecksum(verifyChecksum);
    }

   
    public void setWriteChecksum(boolean writeChecksum){
        proxy.setWriteChecksum(writeChecksum);
    }

   
    public FsStatus getStatus() throws IOException{
        return proxy.getStatus();
    }

   
    public FsStatus getStatus(Path p) throws IOException{
        return proxy.getStatus(p);
    }

   
    public void setPermission(Path p, FsPermission permission) throws IOException{
     
        proxy.setPermission(p, permission);
    }

   
    public void setOwner(Path p, String username, String groupname) throws IOException{
     
        proxy.setOwner(p, username, groupname);
    }

   
    public void setTimes(Path p, long mtime, long atime) throws IOException{
     
        proxy.setTimes(p, mtime, atime);
    }

   
    public Configuration getConf(){
     
        return proxy.getConf();
    }

}
