package org.gluster.test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.glusterfs.GlusterFileSystem;
import org.junit.BeforeClass;

/**
 * A file system specifically for testing.
 */
public class GlusterFileSystemForTest extends GlusterFileSystem{

    
    public static Configuration initializeConfig(File mount) throws Exception{
        String glusterVolume=System.getProperty("gluster-volume");
        String glusterHost=System.getProperty("gluster-host");
        final Configuration conf=new Configuration();
        /*
         * the user can over ride the default gluster volume used for test with
         * ENV var
         */
        glusterVolume=System.getProperty("GLUSTER_VOLUME");
        if(glusterVolume==null||glusterVolume.equals("")){
            System.out.println("WARNING: HOST NOT DEFINED IN ENVIRONMENT! See README");
            glusterVolume="HadoopVol";
        }

        glusterHost=System.getProperty("GLUSTER_HOST");
        if(glusterHost==null||glusterHost.equals("")){
            System.out.println("WARNING: HOST NOT DEFINED IN ENVIRONMENT! See README");
            InetAddress addr=InetAddress.getLocalHost();
            glusterHost=addr.getHostName();
        }

        System.out.println("Testing against host="+glusterHost);
        System.out.println("Testing against volume="+glusterVolume);

        if(glusterVolume==null){
            glusterVolume="hadoop-gluster";
        }

        /* retrieve the local machines hostname */
        if(glusterHost==null||"".compareTo(glusterHost)==0){
            InetAddress addr=null;

            addr=InetAddress.getLocalHost();

            glusterHost=addr.getHostName();
        }

        System.out.println("Confirmed that configuration properties from gluster were found , now creating dirs");
        //System.out.println("Now initializing GlusterFS ! - We will mount to "+mount.getAbsolutePath());
        //setup the configuration object.
        conf.set("fs.glusterfs.volname", glusterVolume);
        conf.set("fs.glusterfs.mount", mount.getAbsolutePath());
        conf.set("fs.glusterfs.server", glusterHost);
        conf.set("fs.default.name", "glusterfs://"+glusterHost+":9000");
        
        return conf;
    }
    
    boolean tempCreated = false;
    public File getTempDirectory(){
        final File tempDirectory=new File(System.getProperty("java.io.tmpdir"), "gluster-test-mount-point");

        //initialize?
        if(!tempCreated){
            tempDirectory.mkdirs();
            tempDirectory.delete();
            tempDirectory.mkdir();
        }
        tempCreated=true;
        return tempDirectory;
    }
    
    public File initializeMounts(File tempDirectory) throws Exception {
        File mount=new File(tempDirectory, "mount");
        mount.mkdir();
        return mount;
    }
    
    public GlusterFileSystemForTest( ) throws Exception{
        super();
        
        //Setup the temp directory.  
        final File mount = this.initializeMounts(getTempDirectory());
        
        //Now set up the config object
        final Configuration conf = this.initializeConfig(mount);
        
        //Initialize GlusterFileSystem
        super.initialize(getTempDirectory().toURI(), conf);

        System.out.println("server "+conf.get("fs.glusterfs.server"));

        //FYI, we fs.default.name is something like "glusterfs://127.0.0.1:9000") 
        super.initialize(new URI(conf.get("fs.default.name")), conf);
    }

}
