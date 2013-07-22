package org.apache.hadoop.fs.local;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.glusterfs.GlusterFileSystem;
import org.apache.hadoop.fs.glusterfs.GlusterVolume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlusterVol extends RawLocalFsG{
    
    protected String glusterMount = null;
    protected static final Logger log = LoggerFactory.getLogger(GlusterFileSystem.class);
    
    
    GlusterVol(final Configuration conf) throws IOException, URISyntaxException {
        this(GlusterVolume.NAME, conf);
        
    }
      
      /**
       * This constructor has the signature needed by
       * {@link AbstractFileSystem#createFileSystem(URI, Configuration)}.
       * 
       * @param theUri which must be that of localFs
       * @param conf
       * @throws IOException
       * @throws URISyntaxException 
       */
    GlusterVol(final URI theUri, final Configuration conf) throws IOException, URISyntaxException {
        super(theUri, new GlusterVolume(), conf, false);
    }
    
    
    
    

}
