package org.gluster.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.TestGetFileBlockLocations;

public class TestGlusterFileBlockLocations extends TestGetFileBlockLocations{

    @Override
    protected void setUp() throws IOException{
        try{
            Configuration cfg = GFSUtil.createConfiguration(true);
            cfg.writeXml(new FileOutputStream(new File("/tmp/core-site.xml")));
            Configuration.addDefaultResource("/tmp/core-site.xml");
            cfg.writeXml(System.out);
        }
        catch(Exception e)
        {
            throw new IOException(e);
        }
        super.setUp();
    }
}