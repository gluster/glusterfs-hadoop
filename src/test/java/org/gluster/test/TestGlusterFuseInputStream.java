package org.gluster.test;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.fs.glusterfs.GlusterFSBrickClass;
import org.apache.hadoop.fs.glusterfs.GlusterFUSEInputStream;
import org.junit.Test;

public class TestGlusterFuseInputStream{

    @Test
    public void testDoubleClose() throws IOException{
        /**
         * GlusterFUSEInputStream gfi= new GlusterFUSEInputStream ( new
         * File(""), null, "localhost") ; gfi.close(); gfi.close();
         **/
    }

    @Test
    public void testDoubleClose2() throws IOException{
        /**
         * GlusterFUSEInputStream gfi= new GlusterFUSEInputStream ( new
         * File(""), null, "localhost") ; gfi.close(); gfi.close();
         **/
    }
}
