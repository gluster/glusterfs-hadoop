package org.gluster.test;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import junit.framework.Assert;

import org.apache.hadoop.fs.glusterfs.GlusterFSBrickClass;
import org.apache.hadoop.fs.glusterfs.GlusterFUSEInputStream;
import org.apache.hadoop.fs.glusterfs.GlusterFUSEOutputStream;
import org.junit.Test;

public class TestGlusterFuseOutputStream {

	@Test
    public void testOutputStream() throws IOException{
      File out = new File("/tmp/testGlusterFuseOutputStream");
      
      //Create a 3 byte FuseOutputStream
      final GlusterFUSEOutputStream stream = new GlusterFUSEOutputStream(out.getAbsolutePath(), true, 3);
      
      stream.write("ab".getBytes());
      
      long sizeBeforeFlush = out.length();
      
      stream.flush();
      stream.close();
      
      //Confirm that the buffer held 100 bytes.
      Assert.assertTrue(out.length() > sizeBeforeFlush);
      out.delete();
    }
}
