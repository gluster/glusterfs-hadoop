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
      File out = new File("/tmp/testGlusterFuseOutputStream"+System.currentTimeMillis());
      out.createNewFile();
      //Create a 3 byte FuseOutputStream
      final GlusterFUSEOutputStream stream = new GlusterFUSEOutputStream(out.getAbsolutePath(), true, 3);
      
      stream.write("ab".getBytes());
      System.out.println(out.length());

      Assert.assertEquals(0, out.length());
      
      stream.flush();
      stream.close();
      
      System.out.println(out.length());
      //Confirm that the buffer held 100 bytes.
      Assert.assertEquals(2, out.length());
      out.delete();
    }
}
