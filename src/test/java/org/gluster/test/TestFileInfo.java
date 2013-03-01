package org.gluster.test;

import java.io.File;

import junit.framework.Assert;

import org.apache.hadoop.fs.glusterfs.FileInfoUtil;
import org.apache.tools.ant.types.FlexInteger;
import org.junit.Test;

public class TestFileInfo {

	/**
	 * This is a unit test of the FileInfoUtils ability 
	 * to read permissions from POSIX.  
	 * It is not a test of gluster. 
	 * For a test of the GlusterFileSystem, see 
	 * {@link TestGluster}
	 */
	@Test
	public void testPosix() throws Exception {
		String user=System.getProperties().getProperty("user.name");
		File f = File.createTempFile("tempjunit", ".tmp");
		String owner=FileInfoUtil.getLSinfo(f.getAbsolutePath()).get("owner");
		System.out.println("Confirming -- \nuser.name(" + user +")=owner("+owner+")");
		Assert.assertEquals(user,owner);
	}

}

