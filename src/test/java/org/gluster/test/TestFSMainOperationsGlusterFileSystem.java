package org.gluster.test;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.tools.ant.util.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class TestFSMainOperationsGlusterFileSystem extends FSMainOperationsBaseTest {

  @Before
  public void setUp() throws Exception {
    fSys =GFSUtil.create(true);
    super.setUp();
  }

  static Path wd = null;
  protected Path getDefaultWorkingDirectory() throws IOException {
    if (wd == null)
      wd = FileSystem.getLocal(new Configuration()).getWorkingDirectory();
    return wd;
  }

  @After
  public void tearDown() throws Exception {
      fSys.close();
      FileUtils.delete(GFSUtil.getTempDirectory());
  }

  @Test
  @Override
  public void testWDAbsolute() throws IOException {
    Path absoluteDir = FileSystemTestHelper.getTestRootPath(fSys,
        "test/existingDir");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(absoluteDir, fSys.getWorkingDirectory());
  }
}
