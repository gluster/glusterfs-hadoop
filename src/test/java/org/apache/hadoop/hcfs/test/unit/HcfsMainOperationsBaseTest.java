package org.apache.hadoop.hcfs.test.unit;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSMainOperationsBaseTest;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hcfs.test.connector.HcfsTestConnectorFactory;
import org.apache.hadoop.hcfs.test.connector.HcfsTestConnectorInterface;
import org.apache.tools.ant.util.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class HcfsMainOperationsBaseTest extends FSMainOperationsBaseTest {

  @Before
  public void setUp() throws Exception {
	HcfsTestConnectorInterface connector = HcfsTestConnectorFactory.getHcfsTestConnector();  
    fSys = connector.create();
    super.setUp();
  }

  protected Path getDefaultWorkingDirectory() throws IOException {
      return fSys.getWorkingDirectory();
  }

  @After
  public void tearDown() throws Exception {
      fSys.close();
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
