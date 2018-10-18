package org.apache.hadoop.fs.test.unit;

import static org.apache.hadoop.fs.FileSystemTestHelper.*;

import org.apache.hadoop.fs.FileSystemTestHelper;

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorInterface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HcfsMainOperationsBaseTest extends org.apache.hadoop.fs.FSMainOperationsBaseTest {

  @Before
  public void setUp() throws Exception {
	HcfsTestConnectorInterface connector = HcfsTestConnectorFactory.getHcfsTestConnector();  
    fSys = connector.create();
    super.setUp();
  }

  @After
  public void tearDown() throws Exception {
	  fSys.delete(getTestRootPath(fSys, "test"),true);
  }
  
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    Assert.assertFalse(exists(fSys, testDir));
    fSys.mkdirs(testDir);
    Assert.assertTrue(exists(fSys, testDir));
    
    createFile(getTestRootPath(fSys, "test/hadoop/file"));
    
    Path testSubDir = getTestRootPath(fSys, "test/hadoop/file/subdir");

    boolean thrown = false;
    try{
      fSys.mkdirs(testSubDir);
    }catch(Exception ex){
      thrown = true;
      assertParentNotDirectoryException(ex);
    }
    Assert.assertTrue(thrown);
    
    Assert.assertFalse(exists(fSys, testSubDir));
    
    Path testDeepSubDir = getTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
    Assert.assertFalse(exists(fSys, testSubDir));
    thrown = false;
    try{
    	Assert.assertFalse(fSys.mkdirs(testDeepSubDir));
    }catch(Exception ex){
      thrown = true;
      assertParentNotDirectoryException(ex);
    }
    Assert.assertTrue(thrown);
    Assert.assertFalse(exists(fSys, testDeepSubDir));
    
  }
  
  @Test
  public void testWDAbsolute() throws IOException {
    Path absoluteDir = getTestRootPath(fSys,
        "test/existingDir");
    fSys.mkdirs(absoluteDir);
    fSys.setWorkingDirectory(absoluteDir);
    Assert.assertEquals(absoluteDir, fSys.getWorkingDirectory());
  }
  
  @Test
  public void testGlobStatusThrowsExceptionForNonExistentFile() throws Exception {
    try {
      // This should throw a FileNotFoundException
      fSys.globStatus(getTestRootPath(fSys, "test/hadoopfsdf/?"));
      /* the API doesn't mention 'FileNotFoundException'.  Instead it says empty array or null for return
       */
      // Assert.fail("Should throw FileNotFoundException");
    } catch (FileNotFoundException fnfe) {
      // expected
    }
  }
  
  
  
  @Test
  public void testDeleteNonExistentFile() throws IOException {
    Path path = getTestRootPath(fSys, "test/hadoop/file");    
    Assert.assertFalse("Doesn't exist", exists(fSys, path));
    Assert.assertFalse("No deletion", fSys.delete(path, true));
  }
  
  protected Path getDefaultWorkingDirectory() throws IOException {
      return fSys.getWorkingDirectory();
  }


  private void assertParentNotDirectoryException(Exception ex) {
    // Hadoop <=2.5 uses FileAlreadyExistsException (HADOOP-9361)
    // Hadoop >=2.6 uses ParentNotDirectoryException
    String errorMsg = "expected FileAlreadyExistsException or ParentNotDirectoryException, got " + ex.getClass().getName();
    Assert.assertTrue(
            errorMsg,
            "org.apache.hadoop.fs.FileAlreadyExistsException".equals(ex.getClass().getName())
                    || "org.apache.hadoop.fs.ParentNotDirectoryException".equals(ex.getClass().getName()));
  }

}
