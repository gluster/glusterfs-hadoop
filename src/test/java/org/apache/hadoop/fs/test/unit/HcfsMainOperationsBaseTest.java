package org.apache.hadoop.fs.test.unit;


import static org.apache.hadoop.fs.FileSystemTestHelper.containsPath;
import static org.apache.hadoop.fs.FileSystemTestHelper.exists;

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorInterface;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.log.Log;

public class HcfsMainOperationsBaseTest extends org.apache.hadoop.fs.FSMainOperationsBaseTest {

  @Override
  protected FileSystem createFileSystem() throws Exception{
      HcfsTestConnectorInterface connector = HcfsTestConnectorFactory.getHcfsTestConnector();  
      return connector.create();
  }

  @After
  public void tearDown() throws Exception {
	  fSys.delete(getTestRootPath(fSys, "test"),true);
  }


  @SuppressWarnings("deprecation")
  private void rename(Path src, Path dst, boolean renameShouldSucceed,
      boolean srcExists, boolean dstExists, Rename... options)
      throws IOException {
    fSys.rename(src, dst);
    if (!renameShouldSucceed)
      Assert.fail("rename should have thrown exception");
    Assert.assertEquals("Source exists", srcExists, exists(fSys, src));
    Assert.assertEquals("Destination exists", dstExists, exists(fSys, dst));
  }
  
  @Test
  public void testRenameNonExistentPath() throws Exception {
    if (!renameSupported()) return;
    Path src = getTestRootPath(fSys, "test/hadoop/nonExistent");
    Path dst = getTestRootPath(fSys, "test/new/newpath");
    try {
      rename(src, dst, false, false, false, Rename.NONE);
      Assert.fail("Should throw FileNotFoundException");
    } catch (IOException e) {
      Assert.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }

    try {
      rename(src, dst, false, false, false, Rename.OVERWRITE);
      Assert.fail("Should throw FileNotFoundException");
    } catch (IOException e) {
      Assert.assertTrue(unwrapException(e) instanceof FileNotFoundException);
    }
  }
  
  @Test
  public void testListStatusThrowsExceptionForNonExistentFile()
  throws Exception {
    try {
      fSys.listStatus(getTestRootPath(fSys, "test/hadoop/file"));
      Assert.fail("Should throw FileNotFoundException");
    } 
    catch (FileNotFoundException fnfe) {
      // expected
    }
    catch (Exception e){
        e.printStackTrace();
        Assert.fail("Wrong exception " + e.getClass().getName());
    }
  }
  
  
  // TODO: update after fixing HADOOP-7352
  @Test
  public void testListStatusThrowsExceptionForUnreadableDir()
  throws Exception {
     
    Path testRootDir = getTestRootPath(fSys, "test/hadoop/dir");
    Path obscuredDir = new Path(testRootDir, "foo");
    Path subDir = new Path(obscuredDir, "bar"); //so foo is non-empty
    fSys.mkdirs(subDir);
    fSys.setPermission(obscuredDir, new FsPermission((short)0)); //no access
    System.out.println("Set perms on " + obscuredDir + " ...");
    System.out.println(fSys.getFileStatus(obscuredDir).getPermission().toShort());
    Thread.sleep(10000);
    try {
      fSys.listStatus(obscuredDir);
      Assert.fail(fSys.getClass().getName()+" Should throw IOException, instead it worked !");
    } catch (IOException ioe) {
      // expected
    } finally {
      // make sure the test directory can be deleted
      fSys.setPermission(obscuredDir, new FsPermission((short)0755)); //default
    }
  }
  
  @Test
  public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception {
    Path testDir = getTestRootPath(fSys, "test/hadoop");
    Assert.assertFalse(exists(fSys, testDir));
    fSys.mkdirs(testDir);
    Assert.assertTrue(exists(fSys, testDir));
    
    createFile(getTestRootPath(fSys, "test/hadoop/file"));
    
    Path testSubDir = getTestRootPath(fSys, "test/hadoop/file/subdir");
    
    try{
    	Assert.assertFalse(fSys.mkdirs(testSubDir));
    }catch(FileAlreadyExistsException ex){
    	// catch exception as expected.
    }
    
    Assert.assertFalse(exists(fSys, testSubDir));
    
    Path testDeepSubDir = getTestRootPath(fSys, "test/hadoop/file/deep/sub/dir");
    Assert.assertFalse(exists(fSys, testSubDir));
    try{
    	Assert.assertFalse(fSys.mkdirs(testDeepSubDir));
    }catch(FileAlreadyExistsException ex){
    	// catch exception as expected.
    }
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



}
