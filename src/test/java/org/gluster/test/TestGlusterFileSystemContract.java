/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.gluster.test;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.slf4j.LoggerFactory;

/**
 * This is the full filesystem contract test -which requires the
 * Default config set up to point to a filesystem
 */
public class TestGlusterFileSystemContract
  extends FileSystemContractBaseTest {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TestGlusterFileSystemContract.class);

  @Override
  protected void setUp() throws Exception{
      fs=GFSUtil.create(true);
      super.setUp();
  }

    /**
     * junit.framework.AssertionFailedError: Rename result expected:<false> but was:<true>
     */
    @Override
    public void testRenameFileAsExistingFile() throws Exception{
        super.testRenameFileAsExistingFile();
    }

    /**
     * java.lang.RuntimeException: org.apache.hadoop.util.Shell$ExitCodeException: chmod: cannot access `/tmp/gluster-test-mount-point/mount/test/hadoop/file/subdir': Not a directory
     */
    @Override
    public void testMkdirsFailsForSubdirectoryOfExistingFile() throws Exception{
        super.testMkdirsFailsForSubdirectoryOfExistingFile();
    }

    /**
     * java.io.IOException: Stream closed.
     */
    @Override
    public void testOutputStreamClosedTwice() throws IOException{
        super.testOutputStreamClosedTwice();
    }
 
    /**
     * junit.framework.AssertionFailedError: Rename result expected:<false> but was:<true>  
     */
    @Override
    public void testListStatusThrowsExceptionForNonExistentFile() throws Exception{
        super.testListStatusThrowsExceptionForNonExistentFile();
    }

    /**
     * junit.framework.AssertionFailedError: expected:<file://null/user/root> but was:</tmp/gluster-test-mount-point/mount>
     */
    @Override
    public void testWorkingDirectory() throws Exception{
        super.testWorkingDirectory();
    }

    /**
     * AssertionFailedError: null
     */
    @Override
    public void testMkdirs() throws Exception{
        super.testMkdirs();
    }

    /**
    java.lang.NoSuchMethodError: org.apache.hadoop.fs.FileSystem.getStatus()Lorg/apache/hadoop/fs/FsStatus;
     */
    @Override
    public void testFsStatus() throws Exception{
        super.testFsStatus();
    }
    
}
