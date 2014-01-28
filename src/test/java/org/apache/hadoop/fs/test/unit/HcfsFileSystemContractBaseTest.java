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

package org.apache.hadoop.fs.test.unit;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorInterface;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * This is the full filesystem contract test -which requires the
 * Default config set up to point to a filesystem
 */
public class HcfsFileSystemContractBaseTest
  extends org.apache.hadoop.fs.FileSystemContractBaseTest {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(HcfsFileSystemContractBaseTest.class);

    /**
     * We ignore this test ... it conflicts the results expected by newer
     * FSMainOperations tests, which throw exceptions.
     */
    @Ignore
    @Test
    @Override
    public void testRenameNonExistentPath() throws Exception{

    }
 
    @Ignore
    @Override
    public void testRenameFileMoveToNonExistentDirectory() throws Exception{
    
    }

    @Ignore
    @Override
    public void testRenameDirectoryMoveToNonExistentDirectory() throws Exception{
    
    }

    protected void setUp() throws Exception{
        HcfsTestConnectorInterface connector=HcfsTestConnectorFactory.getHcfsTestConnector();
        fs=connector.create();
        super.setUp();
    }

    public static FileStatus containsPath(Path path,FileStatus[] dirList) throws IOException{
        for(int i=0;i<dirList.length;i++){
            if(path.equals(dirList[i].getPath())){
                return dirList[i];
            }
        }
        return null;
    }

    public void testListStatusReturnsNullForNonExistentFile() throws Exception{
        try{
            fs.listStatus(path("/test/hadoop/file"));
            fail("Should throw FileNotFoundException");
        }catch (FileNotFoundException ex){
            // exception thrown for non-existent file
        }
    }
  
  public void testListStatusThrowsExceptionForNonExistentFile() throws Exception {
	    try {
	      fs.listStatus(path("/test/hadoop/file"));
	      fail("Should throw FileNotFoundException");
	    } catch (FileNotFoundException fnfe) {
	      // expected
	    }
 }
  
  public void testFsStatus() throws Exception {
	    FsStatus fsStatus = fs.getStatus();
	    assertNotNull(fsStatus);
	    //used, free and capacity are non-negative longs
	    assertTrue(fsStatus.getUsed() >= 0);
	    assertTrue(fsStatus.getRemaining() >= 0);
	    assertTrue(fsStatus.getCapacity() >= 0);
  }
	  
}
