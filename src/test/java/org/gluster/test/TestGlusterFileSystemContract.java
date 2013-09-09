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

import java.util.Arrays;
import java.util.List;

import junit.framework.TestResult;

import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the full filesystem contract test -which requires the Default config
 * set up to point to a filesystem
 */
public class TestGlusterFileSystemContract extends FileSystemContractBaseTest {
    static Logger log = LoggerFactory.getLogger(TestGlusterFileSystemContract.class);
    /**
     * Ignore at runtime: This is to get around an issue with 
     * the @Ignore annotation which doesnt cascade upwards for 
     * TestCases.
     */
    static final List<String> IGNORE=
            Arrays.asList(
              //"testWorkingDirectory",
              "testRenameNonExistentPath",
              "testRenameFileMoveToNonExistentDirectory",
              "testRenameFileAsExistingFile",
              "testWorkingDirectory",
              "testListStatus",
              "testRenameDirectoryMoveToNonExistentDirectory",
              "testRenameDirectoryAsExistingDirectory",
              "testRenameDirectoryAsNonExistentDirectory"
            );
    
  
    

    public void run(TestResult result) {
        //We ignore the tests above.  the @Ignore annotation doesnt
        //work for TestCase descendents.  And thus, to keep 
        //we have a custom run implementation here. 
        if (IGNORE.contains(getName()))
            log.warn("SKIPPING: " +getName());
        else
            super.run(result);
    }

    private static final org.slf4j.Logger LOG = LoggerFactory
            .getLogger(TestGlusterFileSystemContract.class);

    public void fails() {
        fail();
    }

    public void succeeds() {
    }
    
    @Override
    protected void setUp() throws Exception {
        fs = GFSUtil.create(true);
        super.setUp();
    }
    
}
