/**
 *
 * Copyright (c) 2014 Gluster, Inc. <http://www.gluster.com>
 * This file is part of GlusterFS.
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package org.apache.hadoop.fs.test.unit;

import static org.apache.hadoop.fs.FileSystemTestHelper.getTestRootPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorInterface;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for HCFS classes.
 * 
 */
public class HcfsUmaskTest{
    
    static FileSystem fs ; 
    
    @BeforeClass
    public static void setup() throws Exception {
    	HcfsTestConnectorInterface connector = HcfsTestConnectorFactory.getHcfsTestConnector();
        fs= connector.create();
    }
    
    @AfterClass
    public static void after() throws IOException{
        fs.close();
    }

    @After
    public void tearDown() throws Exception {
        fs.delete(getTestRootPath(fs, "test"),true);
    }
    
    @org.junit.Test
    public void testMkdirsWithUmask() throws Exception {
        Configuration conf = fs.getConf();
        String oldUmask = conf.get("fs.permissions.umask-mode");
        Path dir = new Path("dirUmask022");
        conf.set("fs.permissions.umask-mode", "022");
        assertTrue(fs.mkdirs(dir));
        conf.set("fs.permissions.umask-mode", oldUmask);
        FileStatus status = fs.getFileStatus(dir);
        assertTrue(status.isDirectory());
        assertEquals((short)0755, status.getPermission().toShort());

    } 
}
