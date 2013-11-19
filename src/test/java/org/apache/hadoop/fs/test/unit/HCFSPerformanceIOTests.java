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
 *  A class for performance and IO related unit tests:
 * 
 * - Write buffering
 * - Read buffering
 * - Object caching / File lookup caching.
 * - Seeking
 */
public class HCFSPerformanceIOTests {
    
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
    
    @Test
    public void testBufferSpill() throws Exception {
        Path out = new Path("a");
        final Integer buffersize = fs.getConf().getInt("io.file.buffer.size", -1);
        FSDataOutputStream os = fs.create(out);
        
        int written=0;
        /**
         * Assert that writes smaller than 10KB are NOT spilled to disk
         */
        while(written<buffersize){
            os.write("ASDF".getBytes());
            written+="ASDF".getBytes().length;
            //now, we expect
            Assert.assertTrue("asserting that file not written yet...",fs.getLength(out)==0);
        }
        os.flush();
        Assert.assertTrue("asserting that is now written... ",fs.getLength(out)>=buffersize);

        os.close();
        fs.delete(out);
    }
}