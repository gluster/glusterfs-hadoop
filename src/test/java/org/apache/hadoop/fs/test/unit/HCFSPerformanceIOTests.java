package org.apache.hadoop.fs.test.unit;


import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.glusterfs.GlusterVolume;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorInterface;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  A class for performance and IO related unit tests.
 * 
 * - Write buffering
 * - Read buffering
 * - Object caching / File lookup caching.
 * - Seeking
 */
public class HCFSPerformanceIOTests {
    
    static FileSystem fs ; 
    Logger log = LoggerFactory.getLogger(HCFSPerformanceIOTests.class);
    
    @BeforeClass
    public static void setup() throws Exception {
    	HcfsTestConnectorInterface connector = HcfsTestConnectorFactory.getHcfsTestConnector();
        fs= connector.create();
    }
    
    @AfterClass
    public static void after() throws IOException{
        fs.close();
    }

    public Path bufferoutpath(){
        return new Path("/tmp/buffering_test"+HCFSPerformanceIOTests.class.getName());
    }

    @After
    public void tearDown() throws Exception {
  	  fs.delete(bufferoutpath(),true);
    }

    //String to append to file we are writing.
    static final String CONTENT="1234";

    /**
     * This is a complex test.  It documents the expected behaviour of the 
     * FileSystem buffering.  
     * 
     * It assumes that the configuration value of FS is == the {@link GlusterVolume} OVERRIDE_WRITE_BUFFER_SIZE.
     * Then, it starts writing to a stream.  
     */
    @Test
    public void testBufferSpill() throws Exception {
        
        /**
         * Sanity check: This test expects that an override is being performed, i.e., that
         * the buffering is going to be set to the optimal size, because the file system 
         * detected that the configured original buffer size was == to the "bad default" value which 
         * we have decide to override, for the sack of "reasonable defaults" out of the box.
         */
        Assert.assertEquals(
                GlusterVolume.OPTIMAL_WRITE_BUFFER_SIZE, 
                fs.getConf().getInt("io.file.buffer.size",-1));
        
        FSDataOutputStream os = fs.create(bufferoutpath());
        
        int written=0;
        
        /**
         * Now, we assert that no data is spilled to disk until we reach the optimal size.
         */
        while(written < GlusterVolume.OPTIMAL_WRITE_BUFFER_SIZE){
            os.write(CONTENT.getBytes());
            written+=CONTENT.getBytes().length;
            Assert.assertTrue("asserting that file not written yet...",fs.getLength(bufferoutpath())==0);
        }
        os.flush();
        
        Assert.assertTrue("asserting that is now written... ",fs.getLength(bufferoutpath()) >= GlusterVolume.OPTIMAL_WRITE_BUFFER_SIZE);

        os.close();
    }
}