package org.apache.hadoop.fs.test.unit;

import static org.apache.hadoop.fs.FileSystemTestHelper.getTestRootPath;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorInterface;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HcfsTestBufferingOperations{

    FileSystem fSys;

    @Before
    public void setUp() throws Exception{
        HcfsTestConnectorInterface connector=
                HcfsTestConnectorFactory.getHcfsTestConnector();
        fSys=connector.create();
    }

    @Test
    public void test() throws Exception {
        Path out = new Path("a");
        
        System.out.println("Writing file to: "+out.makeQualified(fSys));
        
        FSDataOutputStream os = fSys.create(out);
        
        int written=0;
        /**
         * Assert that writes smaller than 10KB are NOT spilled to disk
         */
        while(written<10000){
            os.write("ASDF".getBytes());
            written+="ASDF".getBytes().length;
            //now, we expect
            System.out.println("Checking if "+ written +" bytes were spilled yet...");
            Assert.assertTrue("asserting that file not written yet",fSys.getLength(out)==0);
        }
        os.flush();
        Assert.assertTrue("asserting that file not written yet",fSys.getLength(out)>=10000);

        os.close();
    }
    
    @After
    public void tearDown() throws Exception{
        fSys.delete(getTestRootPath(fSys, "test"), true);
    }
}
