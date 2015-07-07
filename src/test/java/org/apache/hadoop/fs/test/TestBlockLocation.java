package org.apache.hadoop.fs.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.glusterfs.GlusterFSPathInfo;
import org.apache.hadoop.fs.glusterfs.GlusterFSXattr;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorInterface;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBlockLocation {

        FileStatus fs = new FileStatus(100000,false,0,0,0,new Path("/a"));

	@Test
	public void testDistributedVolume() throws IOException {
		String xattr = "trusted.glusterfs.pathinfo=\"(<DISTRIBUTE:distributed-dht> <POSIX(/mnt/brick1):vm-2:/mnt/brick1/a>)\"";

                GlusterFSPathInfo.clear();
                GlusterFSPathInfo gpi = GlusterFSPathInfo.get(fs.getPath().toString(),xattr);
		BlockLocation[] blocks = gpi.getBlockLocations(10000, 20000);
		assertTrue(blocks.length == 1);
		assertTrue(blocks[0].getHosts()[0].equals("vm-2"));
	}

	@Test
	public void testStripeVolume() throws IOException {
		String xattr = "trusted.glusterfs.pathinfo=\"(<DISTRIBUTE:StripeVolume-dht> (<STRIPE:StripeVolume-stripe-0:[131072]> <POSIX(/mnt/brick1/stripeVol):vm-1:/mnt/brick1/stripeVol/a>))\"";

                GlusterFSPathInfo.clear();
                GlusterFSPathInfo gpi = GlusterFSPathInfo.get(fs.getPath().toString(),xattr);
		BlockLocation[] blocks = gpi.getBlockLocations(0, 10000);
		assertTrue(blocks.length == 1);
		assertTrue(blocks[0].getHosts()[0].equals("vm-1"));
	}

	@Test
	public void testReplicateVolume() throws IOException {
		String xattr = "trusted.glusterfs.pathinfo=\"(<DISTRIBUTE:HadoopVol-dht> (<REPLICATE:HadoopVol-replicate-0> <POSIX(/mnt/brick1/HadoopVol):vm-2:/mnt/brick1/HadoopVol/tmp/a> <POSIX(/mnt/brick1/HadoopVol):vm-1:/mnt/brick1/HadoopVol/tmp/a>))\"";
                GlusterFSPathInfo.clear();
                GlusterFSPathInfo gpi = GlusterFSPathInfo.get(fs.getPath().toString(),xattr);
                BlockLocation[] blocks = gpi.getBlockLocations(0, 10000);
		
		assertTrue(blocks.length == 1);
		String hosts[] = blocks[0].getHosts();
		
		assertTrue(hosts[0].equals("vm-2"));
		assertTrue(hosts[1].equals("vm-1"));
	}

	/*
	 * Make sure the xattr code fails gracefull when the input is garbage.
	 */
	@Test
	public void testFailureVolumeGarbage() throws IOException {
		String xattr = "dsfsffsfdsf";
                GlusterFSPathInfo.clear();
                GlusterFSPathInfo gpi = GlusterFSPathInfo.get(fs.getPath().toString(),xattr);
                BlockLocation[] blocks = gpi.getBlockLocations(0, 10000);
		assertNull(blocks);
	}

	/*
	 * Make sure the xattr code fails gracefully when the input is empty.
	 */

	@Test
	public void testFailureVolumeEmpty() throws IOException {
		String xattr = "";
                GlusterFSPathInfo.clear();
                GlusterFSPathInfo gpi = GlusterFSPathInfo.get(fs.getPath().toString(),xattr);
                BlockLocation[] blocks = gpi.getBlockLocations(0, 10000);
		assertNull(blocks);
	}

	/*
	 * 
	 * Require a patch to fix trusted.glusterfs.pathinfo in gluster stripe volume.
	 * https://bugzilla.redhat.com/show_bug.cgi?id=1200914
	 */
	@Test
	public void testStripeReplicateVolume() throws IOException {
		String xattr = "trusted.glusterfs.pathinfo=\"(<DISTRIBUTE:yuck-dht> (<STRIPE:yuck-stripe-0:[131072]> (<REPLICATE:yuck-replicate-0> <POSIX(/tmp/yuck_brick1):vm-1:/tmp/yuck_brick1/testfile> <POSIX(/tmp/yuck_brick2):vm-2:/tmp/yuck_brick2/testfile>)(<REPLICATE:yuck-replicate-1> <POSIX(/tmp/yuck_brick3):vm-3:/tmp/yuck_brick3/testfile> <POSIX(/tmp/yuck_brick4):vm-4:/tmp/yuck_brick4/testfile>)(<REPLICATE:yuck-replicate-2> <POSIX(/tmp/yuck_brick5):vm-5:/tmp/yuck_brick5/testfile> <POSIX(/tmp/yuck_brick6):vm-6:/tmp/yuck_brick6/testfile>)))\"";
		;
                GlusterFSPathInfo.clear();
                GlusterFSPathInfo gpi = GlusterFSPathInfo.get(fs.getPath().toString(),xattr);
                BlockLocation[] blocks = gpi.getBlockLocations(200000, 500000);
		assertTrue(blocks.length == 5);
		assertTrue(blocks[0].getHosts()[0].equals("vm-3"));
		assertTrue(blocks[0].getHosts()[1].equals("vm-4"));
		assertTrue(blocks[1].getHosts()[0].equals("vm-5"));
		assertTrue(blocks[1].getHosts()[1].equals("vm-6"));
		assertTrue(blocks[2].getHosts()[0].equals("vm-1"));
		assertTrue(blocks[2].getHosts()[1].equals("vm-2"));
		assertTrue(blocks[3].getHosts()[0].equals("vm-3"));
		assertTrue(blocks[3].getHosts()[1].equals("vm-4"));
		assertTrue(blocks[4].getHosts()[0].equals("vm-5"));
		assertTrue(blocks[4].getHosts()[1].equals("vm-6"));
		
	}

}
