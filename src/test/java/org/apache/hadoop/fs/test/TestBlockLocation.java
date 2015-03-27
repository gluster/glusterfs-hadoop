package org.apache.hadoop.fs.test;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.glusterfs.GlusterFSXattr;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorFactory;
import org.apache.hadoop.fs.test.connector.HcfsTestConnectorInterface;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBlockLocation {

	/*
	 * Test subclass to simulate / inject xattr values instead of requiring
	 * filesystem.
	 * 
	 */
	class TestBlockLocationHarnace extends GlusterFSXattr {
		String xattrValue = null;

		TestBlockLocationHarnace(String xattrValue) {
			super(null);
			this.xattrValue = xattrValue;
		}

		public String execGetFattr() throws IOException {
			return this.xattrValue;
		}
	}

	@Test
	public void testReplicateVolume() throws IOException {
		String xattr = "trusted.glusterfs.pathinfo=\"(<DISTRIBUTE:StripeVolume-dht> (<STRIPE:StripeVolume-stripe-0:[131072]> <POSIX(/mnt/brick1/stripeVol):vm-1:/mnt/brick1/stripeVol/a>)\"";
		xattr = xattr.substring(28, xattr.length() - 1);
		TestBlockLocationHarnace tbl = new TestBlockLocationHarnace(xattr);

		int start = 0;
		int len = 0;
		BlockLocation[] blocks = tbl.getPathInfo(start, len);
		assertTrue(blocks.length == 1);
		assertTrue(blocks[0].getHosts()[0].equals("vm-1"));
	}

	@Test
	public void testReplicateVolume2() throws IOException {
		String xattr = "trusted.glusterfs.pathinfo=\"(<DISTRIBUTE:HadoopVol-dht> (<REPLICATE:HadoopVol-replicate-0> <POSIX(/mnt/brick1/HadoopVol):vm-2:/mnt/brick1/HadoopVol/tmp/a> <POSIX(/mnt/brick1/HadoopVol):vm-1:/mnt/brick1/HadoopVol/tmp/a>))\"";
		xattr = xattr.substring(28, xattr.length() - 1);
		TestBlockLocationHarnace tbl = new TestBlockLocationHarnace(xattr);
		BlockLocation[] blocks = tbl.getPathInfo(0, 10000);
		
		assertTrue(blocks.length == 1);
		String hosts[] = blocks[0].getHosts();
		
		assertTrue(hosts[0].equals("vm-2"));
		assertTrue(hosts[1].equals("vm-1"));
	}

	/*
	 * Make sure the xattr code fails gracefull when the input is garbage.
	 */
	@Test
	public void testFailureVolumeGarbage() {
		String xattr = "dsfsffsfdsf";
		TestBlockLocationHarnace tbl = new TestBlockLocationHarnace(xattr);
		BlockLocation[] blocks = tbl.getPathInfo(0, 10000);
		assertNull(blocks);
	}

	/*
	 * Make sure the xattr code fails gracefully when the input is empty.
	 */

	@Test
	public void testFailureVolumeEmpty() {
		String xattr = "";
		TestBlockLocationHarnace tbl = new TestBlockLocationHarnace(xattr);
		BlockLocation[] blocks = tbl.getPathInfo(0, 10000);
		assertNull(blocks);
	}

	/*
	 * 
	 * stripe not yet supported.trusted.pathinfo is incorrect in gluster:
	 * https://bugzilla.redhat.com/show_bug.cgi?id=1200914
	 */

	public void testStripeVolume() {
		String xattr = "trusted.glusterfs.pathinfo=\"(<DISTRIBUTE:yuck-dht> (<STRIPE:yuck-stripe-0:[131072]> (<REPLICATE:yuck-replicate-0> <POSIX(/tmp/yuck_brick1):questor:/tmp/yuck_brick1/testfile> <POSIX(/tmp/yuck_brick2):questor:/tmp/yuck_brick2/testfile>)(<REPLICATE:yuck-replicate-1> <POSIX(/tmp/yuck_brick3):questor:/tmp/yuck_brick3/testfile> <POSIX(/tmp/yuck_brick4):questor:/tmp/yuck_brick4/testfile>)(<REPLICATE:yuck-replicate-2> <POSIX(/tmp/yuck_brick5):questor:/tmp/yuck_brick5/testfile> <POSIX(/tmp/yuck_brick6):questor:/tmp/yuck_brick6/testfile>))\"";
		;
		xattr = xattr.substring(28, xattr.length() - 1);
		TestBlockLocationHarnace tbl = new TestBlockLocationHarnace(xattr);
		BlockLocation[] blocks = tbl.getPathInfo(0, 10000);

	}

}
