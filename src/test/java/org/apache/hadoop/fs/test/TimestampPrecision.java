package org.apache.hadoop.fs.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.glusterfs.GlusterVolume;
import org.junit.Test;

public class TimestampPrecision {

	@Test
	public void testSimpleTruncate0() throws IOException, URISyntaxException{
		Configuration config = new Configuration();
		config.setInt("fs.glusterfs.timestamp.trim", 0);
		GlusterVolume fs = new GlusterVolume();
		fs.initialize(new URI("glusterfs:///"), config);
		
		long value1 = 999911111;
		long trim = fs.trimTimestamp(value1);
		assertEquals(trim,value1);
		
	}
	
	
	@Test
	public void testSimpleTruncate1() throws IOException, URISyntaxException{
		Configuration config = new Configuration();
		config.setInt("fs.glusterfs.timestamp.trim", 1);
		GlusterVolume fs = new GlusterVolume();
		fs.initialize(new URI("glusterfs:///"), config);
		
		long value1 = 999911111;
		long trim = fs.trimTimestamp(value1);
		assertEquals(trim,999911110 );
		
	}
	
	@Test
	public void testSimpleTruncate2() throws IOException, URISyntaxException{
		Configuration config = new Configuration();
		config.setInt("fs.glusterfs.timestamp.trim", 2);
		GlusterVolume fs = new GlusterVolume();
		fs.initialize(new URI("glusterfs:///"), config);
		
		long value1 = 999911111;
		long trim = fs.trimTimestamp(value1);
		assertEquals(trim,999911100 );
		
	}
	@Test
	public void testSimpleTruncate3() throws IOException, URISyntaxException{
		Configuration config = new Configuration();
		config.setInt("fs.glusterfs.timestamp.trim", 3);
		GlusterVolume fs = new GlusterVolume();
		fs.initialize(new URI("glusterfs:///"), config);
		
		long value1 = 999911111;
		long trim = fs.trimTimestamp(value1);
		assertEquals(trim,999911000 );
		
	}
	
}
