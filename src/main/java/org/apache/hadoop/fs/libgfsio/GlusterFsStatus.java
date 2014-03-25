package org.apache.hadoop.fs.libgfsio;

import org.apache.hadoop.fs.FsStatus;
import org.gluster.fs.GlusterVolume;

public class GlusterFsStatus extends FsStatus{
	  
	  /** Construct a FsStatus object, using the specified statistics */
	  public GlusterFsStatus(long capacity, long used, long remaining) {
	   super(capacity,used,remaining);
	  }
	  
	  public GlusterFsStatus(GlusterVolume vol) {
		   super( vol.getSize(),vol.getSize()-vol.getFree(),vol.getFree());
	  }
		
	  

}
