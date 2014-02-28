/**
 *
 * Copyright (c) 2011 Red Hat, Inc. <http://www.redhat.com>
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
 * Implements the Hadoop FileSystem Interface to allow applications to store
 * files on GlusterFS and run Map/Reduce jobs on the data.
 * 
 * 
 */

package org.apache.hadoop.fs.libgfsio;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.gluster.fs.GlusterFile;
/*
 * Copied from org.apache.fs.RawLocalFileSystem.RawFileStatus
 */
public class GlusterFileStatus extends FileStatus{
	

	
	public GlusterFileStatus(GlusterFile file, GlusterfsVolume vol) { 
		  super(file.length(),
				file.isDirectory(),
				0, // repliation count
				file.getBlockSize(),
				file.getMtime(),
				file.getAtime(),
				new FsPermission(new Long(file.getMod()).shortValue()),
				IdLookup.getName(new Long(file.getUid()).intValue()),
				IdLookup.getName(new Long(file.getGid()).intValue()),
				null, // path to link 
				vol.makeQualified(new Path(file.getPath())));
		  
	  }
}
