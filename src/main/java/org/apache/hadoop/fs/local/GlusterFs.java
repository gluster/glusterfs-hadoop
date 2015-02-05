/**
 *
 * Copyright (c) 2013 Red Hat, Inc. <http://www.redhat.com>
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

/**
 * Implements the Hadoop FileSystem 2.x Interface to allow applications to store
 * files on GlusterFS and run Map/Reduce jobs on the data.  This code does not perform a CRC 
 * on the files.
 * 
 * gluster file systems are specified with the glusterfs:// prefix.
 * 
 * 
 */

package org.apache.hadoop.fs.local;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FilterFs;

public class GlusterFs extends FilterFs{

    GlusterFs(Configuration conf) throws IOException, URISyntaxException{
        super(new GlusterVol(conf));
    }

    GlusterFs(final URI theUri, final Configuration conf) throws IOException, URISyntaxException{
        this(conf);
    }


}
