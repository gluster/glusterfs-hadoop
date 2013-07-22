package org.apache.hadoop.fs.local;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFs;

public class GlusterFs extends ChecksumFs{

    GlusterFs(Configuration conf) throws IOException, URISyntaxException{
        super(new GlusterVol(conf));
    }

    GlusterFs(final URI theUri, final Configuration conf) throws IOException, URISyntaxException{
        this(conf);
    }

}
