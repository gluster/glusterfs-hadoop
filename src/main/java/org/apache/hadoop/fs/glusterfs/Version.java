package org.apache.hadoop.fs.glusterfs;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Version extends Properties{
    final static Logger lg = LoggerFactory.getLogger(Version.class);
    public Version() {
        super();
        try{
            load(this.getClass().getClassLoader().getResourceAsStream("git.properties"));
        }
        catch(Throwable t){
            lg.error("Couldn't find git properties for version info " + t.getMessage());
        }
    }
}
