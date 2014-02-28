package org.apache.hadoop.fs.libgfsio;

import java.io.IOException;

import java.util.Properties;

/**
 * Versioning stuff for the shim.  This class is not tested since there is no 
 * deterministic behaviour (i.e. it might not work if not building from binary), 
 * and the effects are pure side effects.
 */
public class Version extends Properties{
    public Version() {
        super();
        try{
            load(this.getClass().getClassLoader().getResourceAsStream("git.properties"));
        }
        catch(Throwable t){
            throw new RuntimeException("Couldn't find git properties for version info " + t.getMessage());
        }
    }
    public String getTag(){
        return this.getProperty("git.commit.id.describe").split("-")[0];
    }
    
    /**
     * For use with terminal version checking. 
       
       Example, run with an argument to get single property:
             java -cp /home/Development/hadoop-glusterfs/glusterfs-2.0-SNAPSHOT.jar \
                 org.apache.hadoop.fs.glusterfs.Version git.commit.id.describe | cut -d'-' -f 1     

       Or just run (no args, prints all properties)
             java -cp /home/Development/hadoop-glusterfs/glusterfs-2.0-SNAPSHOT.jar \
    */
    public static void main(String[] args){
        Version v = new Version();
        //Dump the whole version info if no arg
        if(args.length==0){
            System.out.println(v);
        }
        //if specific arg given, print just that.
        else{
            String prop = v.get(args[0])+"";
            System.out.println(
                    prop!=null?
                        prop
                        :"Couldnt find property "+prop);
        }
    }
}