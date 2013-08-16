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
 * Implements the Hadoop FileSystem Interface to allow applications to store
 * files on GlusterFS and run Map/Reduce jobs on the data.
 * 
 * 
 */

package org.apache.hadoop.fs.glusterfs;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;

public class Util{

    public static String execCommand(File f,String...cmd) throws IOException{
        String[] args=new String[cmd.length+1];
        System.arraycopy(cmd, 0, args, 0, cmd.length);
        args[cmd.length]=FileUtil.makeShellPath(f, true);
        String output=Shell.execCommand(args);
        return output;
    }

    /* copied from unstalbe hadoop API org.apache.hadoop.Shell */
    public static String[] getGET_PERMISSION_COMMAND(){
        // force /bin/ls, except on windows.
        return new String[]{(WINDOWS ? "ls" : "/bin/ls"),"-ld"};
    }
    
    /* copied from unstalbe hadoop API org.apache.hadoop.Shell */
    
    public static final boolean WINDOWS /* borrowed from Path.WINDOWS */
    =System.getProperty("os.name").startsWith("Windows");
    // / loads permissions, owner, and group from `ls -ld`

    /** 
     * Check that a Path belongs to this FileSystem. 
     * lenient : doesn't check authority. This might be temporary ~ 
     * there could be a better long term implementation.  
     * Having custom implementation here is critical for debugging, because
     * existing checkPath in hadoop doesn't print all the scheme/auth values.
     * */
    public static void checkPath(FileSystem fs, Path path) {
        String thisScheme = fs.getUri().getScheme();
        String thisAuthority = fs.getUri().getAuthority();

        String thatScheme = path.toUri().getScheme();
        String thatAuthority = path.toUri().getAuthority();
        
        //String debugInfo="GV: checking path " +path+  " scheme=" + thisScheme+" auth="+thisAuthority + " vs scheme=" + thatScheme +" auth=" + thatAuthority;
        //log.info(debugInfo);
        //log.warn("Not validating authority");
        //now the exception will be traceable in the logs above .
        try{
            //super.checkPath(path);
            if(thisScheme.equals(thatScheme) || (thatScheme==null && thatAuthority==null))
                return ;
            else
                throw new RuntimeException("Schemes dont match: expecting :" + thisScheme + " but input path is :" + thatScheme);
         }
        catch(Throwable t){
            throw new RuntimeException("ERROR matching schemes/auths: " + thisScheme +" ~ " + thisAuthority + " : " + thatScheme + " ~ " + thatAuthority );
        }
    }
}
