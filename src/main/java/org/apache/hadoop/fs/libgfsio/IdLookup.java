package org.apache.hadoop.fs.libgfsio;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IdLookup {
	
	public static final int UID = 1;
	public static final int GID = 2;
	
	public static int getUid(String name) throws IOException{
		return getId(name, UID);
	}
	
	public static int getGid(String name) throws IOException{
		return getId(name, GID);
	}
	
	protected static int getId(String name, int type){

	    String userName = System.getProperty("user.name");
	    String arg = null;
		    
	    switch(type){
	    	case IdLookup.UID : 
	    		arg = "-u";
    		break;
		    		
		    case IdLookup.GID :
		    	arg = "-g";
		    break;
		}
		    
		String command = "id "+ arg + " " + userName;
		Process child = null;
		try {
			child = Runtime.getRuntime().exec(command);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		// Get the input stream and read from it
		InputStream in = child.getInputStream();
		String output = new String();
		int c;
		try{
			while ((c = in.read()) != -1) {
			    output+=c;
			}
			in.close();
		}catch(IOException ex){
			
		}
		return Integer.parseInt(output);
		
	}
	
	public static String getName(int id){
		 String command = "getent passwd";
		 Process child=null;
		 String s=null;
		try {
			child = Runtime.getRuntime().exec(command);
		} catch (IOException e) {
			e.printStackTrace();
		}
		BufferedReader brInput=new BufferedReader(new InputStreamReader(child.getInputStream()));

	     String   cmdOut="";
	        try {
				while ((s=brInput.readLine())!=null)
				    cmdOut+=s;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    String pattern = "^(\\w*)\\:\\w*\\:" + id+":";
	    Matcher results = Pattern.compile(pattern).matcher(cmdOut);
		String name = null;
	    try{
			results.find();
	    	name =  results.group(1);
		}catch(IllegalStateException ex){
			// user not found
		}
		
	    return name;
		
	}

}
