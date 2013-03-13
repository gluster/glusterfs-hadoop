package org.apache.hadoop.fs.glusterfs;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * Because java 6 doesn't support cross platform file info, we use Runtime exec
 * to get details.
 * 
 * This class will be extended in the future to also return permissions.
 */
public class FileInfoUtil {
	
	/**
	 * Returns the array of values from ls -aFl : 
	 * 
	 * Currently only handles "owner" , since that is what is most needed by 
	 * the GFAPI that is meant to use this class.
	 * 
	 * m = getLSinfo("myfile.txt").get("owner") 
	 * System.out.println(m) ; <-- jayunit100
	 */
	public static Map<String, String> getLSinfo(String filename) throws IOException {
		String ret="";
		try {
			String ls_str;
			Process ls_proc = Runtime.getRuntime().exec("/bin/ls -aFl " + filename);
			DataInputStream ls_in = new DataInputStream(
					ls_proc.getInputStream());
			try {
				//should only be one line if input is a file
				while ((ls_str = ls_in.readLine()) != null) {
					if(ls_str.length()<10)
						;
					else{
						ret+=(ls_str);
					}
				}
			} 
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		catch (IOException e1) {
			throw new RuntimeException(e1);
		}
		//initialization to "" was only to make for clean appending.
		if(ret.equals(""))
			ret=null;

		//Convenience: transform to map
		Map<String, String> mm = lsToMap(ret);
		return mm;
	}

	/**
	 * Converts the output of ls -aFl to a map :
	 * 
	 * EXPECTED INPUT STRING: 
	 * -rwxr-xr-x. 1 root root  786 Mar  7 11:42 buildbranch_and_copyjartovms.sh*
	 * 
	 * EXPECTED OUTPUT:
	 * key="owner" -> the owner of the file (root)
	 * key="permissions" -> the hadoop permissions on the file.
	 */
	private static Map<String, String> lsToMap(String ret) {
		String[] values = ret.split("\\s+");
		
		Map<String,String> mm = new TreeMap();
		mm.put("owner", values[2]);
		
		return mm;
	}
	
	
}