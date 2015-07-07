/**
 *
 * Copyright (c) 2011 Gluster, Inc. <http://www.gluster.com>
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

package org.apache.hadoop.fs.glusterfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlusterFSPathInfo {
   static final Logger log = LoggerFactory.getLogger(GlusterFSPathInfo.class);


   private static ConcurrentMap<String,GlusterFSPathInfo> pathInfoCaches = new ConcurrentHashMap<String,GlusterFSPathInfo>();  // The key is the filename, assuming that the file doesn't change during the execution time.

   private String pathInfo;
   private long stripeSize;
   private List<List<String>> locations;

   public static final GlusterFSPathInfo INVALID = new GlusterFSPathInfo("",-1,null);

   public GlusterFSPathInfo(String pathInfo, long stripeSize, List<List<String>> locations) {
      this.pathInfo = pathInfo;
      this.stripeSize = stripeSize;
      this.locations = locations; 
   } 

   /*
    * @param pathInfo A string with no leading white spaces
    * @return The offset of the next subvolume or brick (skip white spaces)
    */
   private static int nextStripeOffset(String pathInfo) {
      int len = pathInfo.length();
      int endIdx = 0;
      if (pathInfo.charAt(0) == '<') {
         endIdx = pathInfo.indexOf('>');
      } else if (pathInfo.charAt(0) == '(') {
         int count = 1;
         for (endIdx=1; endIdx < len; endIdx++) {
            if (pathInfo.charAt(endIdx) == '(') 
               count++;
            else if (pathInfo.charAt(endIdx) == ')')
               count--;
            if (count <= 0)
               break;
         }
         if (count != 0) 
            throw new RuntimeException("Mismatched parenthesis in " + pathInfo);
      }

      if (endIdx == 0) 
         throw new RuntimeException("Expected pattern not found " + pathInfo);

      // Skip trailing white spaces
      for (endIdx++; endIdx < len; endIdx++) {
         if (!Character.isWhitespace(pathInfo.charAt(endIdx)))
            break; 
      }

      return endIdx;
   }

   /*
    * @param pathInfo A string starting with '('
    * @return The string between (excluding) outer parenthesis
    */
   private static String getSubVolume(String pathInfo) {
     int count = 0;
     int idx=0;
     for (; idx < pathInfo.length(); idx++) {
        if (pathInfo.charAt(idx) == '(') 
           count++;
        else if (pathInfo.charAt(idx) == ')')
           count--;
        if (count <= 0)
           break;
     }

     if (count == 0) 
        return pathInfo.substring(1,idx);
     else 
        throw new RuntimeException("Mismatched parenthesis in " + pathInfo);
   }

   /*
    * @param pathInfo A glusterfs pathinfo extended attribute
    * @return A GlusterFSPathInfo
    */
   private static GlusterFSPathInfo parse(String pathInfo) {
      String save = pathInfo;
  
      long stripeSize = Long.MAX_VALUE;

      Pattern pathInfoPattern = Pattern.compile("trusted.glusterfs.pathinfo=\"(.+)\"$"); 
      Pattern dhtPattern = Pattern.compile("^\\(<DISTRIBUTE:[^>]+> (.+)\\)$");
      Pattern stripePattern = Pattern.compile("^<STRIPE:[^:]+:\\[(\\d+)\\]> (.+)$");
      Pattern replicatePattern = Pattern.compile("^<REPLICATE:[^>]+> (.+)$"); 
      Pattern brickPattern = Pattern.compile("<POSIX\\(.*?\\):(.*?):.*?>"); 

      Matcher pathInfoMatcher = pathInfoPattern.matcher(pathInfo);
      if (pathInfoMatcher.find()) {
         pathInfo = pathInfoMatcher.group(1);
      } else {
         throw new RuntimeException("Cannot find pathinfo attribute");
      }

      // First level = DHT (exactly once)
      Matcher dhtMatcher = dhtPattern.matcher(pathInfo);
      if (dhtMatcher.find()) {
         pathInfo = dhtMatcher.group(1);
      }

      // Next level can be nested volume enclosed in ()
      if (pathInfo.startsWith("(")) {
         String subVolume = getSubVolume(pathInfo); 
         if (subVolume.length() != pathInfo.length()-2) 
            log.warn("Ignore garbage at the end of pathInfo: " + pathInfo.substring(subVolume.length()+2)); // Should not have anything else since DHT must return 1 subvolume

         pathInfo = subVolume;
      }

      // Check if STRIPE, then find the stripe index 
      int stripeIndex = 0;
      int numStripes = 0;
      List<String> stripes = new ArrayList<String>();
      Matcher stripeMatcher = stripePattern.matcher(pathInfo);
      if (stripeMatcher.find()) {
         stripeSize = Long.parseLong(stripeMatcher.group(1));
         log.debug("Stripe Size: " + stripeSize);
         pathInfo = stripeMatcher.group(2);

         while (!pathInfo.isEmpty())  { 
            int nextOffset = nextStripeOffset(pathInfo);
            if (pathInfo.charAt(0) == '(') {
               stripes.add(pathInfo.substring(1,pathInfo.lastIndexOf(')',nextOffset)));
            } else {
               stripes.add(pathInfo.substring(0,nextOffset).trim());
            }
            pathInfo = pathInfo.substring(nextOffset);
            numStripes++;
         }

      } else {
         numStripes = 1;
         stripes.add(pathInfo); // Pretending 1 stripe
      }

      List<List<String>> locations = new ArrayList<List<String>>();
      for (String stripe: stripes) {
         // Then, check if REPLICATE 
         pathInfo = stripe;
         Matcher replicateMatcher = replicatePattern.matcher(pathInfo);
         if (replicateMatcher.find()) {
            pathInfo = replicateMatcher.group(1);
         }

         // Last level = POSIX (mandatory)
         Matcher brickMatcher = brickPattern.matcher(pathInfo);
         List<String> hosts = new ArrayList<String>();
         while (brickMatcher.find()) {
            hosts.add(brickMatcher.group(1));
         }
         locations.add(hosts);
      }
      return new GlusterFSPathInfo(save, stripeSize, locations);
   }

   /*
    * @param filename A fuse-mounted filename
    * @return A GlusterFSPathInfo
    */
   public static GlusterFSPathInfo get(String filename) {
      String cmdOut = new GlusterFSXattr(filename).getPathInfo();
      return get(filename, cmdOut);
   }

   /*
    * @param filename A fuse-mounted filename
    * @param xattr A gluster pathinfo extended attribute
    * @return A GlusterFSPathInfo
    */
   public static GlusterFSPathInfo get(String filename, String xattr) {
      GlusterFSPathInfo pathInfo = INVALID;
      if (!pathInfoCaches.containsKey(filename)) {
         try {
            pathInfo = parse(xattr);
         } catch (Exception e) {
            log.error("Invalid pathinfo: " + e);
         }
         pathInfoCaches.putIfAbsent(filename,pathInfo);
      }
      pathInfo = pathInfoCaches.get(filename);
      return pathInfo; 
   }

   /*
    * Clear cache
    */
   public static void clear() {
      pathInfoCaches.clear(); 
   }

   public String getPathInfo() {
      return this.pathInfo;
   }

   public long getStripeSize() {
      return this.stripeSize;
   }

   public List<List<String>> getLocations() {
      return this.locations;
   }

   public BlockLocation[] getBlockLocations(long start, long len) {
        long stripeSize = this.stripeSize;                       
        List<List<String>> locations = this.locations;           
        if (locations == null) 
           return null;
        int numStripes = locations.size();                       
        int blockStart = (int) (start/stripeSize);               
        int blockEnd = (int) ((start+len-1)/stripeSize);         
        int numBlocks = blockEnd-blockStart+1;
        BlockLocation[] blkLocations = new BlockLocation[numBlocks];
        for (int i=0; i < numBlocks; i++) {                      
           long startOffset = (blockStart+i)*stripeSize;         
           long blockLen = Math.min(stripeSize,start+len-startOffset);         
           int stripeIndex = ((int) (startOffset / stripeSize)) % numStripes;  
           String[] hosts = locations.get(stripeIndex).toArray(new String[0]); 
           String[] names = hosts;                               
           blkLocations[i] = new BlockLocation(names,hosts,startOffset,blockLen);
           log.debug("Locate block " + blkLocations[i]);         
        }                                 
        return blkLocations;
   }

}
