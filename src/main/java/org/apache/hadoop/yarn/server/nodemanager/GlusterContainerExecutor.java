/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.ExitCode;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.util.DefaultLCEResourcesHandler;
import org.apache.hadoop.yarn.server.nodemanager.util.LCEResourcesHandler;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.spi.LoggerFactory;
import org.slf4j.Logger;
public class GlusterContainerExecutor extends LinuxContainerExecutor {
  
   static Logger log = org.slf4j.LoggerFactory.getLogger(GlusterContainerExecutor.class);
   /**
   * We override the YARN-1235 patch here.
   * @param user
   * @return
   */
  @Override
  String getRunAsUser(String user) {
   log.info("Container EXEC overrid: returning user " + user);
      //return UserGroupInformation.isSecurityEnabled() ? user : nonsecureLocalUser;
    return user;
  }
@Override
public void deleteAsUser(String arg0,Path arg1,Path...arg2){
    log.info("DELETE AS USER " + arg0 + " " + arg1 +" " + arg2);
    super.deleteAsUser(arg0, arg1, arg2);
}
}

