/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.coordinator;

import java.util.HashMap;
import java.util.Map;
import javax.servlet.Servlet;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.webapp.WebAppServer;

public class SamzaCoordinator {
  private final WebAppServer coordinatorWebApp;
  private int webAppServerPort = 0;

  public SamzaCoordinator(Config config, Map<TaskName, Integer> taskToPartitionMapping) {
    Map<String, Servlet> servlets = new HashMap<String, Servlet>();
    servlets.put("/config/*", new SamzaCoordinatorConfigServlet(config));
    servlets.put("/tasks/*", new SamzaCoordinatorTaskMappingServlet(taskToPartitionMapping));
    this.coordinatorWebApp = new WebAppServer(servlets);
  }

  public void start() throws Exception {
    webAppServerPort = coordinatorWebApp.start();
  }

  public void stop() throws Exception {
    coordinatorWebApp.stop();
  }

  public int getWebAppServerPort() {
    return webAppServerPort;
  }
}
