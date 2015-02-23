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

package org.apache.samza.job.standalone.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;

public class StandaloneZkContainerController {
  private final ZkClient zkClient;
  private final IZkDataListener assignmentPathListener;
  private String containerSequentialId;
  private Thread containerThread;

  public StandaloneZkContainerController(ZkClient zkClient) {
    this.zkClient = zkClient;
    this.assignmentPathListener = new AssignmentPathListener();
  }

  public void start() {
    zkClient.subscribeDataChanges(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH, assignmentPathListener);
    containerSequentialId = new File(zkClient.createEphemeralSequential(StandaloneZkCoordinatorController.CONTAINER_PATH + "/", Collections.emptyList())).getName();
  }

  public void stop() throws Exception {
    zkClient.unsubscribeDataChanges(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH, assignmentPathListener);
    if (containerThread != null) {
      containerThread.interrupt();
      containerThread.join();
    }
    if (containerSequentialId != null) {
      zkClient.delete(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + containerSequentialId);
    }
  }

  public class AssignmentPathListener implements IZkDataListener {
    @Override
    @SuppressWarnings("unchecked")
    public void handleDataChange(String dataPath, Object data) throws Exception {
      List<String> taskNames = new ArrayList<String>();
      if (containerThread != null) {
        // TODO this seems like it might take a while. Should we move it into
        // another thread (off the ZK event thread)?
        // TODO this is a real bummer. looks like something is swallowing
        // interrupts.
        containerThread.interrupt();
        containerThread.join();
      }
      // TODO need to manage everything that's in SamzaContainer.safeMain();
      // JmxServer, exception handler, etc.
      Map<String, Object> assignments = (Map<String, Object>) data;
      if (assignments.size() > 0) {
        Integer containerId = (Integer) assignments.get(containerSequentialId);
        String url = (String) assignments.get(StandaloneZkCoordinatorController.COORDINATOR_URL_KEY);
        JobModel jobModel = SamzaContainer.readJobModel(url);
        ContainerModel containerModel = jobModel.getContainers().get(containerId);
        SamzaContainer container = SamzaContainer.apply(containerModel, jobModel.getConfig());
        containerThread = new Thread(new Runnable() {
          @Override
          public void run() {
            container.run();
          }
        });
        containerThread.setDaemon(true);
        containerThread.setName("Container ID (" + containerId + ")");
        containerThread.start();
        for (TaskName taskName : containerModel.getTasks().keySet()) {
          taskNames.add(taskName.toString());
        }
      }
      // Announce ownership.
      zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + containerSequentialId, taskNames);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      // TODO oh no, how did a delete on assignments happen?!
    }
  }
}
