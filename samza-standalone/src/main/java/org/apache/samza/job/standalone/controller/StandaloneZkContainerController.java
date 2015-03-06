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
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneZkContainerController {
  private static final Logger log = LoggerFactory.getLogger(StandaloneZkContainerController.class);
  private final ZkClient zkClient;
  private final CoordinatorStateListener coordinatorStateListener;
  private final IZkDataListener assignmentPathListener;
  private volatile String containerSequentialId;
  private volatile Thread containerThread;
  private volatile ApplicationStatus status;
  private volatile boolean running;

  public StandaloneZkContainerController(ZkClient zkClient) {
    this.zkClient = zkClient;
    this.coordinatorStateListener = new CoordinatorStateListener();
    this.assignmentPathListener = new AssignmentPathListener();
    this.status = ApplicationStatus.New;
    this.running = false;
  }

  public synchronized void start() throws InterruptedException {
    if (!running) {
      log.info("Starting container controller.");
      zkClient.waitUntilConnected();
      zkClient.subscribeStateChanges(coordinatorStateListener);
      zkClient.subscribeDataChanges(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH, assignmentPathListener);
      if (containerSequentialId == null) {
        containerSequentialId = new File(zkClient.createEphemeralSequential(StandaloneZkCoordinatorController.CONTAINER_PATH + "/", Collections.emptyList())).getName();
      }
      refreshAssignments();
      log.debug("Finished starting container controller.");
      running = true;
    } else {
      log.debug("Attempting to start a container controller that's already been started. Ignoring.");
    }
  }

  public synchronized void pause() throws InterruptedException {
    if (running) {
      log.info("Stopping container controller.");
      zkClient.unsubscribeDataChanges(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH, assignmentPathListener);
      if (containerThread != null) {
        containerThread.interrupt();
        containerThread.join();
      }
      // Relinquish all task ownership.
      zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + containerSequentialId, Collections.emptyList());
      log.debug("Finished stopping container controller.");
      running = false;
    } else {
      log.debug("Attempting to stop a container controller that's already been started. Ignoring.");
    }
  }

  public ApplicationStatus getStatus() {
    return status;
  }

  public void stop() throws InterruptedException {
    this.pause();
    // TODO should delete ephemeral node here.
    // Stop listening to ZK, so we never reconnect.
    zkClient.unsubscribeStateChanges(coordinatorStateListener);
  }

  @SuppressWarnings("unchecked")
  private void refreshAssignments() throws InterruptedException {
    Map<String, Object> assignments = (Map<String, Object>) zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH, true);
    if (assignments != null) {
      refreshAssignments(assignments);
    }
  }

  private synchronized void refreshAssignments(Map<String, Object> assignments) throws InterruptedException {
    List<String> taskNames = new ArrayList<String>();
    if (containerThread != null) {
      log.info("Shutting down container thread.");
      // TODO this seems like it might take a while. Should we move it into
      // another thread (off the ZK event thread)?
      // TODO this is a real bummer. looks like something is swallowing
      // interrupts.
      containerThread.interrupt();
      containerThread.join();
    }
    // TODO need to manage everything that's in SamzaContainer.safeMain();
    // JmxServer, exception handler, etc.
    log.info("Received task task assignments: {}", assignments);
    if (assignments.size() > 0) {
      Integer containerId = (Integer) assignments.get(containerSequentialId);
      if (containerId != null) {
        String url = (String) assignments.get(StandaloneZkCoordinatorController.COORDINATOR_URL_KEY);
        JobModel jobModel = SamzaContainer.readJobModel(url);
        ContainerModel containerModel = jobModel.getContainers().get(containerId);
        SamzaContainer container = SamzaContainer.apply(containerModel, jobModel.getConfig());
        containerThread = new Thread(new Runnable() {
          @Override
          public void run() {
            log.info("Running new container.");
            container.run();
            status = ApplicationStatus.SuccessfulFinish;
          }
        });
        log.info("Starting new container thread.");
        status = ApplicationStatus.Running;
        containerThread.setDaemon(true);
        containerThread.setName("Container ID (" + containerId + ")");
        containerThread.start();
        for (TaskName taskName : containerModel.getTasks().keySet()) {
          taskNames.add(taskName.toString());
        }
      } else {
        log.info("No container assignment for: {}", containerSequentialId);
      }
    }
    // Announce ownership.
    log.info("Announcing ownership for container {} with tasks: {}", containerSequentialId, taskNames);
    zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + containerSequentialId, taskNames);
  }

  public class AssignmentPathListener implements IZkDataListener {
    @Override
    @SuppressWarnings("unchecked")
    public void handleDataChange(String dataPath, Object data) throws Exception {
      log.trace("AssignmentPathListener.handleDataChange with data path {} and payload: {}", dataPath, data);
      Map<String, Object> assignments = (Map<String, Object>) data;
      refreshAssignments(assignments);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      // TODO oh no, how did a delete on assignments happen?!
    }
  }

  // TODO exact same code in both coordinator and container controller. Clean
  // up.
  private class CoordinatorStateListener implements IZkStateListener {
    @Override
    public void handleStateChanged(KeeperState zkState) throws Exception {
      if (zkState.equals(KeeperState.Disconnected) || zkState.equals(KeeperState.Expired)) {
        log.warn("Lost connection with ZooKeeper: {}", zkState);
        pause();
      } else if (zkState.equals(KeeperState.SyncConnected)) {
        log.info("Reconnected to ZooKeeper.");
        start();
      }
    }

    @Override
    public void handleNewSession() throws Exception {
      // TODO what is this?
      log.warn("Got a handleNewSession call. What is this? Should we stop(); start();?");
    }
  }
}
