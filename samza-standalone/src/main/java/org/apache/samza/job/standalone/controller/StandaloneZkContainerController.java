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

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.standalone.controller.StandaloneZkCoordinatorController;
import org.apache.samza.util.ZkUtil;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandaloneZkContainerController {
  private static final Logger log = LoggerFactory.getLogger(StandaloneZkContainerController.class);
  private final ZkClient zkClient;
  private final ContainerStateListener containerStateListener;
  private final IZkChildListener coordinatorPathListener;
  private final IZkDataListener coordinatorDataListener;
  private final String containerId;
  private final String containerPath;
  private volatile Thread containerThread;
  private volatile ApplicationStatus status;
  private volatile boolean running;
  private volatile String coordinatorUrl;

  public StandaloneZkContainerController(String containerId, ZkClient zkClient) {
    this.containerId = containerId;
    this.containerPath = StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + containerId;
    this.zkClient = zkClient;
    this.containerStateListener = new ContainerStateListener();
    this.coordinatorPathListener = new CoordinatorPathListener();
    this.coordinatorDataListener = new CoordinatorDataListener();
    this.status = ApplicationStatus.New;
    this.running = false;
  }

  public synchronized void start() throws InterruptedException {
    if (!running) {
      log.info("Starting container controller.");
      zkClient.waitUntilConnected();
      zkClient.subscribeStateChanges(containerStateListener);
      zkClient.delete(containerPath);
      zkClient.createEphemeral(containerPath, Collections.emptyMap());
      List<String> coordinatorChildren = zkClient.subscribeChildChanges(StandaloneZkCoordinatorController.COORDINATOR_PATH, coordinatorPathListener);
      refreshAssignments(coordinatorChildren);
      log.debug("Finished starting container controller.");
      running = true;
    } else {
      log.debug("Attempting to start a container controller that's already been started. Ignoring.");
    }
  }

  public synchronized void pause() throws InterruptedException {
    if (running) {
      log.info("Stopping container controller.");
      if (containerThread != null) {
        containerThread.interrupt();
        containerThread.join();
      }
      try {
        // Relinquish all task ownership.
        zkClient.writeData(containerPath, Collections.emptyMap());
      } catch (ZkNoNodeException e) {
        log.info("Unable to relinquish task ownership due to missing ZK node path: {}", containerPath);
      }
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
    zkClient.unsubscribeStateChanges(containerStateListener);
  }

  private synchronized void refreshAssignments(List<String> coordinatorControllerChildren) throws InterruptedException {
    String coordinatorControllerChild = ZkUtil.getLeader(coordinatorControllerChildren);
    zkClient.subscribeDataChanges(StandaloneZkCoordinatorController.COORDINATOR_PATH + "/" + coordinatorControllerChild, coordinatorDataListener);
    Map<String, String> urlMap = zkClient.readData(StandaloneZkCoordinatorController.COORDINATOR_PATH + "/" + coordinatorControllerChild, true);
    refreshAssignments(urlMap);
  }

  private synchronized void refreshAssignments(Map<String, String> urlMap) throws InterruptedException {
    String url = urlMap.get("url");
    if (url != null && (coordinatorUrl == null || !coordinatorUrl.equals(url))) {
      coordinatorUrl = url;
      List<String> taskNames = new ArrayList<String>();
      if (containerThread != null) {
        log.info("Shutting down container thread.");
        // TODO this seems like it might take a while. Should we move it into
        // another thread (off the ZK event thread)?
        // TODO this is the worst. We should only have to interrupt once, but
        // Kafka's Metadata class catches InterruptExceptions in
        // Metdata.awaitUpdate! It looks like this is fixed in trunk, but in
        // 0.8.2, the bug is still there. As a result, when we interrupt, the
        // Metadata class catches the exception and disregards it. To get around
        // this, just keep throwing it until the thread dies. Should go away
        // after 0.8.2 Kafka.
        while (containerThread.isAlive()) {
          containerThread.interrupt();
          Thread.sleep(5);
        }
        containerThread = null;
      }
      // TODO need to manage everything that's in SamzaContainer.safeMain();
      // JmxServer, exception handler, etc.
      log.info("Refreshing task assignments with coordinator url: {}", url);
      try {
        JobModel jobModel = SamzaContainer.readJobModel(url);
        ContainerModel containerModel = jobModel.getContainers().get(containerId);
        if (containerModel != null && containerModel.getTasks().size() > 0) {
          SamzaContainer container = SamzaContainer.apply(containerModel, jobModel.getConfig());
          containerThread = new Thread(new Runnable() {
            @Override
            public void run() {
              log.info("Running new container.");
              try {
                container.run();
                status = ApplicationStatus.SuccessfulFinish;
              } catch (InterruptedException e) {
                log.info("Shutdown complete.");
                Thread.currentThread().interrupt();
              }
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
          // Announce ownership.
          log.info("Announcing ownership for container {} with tasks: {}", containerId, taskNames);
          Map<String, List<String>> taskMap = new HashMap<String, List<String>>();
          taskMap.put("tasks", taskNames);
          zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + containerId, taskMap);
        }
      } catch (ConnectException e) {
        log.warn("Pausing. Failed to refresh container assignments.", e);
      }
    }
  }

  private class CoordinatorDataListener implements IZkDataListener {
    @SuppressWarnings("unchecked")
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      log.trace("CoordinatorDataListener.handleDataChange with data path {} and payload: {}", dataPath, data);
      refreshAssignments((Map<String, String>) data);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      // TODO do we need to unsubscribe?
    }
  }

  // TODO exact same code in both coordinator and container controller. Clean
  // up.
  private class ContainerStateListener implements IZkStateListener {
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
      log.warn("Session has expired. Creating a new one.");
      pause();
      start();
    }
  }

  private class CoordinatorPathListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      log.trace("CoordinatorPathListener.handleChildChange with parent path {} and children: {}", parentPath, currentChildren);
      refreshAssignments(currentChildren);
    }
  }
}
