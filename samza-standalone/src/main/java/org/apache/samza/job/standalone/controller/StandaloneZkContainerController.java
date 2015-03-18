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

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
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
  private final ContainerStateListener containerStateListener;
  private final IZkChildListener coordinatorPathListener;
  private volatile String containerSequentialId;
  private volatile Thread containerThread;
  private volatile ApplicationStatus status;
  private volatile boolean running;
  private volatile String coordinatorUrl;

  public StandaloneZkContainerController(ZkClient zkClient) {
    this.zkClient = zkClient;
    this.containerStateListener = new ContainerStateListener();
    this.coordinatorPathListener = new CoordinatorPathListener();
    this.status = ApplicationStatus.New;
    this.running = false;
  }

  public synchronized void start() throws InterruptedException {
    if (!running) {
      log.info("Starting container controller.");
      zkClient.waitUntilConnected();
      zkClient.subscribeStateChanges(containerStateListener);
      zkClient.subscribeChildChanges(StandaloneZkCoordinatorController.COORDINATOR_PATH, coordinatorPathListener);
      if (containerSequentialId == null) {
        containerSequentialId = new File(zkClient.createEphemeralSequential(StandaloneZkCoordinatorController.CONTAINER_PATH + "/", Collections.emptyList())).getName();
      }
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
      String containerSequentialIdPath = StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + containerSequentialId;
      try {
        // Relinquish all task ownership.
        zkClient.writeData(containerSequentialIdPath, Collections.emptyList());
      } catch (ZkNoNodeException e) {
        log.info("Unable to relinquish task ownership due to missing ZK node path: {}", containerSequentialIdPath);
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

  private synchronized void refreshAssignments(String url) throws InterruptedException {
    if (coordinatorUrl == null || !coordinatorUrl.equals(url)) {
      coordinatorUrl = url;
      List<String> taskNames = new ArrayList<String>();
      if (containerThread != null) {
        log.info("Shutting down container thread.");
        // TODO this seems like it might take a while. Should we move it into
        // another thread (off the ZK event thread)?
        // TODO this is a real bummer. looks like something is swallowing
        // interrupts.
        containerThread.interrupt();
        containerThread.join();
        containerThread = null;
      }
      // TODO need to manage everything that's in SamzaContainer.safeMain();
      // JmxServer, exception handler, etc.
      log.info("Refreshing task assignments with coordinator url: {}", url);
      JobModel jobModel = SamzaContainer.readJobModel(url);
      ContainerModel containerModel = jobModel.getContainers().get(containerSequentialId);
      if (containerModel != null && containerModel.getTasks().size() > 0) {
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
        containerThread.setName("Container ID (" + containerSequentialId + ")");
        containerThread.start();
        for (TaskName taskName : containerModel.getTasks().keySet()) {
          taskNames.add(taskName.toString());
        }
        // Announce ownership.
        log.info("Announcing ownership for container {} with tasks: {}", containerSequentialId, taskNames);
        zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + containerSequentialId, taskNames);
      }
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
      // Drop old containerSequentialId since our session has expired. A new one
      // will be created when start is called.
      containerSequentialId = null;
      start();
    }
  }

  private class CoordinatorPathListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      log.trace("CoordinatorPathListener.handleChildChange with parent path {} and children: {}", parentPath, currentChildren);
      Collections.sort(currentChildren);
      if (currentChildren.size() > 0) {
        String coordinatorControllerPath = currentChildren.get(0);
        String url = zkClient.readData(parentPath + "/" + coordinatorControllerPath, true);
        refreshAssignments(url);
      }
    }
  }
}
