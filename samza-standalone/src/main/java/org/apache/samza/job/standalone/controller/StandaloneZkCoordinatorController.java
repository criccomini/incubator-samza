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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.server.HttpServer;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.util.ZkUtil;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO should have metrics for everything.
// TODO javadocs everywhere.

public class StandaloneZkCoordinatorController {
  public static final String COORDINATOR_PATH = "/coordinator";
  public static final String CONTAINER_PATH = "/containers";
  private static final Logger log = LoggerFactory.getLogger(StandaloneZkCoordinatorController.class);
  private final Config config;
  private final String zkConnect;
  private final ZkClient zkClient;
  private final StandaloneZkCoordinatorState state;
  private final CoordinatorStateListener coordinatorStateListener;
  private final IZkChildListener coordinatorPathListener;
  private final IZkChildListener containerPathListener;
  private final IZkDataListener containerAssignmentPathListener;

  // TODO should take ZK connect, and session timeouts, and whatnot.
  // StandaloneJobFactory should yank this from configs. Same for container
  // controller.
  public StandaloneZkCoordinatorController(Config config, String zkConnect, ZkClient zkClient) {
    this(config, zkConnect, zkClient, new StandaloneZkCoordinatorState());
  }

  public StandaloneZkCoordinatorController(Config config, String zkConnect, ZkClient zkClient, StandaloneZkCoordinatorState state) {
    this.config = config;
    this.zkConnect = zkConnect;
    this.zkClient = zkClient;
    this.state = state;
    this.coordinatorStateListener = new CoordinatorStateListener();
    this.coordinatorPathListener = new CoordinatorPathListener();
    this.containerPathListener = new ContainerPathListener();
    this.containerAssignmentPathListener = new ContainerAssignmentPathListener();
  }

  public synchronized void start() {
    if (!state.isRunning()) {
      log.info("Starting coordinator controller.");
      zkClient.waitUntilConnected();
      zkClient.subscribeStateChanges(coordinatorStateListener);
      ZkUtil.setupZkEnvironment(zkConnect);
      zkClient.createPersistent(COORDINATOR_PATH, true);
      zkClient.createPersistent(CONTAINER_PATH, true);
      if (state.getCoordinatorSequentialId() == null) {
        state.setCoordinatorSequentialId(new File(zkClient.createEphemeralSequential(COORDINATOR_PATH + "/", Collections.emptyMap())).getName());
      }
      checkLeadership(zkClient.subscribeChildChanges(COORDINATOR_PATH, coordinatorPathListener));
      log.debug("Finished starting coordinator controller.");
      state.setRunning(true);
    } else {
      log.debug("Attempting to start a coordinator controller that's already been started. Ignoring.");
    }
  }

  public synchronized void pause() {
    if (state.isRunning()) {
      log.info("Stopping coordinator controller.");
      zkClient.unsubscribeChildChanges(COORDINATOR_PATH, coordinatorPathListener);
      zkClient.unsubscribeChildChanges(CONTAINER_PATH, containerPathListener);
      for (String containerSequentialId : state.getContainerIds()) {
        zkClient.unsubscribeDataChanges(CONTAINER_PATH + "/" + containerSequentialId, containerAssignmentPathListener);
      }
      JobCoordinator coordinator = state.getJobCoordinator();
      if (coordinator != null) {
        coordinator.stop();
      }
      state.clear();
      log.debug("Finished stopping coordinator controller.");
    } else {
      log.debug("Attempting to pause a coordinator controller that's already been paused. Ignoring.");
    }
  }

  public void stop() {
    this.pause();
    // TODO should delete ephemeral node here.
    // Stop listening to ZK, so we never reconnect.
    zkClient.unsubscribeStateChanges(coordinatorStateListener);
  }

  private synchronized void checkLeadership(List<String> coordinatorSequentialIds) {
    String coordinatorControllerChild = ZkUtil.getLeader(coordinatorSequentialIds);
    if (coordinatorControllerChild != null && coordinatorControllerChild.equals(state.getCoordinatorSequentialId())) {
      log.info("Becoming leader.");
      state.setContainerIds(zkClient.subscribeChildChanges(CONTAINER_PATH, containerPathListener));
      // Listen to existing container sequential IDs to track ownership.
      for (String containerSequentialId : state.getContainerIds()) {
        zkClient.subscribeDataChanges(CONTAINER_PATH + "/" + containerSequentialId, containerAssignmentPathListener);
      }
      refreshOwnership();
      state.setElectedLeader(true);
    }
  }

  private JobModel rebuildJobModel(JobModel idealJobModel, Set<TaskName> strippedTaskNames) {
    if (strippedTaskNames != null && strippedTaskNames.size() > 0) {
      Map<String, ContainerModel> strippedContainers = new HashMap<String, ContainerModel>();
      for (ContainerModel idealContainerModel : idealJobModel.getContainers().values()) {
        String containerId = idealContainerModel.getContainerId();
        Map<TaskName, TaskModel> strippedTaskModels = new HashMap<TaskName, TaskModel>();
        for (TaskModel taskModel : idealContainerModel.getTasks().values()) {
          if (!strippedTaskNames.contains(taskModel.getTaskName())) {
            strippedTaskModels.put(taskModel.getTaskName(), taskModel);
          }
        }
        if (strippedTaskModels.size() > 0) {
          strippedContainers.put(containerId, new ContainerModel(containerId, strippedTaskModels));
        }
      }
      return new JobModel(config, strippedContainers);
    } else {
      return idealJobModel;
    }
  }

  private synchronized void refreshOwnership() {
    if (state.getContainerIds().size() > 0) {
      log.info("Refreshing container ownership.");
      JobModel idealJobModel = JobCoordinator.buildJobModel(config, new HashSet<String>(state.getContainerIds()));
      log.debug("Got ideal job model: {}", idealJobModel);
      Set<TaskName> strippedTaskNames = new HashSet<TaskName>();
      for (String containerId : state.getContainerIds()) {
        ContainerModel idealContainerModel = idealJobModel.getContainers().get(containerId);
        Map<String, List<String>> taskOwnership = zkClient.readData(CONTAINER_PATH + "/" + containerId, true);
        if (taskOwnership != null) {
          List<String> taskNames = taskOwnership.get("tasks");
          if (taskNames != null) {
            for (String taskNameString : taskNames) {
              TaskName taskName = new TaskName(taskNameString);
              if (idealContainerModel == null || !idealContainerModel.getTasks().containsKey(taskName)) {
                log.info("Stripping task: {} from ownership because it's owned by containers id: {}, but it shouldn't be.", taskName, containerId);
                strippedTaskNames.add(taskName);
              }
            }
          }
        }
      }
      JobModel jobModel = rebuildJobModel(idealJobModel, strippedTaskNames);
      log.debug("Got actual job model: {}", jobModel);
      log.debug("Got previous job model: {}", state.getJobModel());
      if (state.getJobModel() == null || !state.getJobModel().equals(jobModel)) {
        log.info("Updating job coordinator, since previous job model differs from actual job model.");
        state.setJobModel(jobModel);
        HttpServer server = JobCoordinator.buildHttpServer(jobModel);
        // This controller is the leader. Start a JobCoordinator and persist its
        // URL to the ephemeral node.
        JobCoordinator jobCoordinator = state.getJobCoordinator();
        if (jobCoordinator != null) {
          jobCoordinator.stop();
        }
        jobCoordinator = new JobCoordinator(jobModel, server);
        jobCoordinator.start();
        state.setJobCoordinator(jobCoordinator);
        Map<String, Object> coordinatorData = new HashMap<String, Object>();
        coordinatorData.put("url", jobCoordinator.server().getUrl().toString());
        zkClient.writeData(COORDINATOR_PATH + "/" + state.getCoordinatorSequentialId(), coordinatorData);
      }
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
      log.warn("Session has expired. Creating a new one.");
      pause();
      // Drop old containerSequentialId since our session has expired. A new one
      // will be created when start is called.
      state.setCoordinatorSequentialId(null);
      start();
    }
  }

  private class CoordinatorPathListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      log.trace("CoordinatorPathListener.handleChildChange with parent path {} and children: {}", parentPath, currentChildren);
      checkLeadership(currentChildren);
    }
  }

  private class ContainerPathListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      log.trace("ContainerPathListener.handleChildChange with parent path {} and children: {}", parentPath, currentChildren);
      Set<String> previousContainerSequentialIds = new HashSet<String>(state.getContainerIds());
      Set<String> newContainerSequentialIds = new HashSet<String>(currentChildren);
      newContainerSequentialIds.removeAll(previousContainerSequentialIds);
      previousContainerSequentialIds.removeAll(currentChildren);
      // Listen to container sequential IDs so we can keep track of ownership.
      log.debug("Listen to new children: {}", newContainerSequentialIds);
      for (String newContainerSequentialId : newContainerSequentialIds) {
        zkClient.subscribeDataChanges(CONTAINER_PATH + "/" + newContainerSequentialId, containerAssignmentPathListener);
      }
      // Stop listening to removed containers.
      log.debug("Unsubscribe old children: {}", previousContainerSequentialIds);
      for (String oldContainerSequentialId : previousContainerSequentialIds) {
        zkClient.unsubscribeDataChanges(CONTAINER_PATH + "/" + oldContainerSequentialId, containerAssignmentPathListener);
      }
      state.setContainerIds(currentChildren);
      refreshOwnership();
    }
  }

  private class ContainerAssignmentPathListener implements IZkDataListener {
    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
      log.trace("ContainerAssignmentPathListener.handleDataChange with data path {} and payload: {}", dataPath, data);
      refreshOwnership();
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
      // TODO do we need to unsubscribe?
    }
  }
}
