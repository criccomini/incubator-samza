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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.job.model.ContainerModel;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class StandaloneZkCoordinatorController {
  public static final String COORDINATOR_PATH = "/coordinator";
  public static final String CONTAINER_PATH = "/containers";
  public static final String ASSIGNMENTS_PATH = "/assignments";
  public static final String COORDINATOR_URL_KEY = "__url";
  private final Config config;
  private final ZkClient zkClient;
  private final StandaloneZkCoordinatorState state;

  public StandaloneZkCoordinatorController(Config config, ZkClient zkClient) {
    this(config, zkClient, new StandaloneZkCoordinatorState());
  }

  public StandaloneZkCoordinatorController(Config config, ZkClient zkClient, StandaloneZkCoordinatorState state) {
    this.config = config;
    this.zkClient = zkClient;
    this.state = state;
  }

  public void start() {
    zkClient.subscribeStateChanges(new CoordinatorStateListener());
    zkClient.waitUntilConnected();
    zkClient.createPersistent(COORDINATOR_PATH);
    zkClient.createPersistent(CONTAINER_PATH);
    // TODO only do this if it doesn't exist.
    zkClient.createPersistent(ASSIGNMENTS_PATH, Collections.emptyMap());
    state.setCoordinatorSequentialIds(zkClient.subscribeChildChanges(COORDINATOR_PATH, new CoordinatorPathListener()));
    state.setCoordinatorSequentialId(new File(zkClient.createEphemeralSequential(COORDINATOR_PATH + "/", null)).getName());
  }

  public void stop() {
    JobCoordinator coordinator = state.getJobCoordinator();
    if (coordinator != null) {
      coordinator.stop();
    }
  }

  private void checkLeadership() {
    if (state.isLeader() && !state.isLeaderRunning()) {
      state.setContainerSequentialIds(zkClient.subscribeChildChanges(CONTAINER_PATH, new ContainerPathListener()));
    }
  }

  @SuppressWarnings("unchecked")
  private void assignTasksToContainers() {
    boolean allContainersEmpty = true;
    Map<String, Set<String>> expectedAssignments = state.getExpectedTaskAssignments();
    for (String containerSequentialId : state.getContainerSequentialIds()) {
      List<String> taskAssignments = (List<String>) zkClient.readData(CONTAINER_PATH + "/" + containerSequentialId, true);
      Set<String> expectedContainerTasks = expectedAssignments.get(containerSequentialId);
      // If there are assignments, and a container doesn't match the assignment,
      // then clear everything and start over.
      if (expectedAssignments.size() > 0 && taskAssignments.size() > 0 && !taskAssignments.equals(expectedContainerTasks)) {
        clearAssignments();
        break;
      }
      allContainersEmpty &= taskAssignments.size() == 0;
    }
    // If expected assignments matched container ownership, and assignments are
    // empty, then create a new non-empty assignment for all containers.
    if (allContainersEmpty && expectedAssignments.size() == 0) {
      try {
        setAssignments();
      } catch (Exception e) {
        System.err.println(e);
        e.printStackTrace();
      }
    }
  }

  public void clearAssignments() {
    state.setExpectedTaskAssignments(Collections.emptyMap());
    zkClient.writeData(ASSIGNMENTS_PATH, state.getExpectedTaskAssignments());
  }

  public void setAssignments() {
    Map<String, Object> containerIdAssignments = new HashMap<String, Object>();
    List<String> containerSequentialIds = state.getContainerSequentialIds();
    Map<String, Set<String>> expectedTaskAssignments = new HashMap<String, Set<String>>();
    if (containerSequentialIds.size() > 0) {
      JobCoordinator jobCoordinator = state.getJobCoordinator();
      if (jobCoordinator != null) {
        jobCoordinator.stop();
      }
      jobCoordinator = JobCoordinator.apply(config, containerSequentialIds.size());
      jobCoordinator.start();
      state.setJobCoordinator(jobCoordinator);
      Map<Integer, ContainerModel> containerModels = jobCoordinator.jobModel().getContainers();
      // Build an assignment map from sequential ID to container ID.
      List<Integer> containerIds = new ArrayList<Integer>(containerModels.keySet());
      assert containerIds.size() == containerSequentialIds.size();
      Collections.sort(containerIds);
      Iterator<String> containerSequentialIdsIt = containerSequentialIds.iterator();
      Iterator<Integer> containerIdsIt = containerIds.iterator();
      while (containerSequentialIdsIt.hasNext() && containerIdsIt.hasNext()) {
        String containerSequentialId = containerSequentialIdsIt.next();
        Integer containerId = containerIdsIt.next();
        Set<String> taskNames = new HashSet<String>();
        for (TaskName taskName : containerModels.get(containerId).getTasks().keySet()) {
          taskNames.add(taskName.toString());
        }
        expectedTaskAssignments.put(containerSequentialId, taskNames);
        containerIdAssignments.put(containerSequentialId, containerId);
      }
      containerIdAssignments.put(COORDINATOR_URL_KEY, state.getJobCoordinator().server().getUrl().toString());
    }
    state.setExpectedTaskAssignments(expectedTaskAssignments);
    zkClient.writeData(ASSIGNMENTS_PATH, containerIdAssignments);
  }

  private class CoordinatorStateListener implements IZkStateListener {
    @Override
    public void handleStateChanged(KeeperState zkState) throws Exception {
      if (zkState.equals(KeeperState.Disconnected) || zkState.equals(KeeperState.Expired)) {
        state.clear();
      }
    }

    @Override
    public void handleNewSession() throws Exception {
      // TODO what is this?
    }
  }

  private class CoordinatorPathListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      state.setCoordinatorSequentialIds(currentChildren);
      checkLeadership();
      assignTasksToContainers();
    }
  }

  private class ContainerPathListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      state.setContainerSequentialIds(currentChildren);
      assignTasksToContainers();
    }
  }
}
