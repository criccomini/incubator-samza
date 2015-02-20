package org.apache.samza.job.standalone.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.JobCoordinator;
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
    state.setCoordinatorSequentialIds(zkClient.subscribeChildChanges(COORDINATOR_PATH, new CoordinatorPathListener()));
    state.setCoordinatorSequentialId(zkClient.createEphemeralSequential(COORDINATOR_PATH, null));
  }

  private void checkLeadership() {
    if (state.isLeader()) {
      if (!state.isLeaderRunning()) {
        state.setContainerSequentialIds(zkClient.subscribeChildChanges(CONTAINER_PATH, new ContainerPathListener()));
      }
      assignTasksToContainers();
    }
  }

  // TODO this needs work. can't just update assignments. has to be two phase:
  // first revoke, then check /containers for ownership. then shift.
  private void assignTasksToContainers() {
    // TODO should be able to do this without bouncing the entire coordinator.
    if (state.getJobCoordinator() != null) {
      state.getJobCoordinator().stop();
    }
    // TODO update JobCoordinator.buildJobModel to take a previousJobModel to
    // allow it to minimize partition shifting when a container dies. See
    // GroupByContainerCount for a possible injection point.
    state.setJobCoordinator(JobCoordinator.apply(config, state.getContainerSequentialIds().size()));
    state.getJobCoordinator().start();
    List<Integer> containerIds = new ArrayList<Integer>(state.getJobCoordinator().jobModel().getContainers().keySet());
    List<String> containerSequentialIds = state.getContainerSequentialIds();
    // Build an assignment map from sequential ID to container ID.
    assert containerIds.size() == containerSequentialIds.size();
    Collections.sort(containerIds);
    Map<String, Object> containerAssignments = new HashMap<String, Object>();
    Iterator<String> containerSequentialIdsIt = containerSequentialIds.iterator();
    Iterator<Integer> containerIdsIt = containerIds.iterator();
    while (containerSequentialIdsIt.hasNext() && containerIdsIt.hasNext()) {
      containerAssignments.put(containerSequentialIdsIt.next(), containerIdsIt.next());
    }
    // TODO this is hacky.
    containerAssignments.put(COORDINATOR_URL_KEY, state.getJobCoordinator().server().getUrl().toString());
    zkClient.createPersistent(ASSIGNMENTS_PATH, containerAssignments);
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
    }
  }

  private class ContainerPathListener implements IZkChildListener {
    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
      state.setContainerSequentialIds(currentChildren);
      checkLeadership();
    }
  }
}
