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

package org.apache.samza.job.standalone;

import java.util.List;
import java.util.Set;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.SamzaException;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.local.ThreadJob;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class StandaloneJobController {
  private final ZkClient zkClient;
  private int containerId = -1;
  private ThreadJob threadJob;
  private JobModel jobModel;

  public StandaloneJobController() {
    zkClient = new ZkClient("");
    zkClient.subscribeStateChanges(new IZkStateListener() {
      @Override
      public void handleStateChanged(KeeperState state) throws Exception {
        if (state.equals(KeeperState.Disconnected) || state.equals(KeeperState.Expired)) {
          onZooKeeperFailure();
        } else {
          onZooKeeperConnect();
        }
      }

      @Override
      public void handleNewSession() throws Exception {
        // TODO what is this?
      }
    });
  }

  /**
   * Triggered when a container joins or updates their task ownership.
   */
  private void onContainerUpdated() {
    // /containers
    // TODO compute expected task ownership for all containers.
    // TODO if there's a diff with assignments, shift assignments.
  }

  private void onContainerFailed() {
    // /containers
  }

  private void onTaskAssignment() {
    // /assignments
    stopThreadJob();
    startThreadJob();
    announceOwnership();
  }

  private void onCoordinatorElected() {
    // /coordinator
    stopCoordinator();
    startCoordinator();
    refreshCoordinatorPath();
  }

  private void onZooKeeperConnect() {
    electCoordinator();
  }

  private void onZooKeeperFailure() {
    stopCoordinator();
    stopThreadJob();
  }

  private void startThreadJob() {
    if (containerId < 0) {
      throw new SamzaException("Can't create a new container without an assigned container ID.");
    }
    if (threadJob != null && threadJob.getStatus().equals(ApplicationStatus.Running)) {
      throw new SamzaException("Weird state. Can't start a container when one is already running.");
    }
    jobModel = SamzaContainer.readJobModel("TODO need HTTP URL for coordinator");
    ContainerModel containerModel = jobModel.getContainers().get(containerId);
    threadJob = new ThreadJob(SamzaContainer.apply(containerModel, jobModel.getConfig()));
    threadJob.submit();
  }

  private void stopThreadJob() {
    int timeout = 60000;
    threadJob.kill();
    threadJob.waitForFinish(timeout);
    if (!threadJob.getStatus().equals(ApplicationStatus.UnsuccessfulFinish) && !threadJob.getStatus().equals(ApplicationStatus.SuccessfulFinish)) {
      throw new SamzaException("Unable to successfully stop container after " + timeout + " seconds.");
    }
  }

  private void announceOwnership() {
    // TODO tell ZK that the container now owns a bunch of tasks.
    Set<TaskName> taskNames = jobModel.getContainers().get(containerId).getTasks().keySet();
  }

  //
  // ZK
  //

  private void electCoordinator() {
    String path = zkClient.createEphemeralSequential("/coordinator/coordinator", null);
    List<String> children = zkClient.getChildren("/coordinator");
    String prior = null; // TOOD prior = child right before sequential ephemeral
                         // path. chained watchers.
    zkClient.subscribeDataChanges(prior, new IZkDataListener() {
      @Override
      public void handleDataChange(String dataPath, Object data) throws Exception {
      }

      @Override
      public void handleDataDeleted(String dataPath) throws Exception {
      }
    });
    // TODO subscribe to data node instead.
    List<String> children = zkClient.subscribeChildChanges("/coordinator", new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        onCoordinatorElected();
      }
    });
    // TODO if path == min(children), isLeader = true
  }
}
