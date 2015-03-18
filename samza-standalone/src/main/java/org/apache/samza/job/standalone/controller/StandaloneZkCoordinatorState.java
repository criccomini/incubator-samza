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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.coordinator.JobCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO can we get rid of this and just move it into StandaloneZkCoordinatorController?

public class StandaloneZkCoordinatorState {
  private static final Logger log = LoggerFactory.getLogger(StandaloneZkCoordinatorState.class);

  private volatile List<String> containerSequentialIds;
  private volatile String coordinatorSequentialId;
  private volatile JobCoordinator jobCoordinator;
  private volatile Map<String, Set<String>> expectedTaskAssignments;
  private volatile boolean running;
  private volatile boolean electedLeader;

  public StandaloneZkCoordinatorState() {
    clear();
  }

  public Map<String, Set<String>> getExpectedTaskAssignments() {
    return expectedTaskAssignments;
  }

  public void setExpectedTaskAssignments(Map<String, Set<String>> expectedTaskAssignments) {
    Map<String, Set<String>> unmodifiableMap = new HashMap<String, Set<String>>(expectedTaskAssignments);
    for (String key : unmodifiableMap.keySet()) {
      unmodifiableMap.put(key, Collections.unmodifiableSet(unmodifiableMap.get(key)));
    }
    log.debug("Setting expected task assignments: {}", unmodifiableMap);
    this.expectedTaskAssignments = Collections.unmodifiableMap(unmodifiableMap);
  }

  public void setContainerSequentialIds(List<String> containerSequentialIds) {
    if (containerSequentialIds == null) {
      containerSequentialIds = Collections.emptyList();
    }
    log.debug("Setting container sequential IDs: {}", containerSequentialIds);
    this.containerSequentialIds = Collections.unmodifiableList(containerSequentialIds);
  }

  public List<String> getContainerSequentialIds() {
    return containerSequentialIds;
  }

  public String getCoordinatorSequentialId() {
    return coordinatorSequentialId;
  }

  public void setCoordinatorSequentialId(String coordinatorSequentialId) {
    log.debug("Setting container sequential ID: {}", coordinatorSequentialId);
    this.coordinatorSequentialId = coordinatorSequentialId;
  }

  public JobCoordinator getJobCoordinator() {
    return jobCoordinator;
  }

  public void setJobCoordinator(JobCoordinator jobCoordinator) {
    this.jobCoordinator = jobCoordinator;
  }

  public void setElectedLeader(boolean electedLeader) {
    this.electedLeader = electedLeader;
  }

  public boolean isElectedLeader() {
    return electedLeader;
  }

  public boolean isLeaderRunning() {
    return isElectedLeader() && jobCoordinator != null;
  }

  public boolean isRunning() {
    return running;
  }

  public void setRunning(boolean running) {
    this.running = running;
  }

  public void clear() {
    log.info("Clearing all coordinator state.");
    // Don't clear coordinator sequential ID. Try and re-use it.
    containerSequentialIds = Collections.unmodifiableList(Collections.emptyList());
    expectedTaskAssignments = Collections.unmodifiableMap(Collections.emptyMap());
    jobCoordinator = null;
    running = false;
    electedLeader = false;
  }
}
