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

public class StandaloneZkCoordinatorState {
  private List<String> coordinatorSequentialIds;
  private List<String> containerSequentialIds;
  private String coordinatorSequentialId;
  private JobCoordinator jobCoordinator;
  private Map<String, Set<String>> expectedTaskAssignments;

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
    this.expectedTaskAssignments = Collections.unmodifiableMap(unmodifiableMap);
  }

  public void setCoordinatorSequentialIds(List<String> coordinatorSequentialIds) {
    if (coordinatorSequentialIds == null) {
      coordinatorSequentialIds = Collections.emptyList();
    }
    this.coordinatorSequentialIds = Collections.unmodifiableList(coordinatorSequentialIds);
  }

  public List<String> getCoordinatorSequentialIds() {
    return coordinatorSequentialIds;
  }

  public void setContainerSequentialIds(List<String> containerSequentialIds) {
    if (containerSequentialIds == null) {
      containerSequentialIds = Collections.emptyList();
    }
    this.containerSequentialIds = Collections.unmodifiableList(containerSequentialIds);
  }

  public List<String> getContainerSequentialIds() {
    return containerSequentialIds;
  }

  public String getCoordinatorSequentialId() {
    return coordinatorSequentialId;
  }

  public void setCoordinatorSequentialId(String coordinatorSequentialId) {
    this.coordinatorSequentialId = coordinatorSequentialId;
  }

  public JobCoordinator getJobCoordinator() {
    return jobCoordinator;
  }

  public void setJobCoordinator(JobCoordinator jobCoordinator) {
    this.jobCoordinator = jobCoordinator;
  }

  public boolean isLeader() {
    return coordinatorSequentialIds.get(0).equals(coordinatorSequentialId);
  }

  public boolean isLeaderRunning() {
    return isLeader() && jobCoordinator != null;
  }

  public void clear() {
    coordinatorSequentialIds = Collections.unmodifiableList(Collections.emptyList());
    containerSequentialIds = Collections.unmodifiableList(Collections.emptyList());
    expectedTaskAssignments = Collections.unmodifiableMap(Collections.emptyMap());
    coordinatorSequentialId = null;
    jobCoordinator = null;
  }
}
