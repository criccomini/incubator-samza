package org.apache.samza.job.standalone.controller;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.samza.coordinator.JobCoordinator;

public class StandaloneZkCoordinatorState {
  private List<String> coordinatorSequentialIds;
  private List<String> containerSequentialIds;
  private String coordinatorSequentialId;
  private JobCoordinator jobCoordinator;

  public StandaloneZkCoordinatorState() {
    clear();
  }

  public void setCoordinatorSequentialIds(List<String> coordinatorSequentialIds) {
    this.coordinatorSequentialIds = Collections.unmodifiableList(coordinatorSequentialIds);
  }

  public List<String> getCoordinatorSequentialIds() {
    return coordinatorSequentialIds;
  }

  public void setContainerSequentialIds(List<String> containerSequentialIds) {
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
    coordinatorSequentialIds = Collections.unmodifiableList(new ArrayList<String>());
    containerSequentialIds = Collections.unmodifiableList(new ArrayList<String>());
    coordinatorSequentialId = null;
    jobCoordinator = null;
  }
}
