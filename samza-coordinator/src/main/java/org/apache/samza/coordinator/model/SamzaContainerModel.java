package org.apache.samza.coordinator.model;

import java.util.Collections;
import java.util.Set;

public class SamzaContainerModel {
  private final int id;
  private final Set<SamzaTaskModel> tasks;

  public SamzaContainerModel(int id, Set<SamzaTaskModel> tasks) {
    this.id = id;
    this.tasks = Collections.unmodifiableSet(tasks);
  }

  public int getId() {
    return id;
  }

  public Set<SamzaTaskModel> getTasks() {
    return tasks;
  }
}
