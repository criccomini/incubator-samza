package org.apache.samza.coordinator.model;

import java.util.Collections;
import java.util.Set;

public class SamzaModelJob {
  private final Set<SamzaModelContainer> containers;

  public SamzaModelJob(Set<SamzaModelContainer> containers) {
    this.containers = Collections.unmodifiableSet(containers);
  }

  public Set<SamzaModelContainer> getContainers() {
    return containers;
  }
}
