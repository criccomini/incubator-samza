package org.apache.samza.system.mock;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamPartition;

/**
 * A SystemAdmin that returns a constant set of partitions for all streams.
 */
public class MockSystemAdmin implements SystemAdmin {
  private final Set<Partition> partitions;

  public MockSystemAdmin(int partitionCount) {
    this.partitions = new HashSet<Partition>();

    for (int i = 0; i < partitionCount; ++i) {
      partitions.add(new Partition(i));
    }
  }

  @Override
  public Set<Partition> getPartitions(String streamName) {
    // Partitions are immutable, so making the set immutable should make the
    // partition set fully safe to re-use.
    return Collections.unmodifiableSet(partitions);
  }

  @Override
  public Map<SystemStreamPartition, String> getLastOffsets(Set<String> streams) {
    throw new RuntimeException("MockSystemAdmin doesn't implement this method.");
  }
}
