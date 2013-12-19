package org.apache.samza.system.mock;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamPartition;

public class MockSystemAdmin implements SystemAdmin {
  private final int partitionCount;

  public MockSystemAdmin(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  @Override
  public Set<Partition> getPartitions(String streamName) {
    Set<Partition> partitions = new HashSet<Partition>();

    for (int i = 0; i < partitionCount; ++i) {
      partitions.add(new Partition(i));
    }

    return partitions;
  }

  @Override
  public Map<SystemStreamPartition, String> getLastOffsets(Set<String> streams) {
    throw new RuntimeException("MockSystemAdmin doesn't implement this method.");
  }
}
