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

package org.apache.samza.system.mock;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionMetadata;

/**
 * A SystemAdmin that returns a constant set of partitions for all streams.
 */
public class MockSystemAdmin implements SystemAdmin {
  private final String systemName;
  private final Set<Partition> partitions;

  public MockSystemAdmin(String systemName, int partitionCount) {
    this.systemName = systemName;
    this.partitions = new HashSet<Partition>();

    for (int i = 0; i < partitionCount; ++i) {
      partitions.add(new Partition(i));
    }
  }

  @Override
  public Map<SystemStreamPartition, SystemStreamPartitionMetadata> getSystemStreamPartitionMetadata(Set<String> streamNames) {
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> metadata = new HashMap<SystemStreamPartition, SystemStreamPartitionMetadata>();

    for (Partition partition : partitions) {
      for (String streamName : streamNames) {
        SystemStreamPartition systemStreamPartition = new SystemStreamPartition(systemName, streamName, partition);
        metadata.put(systemStreamPartition, new SystemStreamPartitionMetadata(null, null, null));
      }
    }

    return metadata;
  }
}
