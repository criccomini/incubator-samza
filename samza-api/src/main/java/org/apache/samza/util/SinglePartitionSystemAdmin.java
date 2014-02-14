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

package org.apache.samza.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionMetadata;

/**
 * A simple helper admin class that defines a single partition (partition 0) for
 * a given system. The metadata uses null for all offsets, which means that the
 * stream doesn't support offsets, and will be treated as empty. This class
 * should be used when a system has no concept of partitioning or offsets, since
 * Samza needs at least one partition for an input stream, in order to read it.
 */
public class SinglePartitionSystemAdmin implements SystemAdmin {
  private final String systemName;

  public SinglePartitionSystemAdmin(String systemName) {
    this.systemName = systemName;
  }

  @Override
  public Map<SystemStreamPartition, SystemStreamPartitionMetadata> getSystemStreamPartitionMetadata(Set<String> streamNames) {
    Map<SystemStreamPartition, SystemStreamPartitionMetadata> metadata = new HashMap<SystemStreamPartition, SystemStreamPartitionMetadata>();

    for (String streamName : streamNames) {
      SystemStreamPartition systemStreamPartition = new SystemStreamPartition(systemName, streamName, new Partition(0));
      metadata.put(systemStreamPartition, new SystemStreamPartitionMetadata(null, null, null));
    }

    return metadata;
  }

  public String getSystemName() {
    return systemName;
  }

  @Override
  public String toString() {
    return "SinglePartitionSystemAdmin [systemName=" + systemName + "]";
  }
}
