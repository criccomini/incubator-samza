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

package org.apache.samza.system;

import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;

public class SystemStreamMetadata {
  private final String streamName;
  private final Set<Partition> partitions;
  private final Map<Partition, String> earliestOffsets;
  private final Map<Partition, String> latestOffsets;
  private final Map<Partition, String> nextOffsets;

  public SystemStreamMetadata(String streamName, Set<Partition> partitions, Map<Partition, String> earliestOffsets, Map<Partition, String> latestOffsets, Map<Partition, String> nextOffsets) {
    this.streamName = streamName;
    this.partitions = partitions;
    this.earliestOffsets = earliestOffsets;
    this.latestOffsets = latestOffsets;
    this.nextOffsets = nextOffsets;
  }

  public Set<Partition> getPartitions() {
    return partitions;
  }

  /**
   * 
   * @return Earliest offset, or null if stream is empty.
   */
  public String getEarliestOffset(Partition partition) {
    return earliestOffsets.get(partition);
  }

  /**
   * 
   * @return Latest offset, or null if stream is empty.
   */
  public String getLatestOffset(Partition partition) {
    return latestOffsets.get(partition);
  }

  /**
   * 
   * @return Next offset, or null if stream doesn't support offsets.
   */
  public String getNextOffset(Partition partition) {
    return nextOffsets.get(partition);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((earliestOffsets == null) ? 0 : earliestOffsets.hashCode());
    result = prime * result + ((latestOffsets == null) ? 0 : latestOffsets.hashCode());
    result = prime * result + ((nextOffsets == null) ? 0 : nextOffsets.hashCode());
    result = prime * result + ((streamName == null) ? 0 : streamName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SystemStreamMetadata other = (SystemStreamMetadata) obj;
    if (earliestOffsets == null) {
      if (other.earliestOffsets != null)
        return false;
    } else if (!earliestOffsets.equals(other.earliestOffsets))
      return false;
    if (latestOffsets == null) {
      if (other.latestOffsets != null)
        return false;
    } else if (!latestOffsets.equals(other.latestOffsets))
      return false;
    if (nextOffsets == null) {
      if (other.nextOffsets != null)
        return false;
    } else if (!nextOffsets.equals(other.nextOffsets))
      return false;
    if (streamName == null) {
      if (other.streamName != null)
        return false;
    } else if (!streamName.equals(other.streamName))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "StreamMetadata [streamName=" + streamName + ", earliestOffsets=" + earliestOffsets + ", latestOffsets=" + latestOffsets + ", nextOffsets=" + nextOffsets + "]";
  }
}
