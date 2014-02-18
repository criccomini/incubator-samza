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

/**
 * SystemAdmins use this class to return useful metadata about a stream's offset
 * and partition information.
 */
public class SystemStreamMetadata {
  private final String streamName;
  private final Set<Partition> partitions;
  private final Map<Partition, String> oldestOffsets;
  private final Map<Partition, String> newestOffsets;
  private final Map<Partition, String> futureOffsets;

  public SystemStreamMetadata(String streamName, Set<Partition> partitions, Map<Partition, String> oldestOffsets, Map<Partition, String> newestOffsets, Map<Partition, String> futureOffsets) {
    this.streamName = streamName;
    this.partitions = partitions;
    this.oldestOffsets = oldestOffsets;
    this.newestOffsets = newestOffsets;
    this.futureOffsets = futureOffsets;
  }

  /**
   * @return The stream name that's associated with the metadata contained in an
   *         instance of this class.
   */
  public String getStreamName() {
    return streamName;
  }

  /**
   * @return A set of partitions that exist for the stream.
   */
  public Set<Partition> getPartitions() {
    return partitions;
  }

  /**
   * @return The oldest offset that still exists in the stream for the partition
   *         given. If a partition has two messages with offsets 0 and 1,
   *         respectively, then this method would return 0 for the oldest
   *         offset. This offset is useful when one wishes to read all messages
   *         in a stream from the very beginning.
   */
  public String getOldestOffset(Partition partition) {
    return oldestOffsets.get(partition);
  }

  /**
   * @return The newest offset that exists in the stream for the partition
   *         given. If a partition has two messages with offsets 0 and 1,
   *         respectively, then this method would return 1 for the newest
   *         offset. This offset is useful when one wishes to see if all
   *         messages have been read from a stream (offset of last message read
   *         == newest offset).
   */
  public String getNewestOffset(Partition partition) {
    return newestOffsets.get(partition);
  }

  /**
   * @return The offset that represents the next message to be written in the
   *         stream for the partition given. If a partition has two messages
   *         with offsets 0 and 1, respectively, then this method would return 2
   *         for the future offset. This offset is useful when one wishes to
   *         pick up reading at the very end of a stream.
   */
  public String getFutureOffset(Partition partition) {
    return futureOffsets.get(partition);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((futureOffsets == null) ? 0 : futureOffsets.hashCode());
    result = prime * result + ((newestOffsets == null) ? 0 : newestOffsets.hashCode());
    result = prime * result + ((oldestOffsets == null) ? 0 : oldestOffsets.hashCode());
    result = prime * result + ((partitions == null) ? 0 : partitions.hashCode());
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
    if (futureOffsets == null) {
      if (other.futureOffsets != null)
        return false;
    } else if (!futureOffsets.equals(other.futureOffsets))
      return false;
    if (newestOffsets == null) {
      if (other.newestOffsets != null)
        return false;
    } else if (!newestOffsets.equals(other.newestOffsets))
      return false;
    if (oldestOffsets == null) {
      if (other.oldestOffsets != null)
        return false;
    } else if (!oldestOffsets.equals(other.oldestOffsets))
      return false;
    if (partitions == null) {
      if (other.partitions != null)
        return false;
    } else if (!partitions.equals(other.partitions))
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
    return "SystemStreamMetadata [streamName=" + streamName + ", partitions=" + partitions + ", oldestOffsets=" + oldestOffsets + ", newestOffsets=" + newestOffsets + ", futureOffsets=" + futureOffsets + "]";
  }
}
