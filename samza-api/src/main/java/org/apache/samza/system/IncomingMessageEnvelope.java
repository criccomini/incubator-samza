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

/**
 * This class represents a message envelope that is received by a StreamTask for
 * each message that is received from a partition of a specific input stream.
 */
public class IncomingMessageEnvelope {
  private final SystemStreamPartition systemStreamPartition;
  private final String currentOffset;
  private final String nextOffset;
  private final Object key;
  private final Object message;

  /**
   * Constructs a new IncomingMessageEnvelope from specified components.
   * 
   * @param systemStreamPartition
   *          The aggregate object representing the incoming stream name, the
   *          name of the cluster from which the stream came, and the partition
   *          of the stream from which the message was received.
   * @param currentOffset
   *          The offset in the partition that the message was received from.
   * @param nextOffset
   *          The offset in the partition for the next message to be read after
   *          this message.
   * @param key
   *          A deserialized key received from the partition offset.
   * @param message
   *          A deserialized message received from the partition offset.
   */
  public IncomingMessageEnvelope(SystemStreamPartition systemStreamPartition, String currentOffset, String nextOffset, Object key, Object message) {
    this.systemStreamPartition = systemStreamPartition;
    this.currentOffset = currentOffset;
    this.nextOffset = nextOffset;
    this.key = key;
    this.message = message;
  }

  public SystemStreamPartition getSystemStreamPartition() {
    return systemStreamPartition;
  }

  public Object getKey() {
    return key;
  }

  public Object getMessage() {
    return message;
  }

  public String getCurrentOffset() {
    return currentOffset;
  }

  public String getNextOffset() {
    return nextOffset;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((currentOffset == null) ? 0 : currentOffset.hashCode());
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + ((message == null) ? 0 : message.hashCode());
    result = prime * result + ((nextOffset == null) ? 0 : nextOffset.hashCode());
    result = prime * result + ((systemStreamPartition == null) ? 0 : systemStreamPartition.hashCode());
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
    IncomingMessageEnvelope other = (IncomingMessageEnvelope) obj;
    if (currentOffset == null) {
      if (other.currentOffset != null)
        return false;
    } else if (!currentOffset.equals(other.currentOffset))
      return false;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equals(other.key))
      return false;
    if (message == null) {
      if (other.message != null)
        return false;
    } else if (!message.equals(other.message))
      return false;
    if (nextOffset == null) {
      if (other.nextOffset != null)
        return false;
    } else if (!nextOffset.equals(other.nextOffset))
      return false;
    if (systemStreamPartition == null) {
      if (other.systemStreamPartition != null)
        return false;
    } else if (!systemStreamPartition.equals(other.systemStreamPartition))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "IncomingMessageEnvelope [systemStreamPartition=" + systemStreamPartition + ", currentOffset=" + currentOffset + ", nextOffset=" + nextOffset + ", key=" + key + ", message=" + message + "]";
  }
}
