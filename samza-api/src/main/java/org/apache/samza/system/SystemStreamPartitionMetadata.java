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

public class SystemStreamPartitionMetadata {
  private final String earliestOffset;
  private final String latestOffset;
  private final String nextOffset;

  public SystemStreamPartitionMetadata(String earliestOffset, String latestOffset, String nextOffset) {
    this.earliestOffset = earliestOffset;
    this.latestOffset = latestOffset;
    this.nextOffset = nextOffset;
  }

  /**
   * 
   * @return Earliest offset, or null if stream is empty.
   */
  public String getEarliestOffset() {
    return earliestOffset;
  }

  /**
   * 
   * @return Latest offset, or null if stream is empty.
   */
  public String getLatestOffset() {
    return latestOffset;
  }

  /**
   * 
   * @return Next offset, or null if stream doesn't support offsets.
   */
  public String getNextOffset() {
    return nextOffset;
  }
}
