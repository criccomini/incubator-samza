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

package org.apache.samza.checkpoint;

import java.util.Collections;
import java.util.Map;

import org.apache.samza.system.SystemStream;

/**
 * Used to represent a checkpoint in the running of a Samza system.
 */
public class Checkpoint {
  private final Map<SystemStream, String> offsets;

  /**
   * Constructs a new checkpoint based off a map of Samza stream offsets.
   * @param offsets Map of Samza streams to their current offset.
   */
  public Checkpoint(Map<SystemStream, String> offsets) {
    this.offsets = offsets;
  }

  /**
   * Gets a unmodifiable view of the next Samza stream offsets.
   * @return A unmodifiable view of a Map of Samza streams to their recorded next offsets.
   */
  public Map<SystemStream, String> getOffsets() {
    return Collections.unmodifiableMap(offsets);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((offsets == null) ? 0 : offsets.hashCode());
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
    Checkpoint other = (Checkpoint) obj;
    if (offsets == null) {
      if (other.offsets != null)
        return false;
    } else if (!offsets.equals(other.offsets))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "Checkpoint [offsets=" + offsets + "]";
  }
}
