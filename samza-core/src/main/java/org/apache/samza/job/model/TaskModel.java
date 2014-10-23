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

package org.apache.samza.job.model;

import java.util.Collections;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

public class TaskModel implements Comparable<TaskModel> {
  private final TaskName taskName;
  private final Set<SystemStreamPartition> inputSystemStreamPartitions;
  private final Partition changelogPartition;

  public TaskModel(TaskName taskName, Set<SystemStreamPartition> inputSystemStreamPartitions, Partition changelogPartition) {
    this.taskName = taskName;
    this.inputSystemStreamPartitions = Collections.unmodifiableSet(inputSystemStreamPartitions);
    this.changelogPartition = changelogPartition;
  }

  public TaskName getTaskName() {
    return taskName;
  }

  public Set<SystemStreamPartition> getInputSystemStreamPartitions() {
    return inputSystemStreamPartitions;
  }

  public Partition getChangelogPartition() {
    return changelogPartition;
  }

  @Override
  public String toString() {
    return "TaskModel [taskName=" + taskName + ", inputSystemStreamPartitions=" + inputSystemStreamPartitions + ", changeLogPartition=" + changelogPartition + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((taskName == null) ? 0 : taskName.hashCode());
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
    TaskModel other = (TaskModel) obj;
    if (taskName == null) {
      if (other.taskName != null)
        return false;
    } else if (!taskName.equals(other.taskName))
      return false;
    return true;
  }

  public int compareTo(TaskModel other) {
    return taskName.compareTo(other.getTaskName());
  }
}
