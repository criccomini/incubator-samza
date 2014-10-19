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

package org.apache.samza.coordinator.server;

import java.util.HashMap;
import java.util.Map;

import org.apache.samza.coordinator.model.SamzaModelContainer;
import org.apache.samza.coordinator.model.SamzaModelJob;
import org.apache.samza.coordinator.model.SamzaModelTask;

@SuppressWarnings("serial")
public class ServletTaskMapping extends ServletBase {
  private final Map<String, Integer> mapping;

  public ServletTaskMapping(SamzaModelJob jobModel) {
    this.mapping = new HashMap<String, Integer>();

    for (SamzaModelContainer contianer : jobModel.getContainers()) {
      for (SamzaModelTask task : contianer.getTasks()) {
        this.mapping.put(task.getTaskName().toString(), task.getChangelogPartition().getPartitionId());
      }
    }
  }

  protected Object getObjectToWrite() {
    return mapping;
  }
}