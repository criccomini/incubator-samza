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

package org.apache.samza.coordinator.stream;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;
import org.apache.samza.util.Util;

public class MockCoordinatorStreamSystemFactory implements SystemFactory {
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    String jobName = config.get("job.name");
    String jobId = config.get("job.id");
    if (jobName == null) {
      throw new ConfigException("Must define job.name.");
    }
    if (jobId == null) {
      jobId = "1";
    }
    String streamName = Util.getCoordinatorStreamName(jobName, jobId);
    SystemStreamPartition systemStreamPartition = new SystemStreamPartition(systemName, streamName, new Partition(0));
    return new MockCoordinatorStreamWrappedConsumer(systemStreamPartition, config);
  }

  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    throw new UnsupportedOperationException();
  }

  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SinglePartitionWithoutOffsetsSystemAdmin();
  }
}
