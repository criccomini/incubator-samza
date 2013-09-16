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

package org.apache.samza.system

import org.apache.samza.metrics.MetricsRegistry
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.metrics.Counter
import org.apache.samza.metrics.MetricsHelper

class SystemConsumersMetrics(val registry: MetricsRegistry = new MetricsRegistryMap) extends MetricsHelper {
  val choseNull = newCounter("chose-null")
  val choseObject = newCounter("chose-object")
  var registeredSystemNames = Set[String]()

  def setUnprocessedMessages(getValue: () => Int) {
    newGauge("unprocessed-messages", getValue)
  }

  def setNeededByChooser(getValue: () => Int) {
    newGauge("ssps-needed-by-chooser", getValue)
  }

  def setTimeout(getValue: () => Long) {
    newGauge("poll-timeout", getValue)
  }

  def setMaxMessagesPerStreamPartition(getValue: () => Int) {
    newGauge("max-buffered-messages-per-stream-partition", getValue)
  }

  def setNoNewMessagesTimeout(getValue: () => Long) {
    newGauge("blocking-poll-timeout", getValue)
  }

  def registerSystem(systemName: String) {
    if (!systemName.contains(systemName)) {
      registeredSystemNames += systemName
      newCounter("%s-polls" format systemName)
      newCounter("%s-ssp-fetches-per-poll" format systemName)
      newCounter("%s-messages-per-poll" format systemName)
    }
  }
}