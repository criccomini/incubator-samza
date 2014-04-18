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

import org.apache.samza.serializers.SerdeManager
import grizzled.slf4j.Logging
import org.apache.samza.system.chooser.MessageChooser
import java.util.Queue
import java.util.Set
import java.util.HashMap
import java.util.ArrayDeque
import java.util.HashSet
import scala.collection.JavaConversions._

/**
 * The SystemConsumers class coordinates between all SystemConsumers, the
 * MessageChooser, and the SamzaContainer. Its job is to poll each
 * SystemConsumer for messages, update the
 * {@link org.apache.samza.system.chooser.MessageChooser} with new incoming
 * messages, poll the MessageChooser for the next message to process, and
 * return that message to the SamzaContainer.
 */
class SystemConsumers(

  /**
   * The class that determines the order to process incoming messages.
   */
  chooser: MessageChooser,

  /**
   * A map of SystemConsumers that should be polled for new messages.
   */
  consumers: Map[String, SystemConsumer],

  /**
   * The class that handles deserialization of incoming messages.
   */
  serdeManager: SerdeManager = new SerdeManager,

  /**
   * A helper class to hold all of SystemConsumers' metrics.
   */
  metrics: SystemConsumersMetrics = new SystemConsumersMetrics,

  /**
   * If MessageChooser returns null when it's polled, SystemConsumers will
   * poll each SystemConsumer with a timeout next time it tries to poll for
   * messages. Setting the timeout to 0 means that SamzaContainer's main
   * thread will sit in a tight loop polling every SystemConsumer over and
   * over again if no new messages are available.
   */
  noNewMessagesTimeout: Int = 10) extends Logging {

  /**
   * A buffer of incoming messages grouped by SystemStreamPartition.
   */
  val unprocessedMessages = new HashMap[SystemStreamPartition, Queue[IncomingMessageEnvelope]]()

  /**
   * A simple count of all unprocessed messages that are currently sitting in
   * either the MessageChooser or the unprocessedMessages buffer. The count is
   * used to determine when to refresh the buffer with more messages by
   * fetching from underlying systems.
   */
  var totalUnprocessedMessages = 0

  /**
   * A set of SystemStreamPartitions grouped by systemName. This is used as a
   * cache to figure out which SystemStreamPartitions we need to poll from the
   * underlying system consumer.
   */
  val emptySystemStreamPartitionsBySystem = new HashMap[String, Set[SystemStreamPartition]]()

  /**
   * Default timeout to noNewMessagesTimeout. Every time SystemConsumers
   * receives incoming messages, it sets timout to 0. Every time
   * SystemConsumers receives no new incoming messages from the MessageChooser,
   * it sets timeout to noNewMessagesTimeout again.
   */
  var timeout = noNewMessagesTimeout

  debug("Got stream consumers: %s" format consumers)

  metrics.setUnprocessedMessages(() => totalUnprocessedMessages)

  def start {
    debug("Starting consumers.")

    unprocessedMessages
      .keySet
      .groupBy(_.getSystem)
      .foreach {
        case (systemName, systemStreamPartitions) =>
          emptySystemStreamPartitionsBySystem.put(systemName, systemStreamPartitions)
      }

    consumers
      .keySet
      .foreach(metrics.registerSystem)

    consumers.values.foreach(_.start)

    chooser.start
  }

  def stop {
    debug("Stopping consumers.")

    consumers.values.foreach(_.stop)

    chooser.stop
  }

  def register(systemStreamPartition: SystemStreamPartition, offset: String) {
    debug("Registering stream: %s, %s" format (systemStreamPartition, offset))
    metrics.registerSystemStream(systemStreamPartition.getSystemStream)
    unprocessedMessages.put(systemStreamPartition, new ArrayDeque[IncomingMessageEnvelope]())
    consumers(systemStreamPartition.getSystem).register(systemStreamPartition, offset)
    chooser.register(systemStreamPartition, offset)
  }

  def choose: IncomingMessageEnvelope = {
    val envelopeFromChooser = chooser.choose

    if (envelopeFromChooser == null) {
      debug("Chooser returned null.")

      metrics.choseNull.inc

      // Sleep for a while so we don't poll in a tight loop.
      timeout = noNewMessagesTimeout
    } else {
      val systemStreamPartition = envelopeFromChooser.getSystemStreamPartition

      debug("Chooser returned an incoming message envelope: %s" format envelopeFromChooser)

      metrics.choseObject.inc

      timeout = 0
      totalUnprocessedMessages -= 1

      // Ok to give the chooser a new message from this stream.
      val q = unprocessedMessages.get(systemStreamPartition)

      if (q.size > 0) {
        chooser.update(q.poll)
      } else {
        emptySystemStreamPartitionsBySystem.get(systemStreamPartition.getSystem).add(systemStreamPartition)
      }
    }

    // TODO should make refresh threshold configurable
    if (envelopeFromChooser == null || totalUnprocessedMessages < 1000) {
      refresh
    }

    envelopeFromChooser
  }

  /**
   * Poll a system for new messages from SystemStreamPartitions that have
   * dipped below the depletedQueueSizeThreshold threshold.  Return true if
   * any envelopes were found, false if none.
   */
  private def poll(systemName: String) {
    debug("Polling system consumer: %s" format systemName)

    metrics.systemPolls(systemName).inc

    val consumer = consumers(systemName)

    debug("Getting fetch map for system: %s" format systemName)

    val systemFetchSet = emptySystemStreamPartitionsBySystem.get(systemName)

    debug("Fetching: %s" format systemFetchSet)

    metrics.systemStreamPartitionFetchesPerPoll(systemName).inc(systemFetchSet.size)

    val systemStreamPartitionEnvelopes = consumer.poll(systemFetchSet, timeout)

    debug("Got incoming message envelopes: %s" format systemStreamPartitionEnvelopes)

    metrics.systemMessagesPerPoll(systemName).inc

    val sspAndEnvelopeIterator = systemStreamPartitionEnvelopes.entrySet.iterator

    while (sspAndEnvelopeIterator.hasNext) {
      val sspAndEnvelope = sspAndEnvelopeIterator.next
      val systemStreamPartition = sspAndEnvelope.getKey
      val envelopes = sspAndEnvelope.getValue
      val numEnvelopes = envelopes.size

      if (numEnvelopes > 0) {
        totalUnprocessedMessages += numEnvelopes

        if (emptySystemStreamPartitionsBySystem.get(systemStreamPartition.getSystem).remove(systemStreamPartition)) {
          chooser.update(envelopes.poll)
        }

        unprocessedMessages.put(systemStreamPartition, envelopes)
      }
    }
  }

  private def refresh {
    debug("Refreshing chooser with new messages.")

    // Poll every system for new messages.
    consumers.keys.map(poll(_))
  }
}
