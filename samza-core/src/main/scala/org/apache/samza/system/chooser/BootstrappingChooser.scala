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

package org.apache.samza.system.chooser

import java.util.concurrent.atomic.AtomicInteger
import org.apache.samza.system.SystemStream
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.IncomingMessageEnvelope

/**
 * BootstrappingChooser is a composable MessageChooser that only chooses
 * an envelope when it's received at least one envelope for each SystemStream.
 * It does this by only allowing wrapped.choose to be called when the wrapped
 * MessageChooser has been updated with at least on envelope for every
 * SystemStream defined in the latestMessageOffsets map. Thus, the guarantee
 * is that the wrapped chooser will have an envelope from each SystemStream
 * whenever it has to make a choice about which envelope to process next.
 *
 * This behavior continues for each SystemStream that has lagging partitions.
 * As a SystemStream catches up to head, it is no longer marked as lagging,
 * and the requirement that the wrapped chooser have an envelope from the
 * SystemStream is dropped. Once all SystemStreams have caught up, this
 * MessageChooser just becomes a pass-through that always delegates to the
 * wrapped chooser.
 *
 * If a SystemStream falls behind after the initial catch-up, this chooser
 * makes no effort to catch the SystemStream back up, again.
 */
class BootstrappingChooser(
  /**
   * The message chooser that BootstrappingChooser delegates to when it's
   * updating or choosing envelopes.
   */
  wrapped: MessageChooser,

  /**
   * A map from SSP to latest offset for each SSP. If a stream does not need
   * to be guaranteed available to the underlying wrapped chooser, it should
   * not be included in this map.
   */
  var latestMessageOffsets: Map[SystemStreamPartition, String] = Map()) extends BaseMessageChooser {

  /**
   * The number of lagging partitions for each SystemStream that's behind.
   */
  var systemStreamLagCounts = latestMessageOffsets
    .keySet
    .groupBy(_.getSystemStream)
    .mapValues(partitions => new AtomicInteger(partitions.size))

  /**
   * The total number of SystemStreams that are lagging.
   */
  var systemStreamLagSize = systemStreamLagCounts.size

  /**
   * The number of lagging partitions that the underlying wrapped chooser has
   * been updated with, grouped by SystemStream.
   */
  var updatedSystemStreams = scala.collection.mutable.Map[SystemStream, Int]().withDefaultValue(0)

  override def register(systemStreamPartition: SystemStreamPartition, lastReadOffset: String) {
    // If the last offset read is the same as the latest offset in the SSP, 
    // then we're already at head for this SSP, so remove it from the lag list.
    checkOffset(systemStreamPartition, lastReadOffset)
  }

  def update(envelope: IncomingMessageEnvelope) {
    wrapped.update(envelope)

    // If this is an SSP that is still lagging, update the count for the stream.
    if (latestMessageOffsets.contains(envelope.getSystemStreamPartition)) {
      updatedSystemStreams(envelope.getSystemStreamPartition.getSystemStream) += 1
    }
  }

  /**
   * If choose is called, and the parent MessageChoser has received an
   * envelope from at least one partition in each lagging SystemStream, then
   * the choose call is forwarded  to the wrapped chooser. Otherwise, the
   * BootstrappingChooser simply returns null, and waits for more updates.
   */
  def choose = {
    // If no system streams are behind, then go straight to the wrapped chooser.
    if (systemStreamLagSize == 0) {
      wrapped.choose
    } else if (okToChoose) {
      val envelope = wrapped.choose

      if (envelope != null) {
        val systemStreamPartition = envelope.getSystemStreamPartition
        val offset = envelope.getOffset

        if (latestMessageOffsets.contains(systemStreamPartition)) {
          updatedSystemStreams(systemStreamPartition.getSystemStream) -= 1
        }

        checkOffset(systemStreamPartition, offset)
      }

      envelope
    } else {
      null
    }
  }

  private def checkOffset(systemStreamPartition: SystemStreamPartition, offset: String) {
    val latestOffset = latestMessageOffsets.getOrElse(systemStreamPartition, null)
    val systemStream = systemStreamPartition.getSystemStream

    // The SSP is no longer lagging if the envelope's offset equals the 
    // lastOffset map. 
    if (offset != null && offset.equals(latestOffset)) {
      latestMessageOffsets -= systemStreamPartition

      if (systemStreamLagCounts(systemStream).decrementAndGet == 0) {
        // If the lag count is 0, then no partition for this stream is lagging 
        // (the stream has been fully caught up).
        systemStreamLagSize -= 1
        systemStreamLagCounts -= systemStream
      }
    }
  }

  /**
   * It's only OK to allow the wrapped MessageChooser to choose if it's been
   * given at least one envelope from each lagging SystemStream.
   */
  private def okToChoose = {
    updatedSystemStreams.values.filter(_ > 0).size == systemStreamLagSize
  }
}
