/*
 *
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
 *
 */

package org.apache.samza.system.kafka

import kafka.common.{ OffsetOutOfRangeException, ErrorMapping }
import kafka.api._
import kafka.common.TopicAndPartition
import kafka.api.PartitionOffsetRequestInfo
import grizzled.slf4j.Logging
import kafka.message.MessageAndOffset
import org.apache.samza.SamzaException

/**
 * TODO
 */
class GetOffset(default: String, autoOffsetResetTopics: Map[String, String] = Map()) extends Logging with Toss {

  /**
   * TODO
   */
  def isValidOffset(consumer: DefaultFetchSimpleConsumer, topicAndPartition: TopicAndPartition, offset: String) = {
    info("Validating offset %s for topic and partition %s" format (offset, topicAndPartition))

    try {
      val messages = consumer.defaultFetch((topicAndPartition, offset.toLong))

      if (messages.hasError) {
        ErrorMapping.maybeThrowException(messages.errorCode(topicAndPartition.topic, topicAndPartition.partition))
      }

      info("Able to successfully read from offset %s for topic and partition %s. Using it to instantiate consumer." format (offset, topicAndPartition))

      val messageSet = messages.messageSet(topicAndPartition.topic, topicAndPartition.partition)

      if (messageSet.isEmpty) {
        throw new SamzaException("Got empty message set for a valid offset. This is unexpected.")
      }

      true
    } catch {
      case e: OffsetOutOfRangeException => false
    }
  }

  /**
   * TODO
   */
  def getResetOffset(consumer: DefaultFetchSimpleConsumer, topicAndPartition: TopicAndPartition) = {
    val offsetRequest = new OffsetRequest(Map(topicAndPartition -> new PartitionOffsetRequestInfo(getAutoOffset(topicAndPartition.topic), 1)))
    val offsetResponse = consumer.getOffsetsBefore(offsetRequest)
    val partitionOffsetResponse = offsetResponse
      .partitionErrorAndOffsets
      .get(topicAndPartition)
      .getOrElse(toss("Unable to find offset information for %s" format topicAndPartition))

    ErrorMapping.maybeThrowException(partitionOffsetResponse.error)

    partitionOffsetResponse
      .offsets
      .headOption
      .getOrElse(toss("Got response, but no offsets defined for %s" format topicAndPartition))
  }

  /**
   * TODO
   */
  private def getAutoOffset(topic: String): Long = {
    info("Checking if auto.offset.reset is defined for topic %s" format (topic))
    autoOffsetResetTopics.getOrElse(topic, default) match {
      case OffsetRequest.LargestTimeString =>
        info("Got reset of type %s." format OffsetRequest.LargestTimeString)
        OffsetRequest.LatestTime
      case OffsetRequest.SmallestTimeString =>
        info("Got reset of type %s." format OffsetRequest.SmallestTimeString)
        OffsetRequest.EarliestTime
      case other => toss("Can't get offset value for topic %s due to invalid value: %s" format (topic, other))
    }
  }
}
