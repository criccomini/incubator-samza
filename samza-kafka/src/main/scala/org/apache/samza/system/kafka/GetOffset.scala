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

object GetOffset extends Toss {
  /**
   * Convert Kafka's "largest" and "smallest" auto.offset.reset String
   * constants to their corresponding Long values.
   */
  def convertAutoOffsetReset(autoOffsetReset: String): Long = {
    autoOffsetReset match {
      case OffsetRequest.LargestTimeString => OffsetRequest.LatestTime
      case OffsetRequest.SmallestTimeString => OffsetRequest.EarliestTime
      case other => toss("Can't get offset value for: %s" format other)
    }
  }

  /**
   * Convert Kafka's "largest" and "smallest" String constants to their
   * corresponding Long values.
   */
  def convertAutoOffsetReset(autoOffsetReset: Long): String = {
    autoOffsetReset match {
      case OffsetRequest.LatestTime => OffsetRequest.LargestTimeString
      case OffsetRequest.EarliestTime => OffsetRequest.SmallestTimeString
      case other => toss("Can't get offset value for: %s" format other)
    }
  }
}

class GetOffset(
  /**
   * Default auto.offset.reset setting to use when no default exists for a
   * given topic. This is used when an OffsetOutOfRangeException exception is
   * thrown by Kafka's consumer, and no topic-level auto.offset.reset setting
   * is defined for the topic in question.
   */
  default: String,

  /**
   * Topic-level auto.offset.reset settings for topics. These settings are
   * used when an OffsetOutOfRangeException exception is thrown by Kafka's
   * consumer.
   */
  autoOffsetResetTopics: Map[String, String] = Map()) extends Logging with Toss {

  import GetOffset._

  debug("Default offset set to: %s" format default)
  debug("Auto offset reset topics set to: %s" format autoOffsetResetTopics)

  /**
   * Use a topic's auto.offset.reset setting to try and get the next offset in
   * the topic. First uses the override value for the topic's
   * auto.offset.reset, then falls back to the topic-level auto.offset.reset
   * if the parameter is not provided. If no topic-level definition is
   * available, then the default is used.
   */
  def getNextOffsetUsingAutoOffsetReset(topicPartitionConsumer: TopicAndPartitionConsumer, autoOffsetResetOverride: Option[Long] = None) = {
    val topicAutoOffsetReset = convertAutoOffsetReset(autoOffsetResetTopics.getOrElse(topicPartitionConsumer.topicPartition.topic, default))

    debug("Got auto.offset.reset %s for topic/partition %s." format (convertAutoOffsetReset(topicAutoOffsetReset), topicPartitionConsumer.topicPartition))

    val autoOffsetReset = autoOffsetResetOverride match {
      case Some(autoOffsetResetOverride) =>
        debug("Overriding auto.offset.reset for topic %s with %s." format (topicPartitionConsumer.topicPartition, convertAutoOffsetReset(autoOffsetResetOverride)))

        autoOffsetResetOverride
      case _ => topicAutoOffsetReset
    }

    getNextOffset(topicPartitionConsumer, autoOffsetReset)
  }

  /**
   * Get the next offset after the last checkpointed offset. This is useful
   * when a consumer wishes to pick up where it left off from.
   */
  def getNextOffsetUsingLastCheckpointedOffset(topicPartitionConsumer: TopicAndPartitionConsumer, lastCheckpointedOffset: Option[String] = None) = {
    debug("Getting next offset for topic/partition %s using last checkpointed offset %s." format (topicPartitionConsumer.topicPartition, lastCheckpointedOffset))

    lastCheckpointedOffset match {
      case Some(lastCheckpointedOffset) => verifyOffset(topicPartitionConsumer, lastCheckpointedOffset).getOrElse(getNextOffsetUsingAutoOffsetReset(topicPartitionConsumer))
      case _ => getNextOffsetUsingAutoOffsetReset(topicPartitionConsumer, Some(OffsetRequest.EarliestTime))
    }
  }

  /**
   * Use auto.offset.reset setting to fetch the next offset for a
   * topic/partition.
   */
  private def getNextOffset(topicPartitionConsumer: TopicAndPartitionConsumer, autoOffsetReset: Long) = {
    debug("Getting next offset for %s using auto.offset.reset %s." format (topicPartitionConsumer.topicPartition, convertAutoOffsetReset(autoOffsetReset)))

    val offsetRequest = new OffsetRequest(Map(topicPartitionConsumer.topicPartition -> new PartitionOffsetRequestInfo(autoOffsetReset, 1)))
    val offsetResponse = topicPartitionConsumer
      .simpleConsumer
      .getOffsetsBefore(offsetRequest)
    val partitionOffsetResponse = offsetResponse
      .partitionErrorAndOffsets
      .get(topicPartitionConsumer.topicPartition)
      .getOrElse(toss("Unable to find offset information for %s" format topicPartitionConsumer.topicPartition))

    ErrorMapping.maybeThrowException(partitionOffsetResponse.error)

    val nextOffset = partitionOffsetResponse.offsets.headOption.getOrElse(toss("Got response, but no offsets defined for %s" format topicPartitionConsumer.topicPartition))

    debug("Got next offset %s for %s using auto.offset.reset %s." format (nextOffset, topicPartitionConsumer.topicPartition, convertAutoOffsetReset(autoOffsetReset)))

    nextOffset
  }

  /**
   * Verify that the offset of the last checkpointed message still exists for
   * topic/partition, then get the offset for the message after the last
   * checkpointed message, and return it.
   */
  private def verifyOffset(topicPartitionConsumer: TopicAndPartitionConsumer, lastReadOffset: String) = {
    debug("Validating offset %s for topic and partition %s." format (lastReadOffset, topicPartitionConsumer.topicPartition))

    try {
      val messages = topicPartitionConsumer
        .simpleConsumer
        .defaultFetch((topicPartitionConsumer.topicPartition, lastReadOffset.toLong))
      val topic = topicPartitionConsumer
        .topicPartition
        .topic
      val partition = topicPartitionConsumer
        .topicPartition
        .partition

      ErrorMapping.maybeThrowException(messages.errorCode(topic, partition))

      debug("Successfully validated offset %s for topic/partition %s." format (lastReadOffset, topicPartitionConsumer.topicPartition))

      val messageSet = messages.messageSet(topicPartitionConsumer.topicPartition.topic, topicPartitionConsumer.topicPartition.partition)

      if (messageSet.isEmpty) {
        debug("Got empty response when trying to read message with last checkpointed offset %s for topic/partition %s. This is unexpected, since we used to be able to read this message." format (lastReadOffset, topicPartitionConsumer.topicPartition))

        None
      } else {
        val nextOffset = messageSet
          .head
          .nextOffset

        debug("Got next offset %s for %s." format (nextOffset, topicPartitionConsumer.simpleConsumer))

        Some(nextOffset)
      }
    } catch {
      case e: OffsetOutOfRangeException =>
        debug("Received an offset out of range exception while trying to validate offset %s for topic/partition %s." format (lastReadOffset, topicPartitionConsumer.topicPartition))
        None
    }
  }
}

/**
 * A helper class to couple a TopicAndPartition with its SimpleConsumer.
 */
case class TopicAndPartitionConsumer(simpleConsumer: DefaultFetchSimpleConsumer, topicPartition: TopicAndPartition)
