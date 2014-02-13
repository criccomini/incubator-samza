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
  def convertAutoOffsetReset(autoOffsetReset: String): Long = {
    autoOffsetReset match {
      case OffsetRequest.LargestTimeString => OffsetRequest.LatestTime
      case OffsetRequest.SmallestTimeString => OffsetRequest.EarliestTime
      case other => toss("Can't get offset value for: %s" format other)
    }
  }
}

class GetOffset(
  default: String,
  autoOffsetResetTopics: Map[String, String] = Map()) extends Logging with Toss {

  def getNextOffsetUsingAutoOffsetReset(topicPartitionConsumer: TopicAndPartitionConsumer, autoOffsetResetOverride: Option[Long] = None) = {
    val topicAutoOffsetReset = GetOffset.convertAutoOffsetReset(autoOffsetResetTopics.getOrElse(topicPartitionConsumer.topicPartition.topic, default))
    val autoOffsetReset = autoOffsetResetOverride match {
      case Some(autoOffsetResetOverride) => autoOffsetResetOverride
      case _ => topicAutoOffsetReset
    }

    getNextOffset(topicPartitionConsumer, autoOffsetReset)
  }

  def getNextOffsetUsingLastCheckpointedOffset(topicPartitionConsumer: TopicAndPartitionConsumer, lastCheckpointedOffset: Option[String] = None) = {
    lastCheckpointedOffset match {
      case Some(lastCheckpointedOffset) =>
        if (verifyOffset(topicPartitionConsumer, lastCheckpointedOffset)) {
          lastCheckpointedOffset.toLong
        } else {
          getNextOffsetUsingAutoOffsetReset(topicPartitionConsumer)
        }
      case _ => getNextOffsetUsingAutoOffsetReset(topicPartitionConsumer, Some(OffsetRequest.EarliestTime))
    }
  }

  private def getNextOffset(topicPartitionConsumer: TopicAndPartitionConsumer, autoOffsetReset: Long) = {
    val offsetRequest = new OffsetRequest(Map(topicPartitionConsumer.topicPartition -> new PartitionOffsetRequestInfo(autoOffsetReset, 1)))
    val offsetResponse = topicPartitionConsumer
      .simpleConsumer
      .getOffsetsBefore(offsetRequest)
    val partitionOffsetResponse = offsetResponse
      .partitionErrorAndOffsets
      .get(topicPartitionConsumer.topicPartition)
      .getOrElse(toss("Unable to find offset information for %s" format topicPartitionConsumer.topicPartition))

    ErrorMapping.maybeThrowException(partitionOffsetResponse.error)

    partitionOffsetResponse.offsets.headOption.getOrElse(toss("Got response, but no offsets defined for %s" format topicPartitionConsumer.topicPartition))
  }

  private def verifyOffset(topicPartitionConsumer: TopicAndPartitionConsumer, offset: String) = {
    try {
      val messages = topicPartitionConsumer.simpleConsumer.defaultFetch((topicPartitionConsumer.topicPartition, offset.toLong))
      val topic = topicPartitionConsumer
        .topicPartition
        .topic
      val partition = topicPartitionConsumer
        .topicPartition
        .partition

      ErrorMapping.maybeThrowException(messages.errorCode(topic, partition))

      true
    } catch {
      case e: OffsetOutOfRangeException =>
        false
    }
  }
}

case class TopicAndPartitionConsumer(simpleConsumer: DefaultFetchSimpleConsumer, topicPartition: TopicAndPartition)