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

package org.apache.samza.system.kafka

import org.apache.samza.Partition
import org.apache.samza.SamzaException
import org.apache.samza.system.SystemAdmin
import org.apache.samza.system.SystemStreamPartitionMetadata
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.ClientUtilTopicMetadataStore
import kafka.api._
import kafka.consumer.SimpleConsumer
import kafka.utils.Utils
import kafka.client.ClientUtils
import kafka.common.TopicAndPartition
import kafka.common.ErrorMapping
import kafka.cluster.Broker
import grizzled.slf4j.Logging
import java.util.UUID
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemStreamPartitionMetadata

class KafkaSystemAdmin(
  systemName: String,
  // TODO whenever Kafka decides to make the Set[Broker] class public, let's switch to Set[Broker] here.
  brokerListString: String,
  timeout: Int = Int.MaxValue,
  bufferSize: Int = 1024000,
  clientId: String = UUID.randomUUID.toString) extends SystemAdmin with Logging {

  private def getTopicMetadata(topics: Set[String]) = {
    new ClientUtilTopicMetadataStore(brokerListString, clientId)
      .getTopicInfo(topics)
  }

  def getSystemStreamPartitionMetadata(streams: java.util.Set[String]) = {
    var earliestOffsets = Map[SystemStreamPartition, String]()
    var latestOffsets = Map[SystemStreamPartition, String]()
    // TODO populate this
    var nextOffsets = Map[SystemStreamPartition, String]()
    var done = false
    var consumer: SimpleConsumer = null

    debug("Fetching offsets for: %s" format streams)

    while (!done) {
      try {
        val metadata = TopicMetadataCache.getTopicMetadata(
          streams.toSet,
          systemName,
          getTopicMetadata)

        debug("Got metadata for streams: %s" format metadata)

        // Break topic metadata topic/partitions into per-broker map.
        val brokersToTopicPartitions = metadata
          .values
          // Convert the topic metadata to a Seq[(Broker, TopicAndPartition)] 
          .flatMap(topicMetadata => topicMetadata
            .partitionsMetadata
            // Convert Seq[PartitionMetadata] to Seq[(Broker, TopicAndPartition)]
            .map(partitionMetadata => {
              ErrorMapping.maybeThrowException(partitionMetadata.errorCode)
              val topicAndPartition = new TopicAndPartition(topicMetadata.topic, partitionMetadata.partitionId)
              val leader = partitionMetadata
                .leader
                .getOrElse(throw new SamzaException("Need leaders for all partitions when fetching offsets. No leader available for TopicAndPartition: %s" format topicAndPartition))
              (leader, topicAndPartition)
            }))
          // Convert to a Map[Broker, Seq[(Broker, TopicAndPartition)]]
          .groupBy(_._1)
          // Convert to a Map[Broker, Seq[TopicAndPartition]]
          .mapValues(_.map(_._2).toSet)

        debug("Got topic partition data for brokers: %s" format brokersToTopicPartitions)

        // Get the latest offsets for each topic and partition.
        for ((broker, topicsAndPartitions) <- brokersToTopicPartitions) {
          consumer = new SimpleConsumer(broker.host, broker.port, timeout, bufferSize, clientId)
          debug("Getting earliest offsets for %s" format topicsAndPartitions)
          earliestOffsets ++= getOffsets(consumer, topicsAndPartitions, OffsetRequest.EarliestTime)
          debug("Getting latest offsets for %s" format topicsAndPartitions)
          latestOffsets ++= getOffsets(consumer, topicsAndPartitions, OffsetRequest.LatestTime)
          debug("Shutting down consumer for %s:%s." format (broker.host, broker.port))
          consumer.close
        }

        done = true
      } catch {
        case e: InterruptedException =>
          info("Interrupted while fetching last offsets, so forwarding.")
          if (consumer != null) {
            consumer.close
          }
          throw e
        case e: Exception =>
          // Retry.
          warn("Unable to fetch last offsets for streams due to: %s, %s. Retrying. Turn on debugging to get a full stack trace." format (e.getMessage, streams))
          debug(e)
      }
    }

    val streamMetadata = (earliestOffsets.keySet ++ latestOffsets.keySet ++ nextOffsets.keySet)
      .map(systemStreamPartition => {
        val earliestOffset = earliestOffsets(systemStreamPartition)
        val latestOffset = latestOffsets(systemStreamPartition)
        val nextOffset = nextOffsets(systemStreamPartition)
        val systemStreamPartitionMetadata = new SystemStreamPartitionMetadata(earliestOffset, latestOffset, nextOffset)
        (systemStreamPartition, systemStreamPartitionMetadata)
      })
      .toMap

    info("Got latest offsets for streams: %s, %s" format (streams, streamMetadata))

    streamMetadata
  }

  private def getOffsets(consumer: SimpleConsumer, topicsAndPartitions: Set[TopicAndPartition], earliestOrLatest: Long) = {
    var offsets = Map[SystemStreamPartition, String]()
    val partitionOffsetInfo = topicsAndPartitions
      .map(topicAndPartition => (topicAndPartition, PartitionOffsetRequestInfo(earliestOrLatest, 1)))
      .toMap
    val brokerOffsets = consumer
      .getOffsetsBefore(new OffsetRequest(partitionOffsetInfo))
      .partitionErrorAndOffsets
      // TODO should we check errors here?
      .mapValues(_.offsets.head)

    for ((topicAndPartition, offset) <- brokerOffsets) {
      offsets += new SystemStreamPartition(systemName, topicAndPartition.topic, new Partition(topicAndPartition.partition)) -> offset.toString
    }

    debug("Got offsets: %s" format offsets)

    offsets
  }
}
