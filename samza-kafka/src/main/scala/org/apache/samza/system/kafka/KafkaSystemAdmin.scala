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
import org.apache.samza.system.SystemStreamMetadata
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

  def getSystemStreamMetadata(streams: java.util.Set[String]) = {
    var partitions = Map[String, Set[Partition]]()
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

        partitions = streams.map(streamName => {
          val partitions = metadata(streamName)
            .partitionsMetadata
            .map(pm => new Partition(pm.partitionId))
            .toSet[Partition]
          (streamName, partitions)
        }).toMap

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
          debug("Getting next offsets for %s" format topicsAndPartitions)
          nextOffsets ++= getOffsets(consumer, topicsAndPartitions, OffsetRequest.LatestTime)
          // Kafka's latest offset is always last message in stream's offset + 
          // 1, so get last message in stream by subtracting one. this is safe 
          // even for key-deduplicated streams, since the last message will 
          // never be deduplicated.
          latestOffsets = nextOffsets.mapValues(offset => (offset.toLong - 1).toString)
          // Keep only earliest/latest offsets where there is a message. Should 
          // return null offsets for empty streams.
          nextOffsets.foreach {
            case (topicAndPartition, offset) =>
              if (offset.toLong <= 0) {
                earliestOffsets -= topicAndPartition
                latestOffsets -= topicAndPartition
              }
          }
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

    val allMetadata = (earliestOffsets.keySet ++ latestOffsets.keySet ++ nextOffsets.keySet)
      .groupBy(_.getStream)
      .map {
        case (streamName, systemStreamPartitions) =>
          val partitionOffsets = systemStreamPartitions
            .map(systemStreamPartition => {
              val offsets = SystemStreamPartitionOffsets(
                earliestOffsets.getOrElse(systemStreamPartition, null),
                latestOffsets.getOrElse(systemStreamPartition, null),
                nextOffsets(systemStreamPartition))
              (systemStreamPartition.getPartition, offsets)
            })
            .toMap
          val streamEarliestOffsets = partitionOffsets.mapValues(_.earliestOffset)
          val streamLatestOffsets = partitionOffsets.mapValues(_.latestOffset)
          val streamNextOffsets = partitionOffsets.mapValues(_.nextOffset)
          val streamPartitions = partitions(streamName)
          val streamMetadata = new SystemStreamMetadata(streamName, streamPartitions, streamEarliestOffsets, streamLatestOffsets, streamNextOffsets)
          (streamName, streamMetadata)
      }
      .toMap

    info("Got metadata for streams: %s, %s" format (streams, allMetadata))

    allMetadata
  }

  private def getOffsets(consumer: SimpleConsumer, topicsAndPartitions: Set[TopicAndPartition], earliestOrLatest: Long) = {
    var offsets = Map[SystemStreamPartition, String]()
    val partitionOffsetInfo = topicsAndPartitions
      .map(topicAndPartition => (topicAndPartition, PartitionOffsetRequestInfo(earliestOrLatest, 1)))
      .toMap

    val brokerOffsets = consumer
      .getOffsetsBefore(new OffsetRequest(partitionOffsetInfo))
      .partitionErrorAndOffsets
      .mapValues(_.offsets.head)

    for ((topicAndPartition, offset) <- brokerOffsets) {
      offsets += new SystemStreamPartition(systemName, topicAndPartition.topic, new Partition(topicAndPartition.partition)) -> offset.toString
    }

    debug("Got offsets: %s" format offsets)

    offsets
  }
}

case class SystemStreamPartitionOffsets(earliestOffset: String, latestOffset: String, nextOffset: String)