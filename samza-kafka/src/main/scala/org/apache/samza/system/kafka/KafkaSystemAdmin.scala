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
import java.util.UUID
import org.apache.samza.util.ClientUtilTopicMetadataStore
import kafka.api.TopicMetadata
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemAdmin
import org.apache.samza.SamzaException
import kafka.consumer.SimpleConsumer
import kafka.utils.Utils
import kafka.client.ClientUtils
import java.util.Random
import kafka.api.TopicMetadataRequest
import kafka.common.TopicAndPartition
import kafka.api.PartitionOffsetRequestInfo
import kafka.api.OffsetRequest
import kafka.api.FetchRequestBuilder
import org.apache.samza.system.SystemStreamPartition
import kafka.common.ErrorMapping
import grizzled.slf4j.Logging

class KafkaSystemAdmin(
  systemName: String,
  // TODO whenever Kafka decides to make the Set[Broker] class public, let's switch to Set[Broker] here.
  brokerListString: String,
  timeout: Int = Int.MaxValue,
  bufferSize: Int = 1024000,
  clientId: String = UUID.randomUUID.toString) extends SystemAdmin with Logging {

  val rand = new Random

  def getPartitions(streamName: String): java.util.Set[Partition] = {
    val getTopicMetadata = (topics: Set[String]) => {
      new ClientUtilTopicMetadataStore(brokerListString, clientId)
        .getTopicInfo(topics)
    }

    val metadata = TopicMetadataCache.getTopicMetadata(
      Set(streamName),
      systemName,
      getTopicMetadata)

    metadata(streamName)
      .partitionsMetadata
      .map(pm => new Partition(pm.partitionId))
      .toSet[Partition]
  }

  def getLastOffsets(streams: java.util.Set[String]) = {
    var offsets = Map[SystemStreamPartition, String]()
    var done = false

    while (!done) {
      try {
        // Get brokers.
        val brokers = ClientUtils
          .parseBrokerList(brokerListString)
          .toArray

        // Grab a broker at random, and send a topic metadata request for all topics.
        val broker = brokers(rand.nextInt(brokers.size))
        var correlationId = 0
        val topicMetadataRequest = TopicMetadataRequest(
          TopicMetadataRequest.CurrentVersion,
          correlationId,
          clientId,
          streams.toSeq)
        val consumer = new SimpleConsumer(broker.host, broker.port, timeout, bufferSize, clientId)
        val topicMetadataResponse = consumer.send(topicMetadataRequest)

        consumer.close

        // Break topic metadata topic/partitions into per-broker map.
        val brokersToTopicPartitions = topicMetadataResponse
          .topicsMetadata
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
          .mapValues(_.map(_._2))

        // Get the latest offsets for each topic and partition.
        for ((broker, topicsAndPartitions) <- brokersToTopicPartitions) {
          val partitionOffsetInfo = topicsAndPartitions
            .map(topicAndPartition => (topicAndPartition, PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
            .toMap
          val consumer = new SimpleConsumer(broker.host, broker.port, timeout, bufferSize, clientId)
          val brokerOffsets = consumer
            .getOffsetsBefore(new OffsetRequest(partitionOffsetInfo))
            .partitionErrorAndOffsets
            .filter(_._2.offsets.head > 0)
            // Kafka returns 1 greater than the offset of the last message in 
            // the topic, so subtract one to fetch the last message.
            .mapValues(_.offsets.head - 1)

          consumer.close

          for ((topicAndPartition, offset) <- brokerOffsets) {
            offsets += new SystemStreamPartition(systemName, topicAndPartition.topic, new Partition(topicAndPartition.partition)) -> offset.toString
          }
        }

        done = true
      } catch {
        case e: Exception =>
          // Retry.
          warn("Unable to fetch last offsets for streams due to: %s, %s. Retrying. Turn on debugging to get a full stack trace." format (e.getMessage, streams))
          debug(e)
      }
    }

    offsets
  }
}
