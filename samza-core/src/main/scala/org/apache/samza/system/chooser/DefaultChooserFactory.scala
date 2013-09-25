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

import org.apache.samza.config.Config
import org.apache.samza.config.DefaultChooserConfig._
import org.apache.samza.config.TaskConfig._
import org.apache.samza.config.SystemConfig._
import org.apache.samza.SamzaException
import org.apache.samza.util.Util
import scala.collection.JavaConversions._
import org.apache.samza.system.SystemFactory
import org.apache.samza.system.SystemStream

/**
 * DefaultChooserFactory builds the default MessageChooser for Samza, when one
 * is not defined using "task.chooser.class".
 *
 * The chooser that this factory builds supports the following behaviors:
 *
 * 1. Batching.
 * 2. Prioritized streams.
 * 3. Bootstrapping.
 *
 * By default, this chooser will not do any of this. It will simply default to
 * a RoundRobinChooser.
 *
 * To activate batching, you define must define:
 *
 *   task.chooser.batch.size
 *
 * To define a priority for a stream, you must define:
 *
 *   task.chooser.prioriites.<system>.<stream>
 *
 * To declare a bootstrap stream, you must define:
 *
 *   task.chooser.bootstrap.<system>.<stream>
 *
 * When batching is activated, the DefaultChooserFactory will allow the
 * initial strategy to be executed once (by default, this is RoundRobin). It
 * will then keep picking the SystemStreamPartition that the RoundRobin
 * chooser selected, up to the batch size, provided that there are messages
 * available for this SystemStreamPartition. If the batch size is reached, or
 * there are no messages available, the RoundRobinChooser will be executed
 * again, and the batching process will repeat itself.
 *
 * When a stream is defined with a priority, it is preferred over all lower
 * priority streams in cases where there are messages available from both
 * streams. If two envelopes exist for two SystemStreamPartitions that have
 * the same priority, the default strategy is used to determine which envelope
 * to use (RoundRobinChooser, by default). If a stream doesn't have a
 * configured priority, its priority is 0. Higher priority streams are
 * preferred over lower priority streams.
 *
 * When a stream is defined as a bootstrap stream, it is prioritized with a
 * default priority of Int.MaxValue. This priority can be overridden using the
 * same priority configuration defined above (task.chooser.priorites.*). The
 * DefaultChooserFactory guarantees that the wrapped MessageChooser will have
 * at least one envelope from each bootstrap stream whenever the wrapped
 * MessageChooser must make a decision about which envelope to process next.
 * If a stream is defined as a bootstrap stream, and is prioritized higher
 * than all other streams, it means that all messages in the stream will be
 * processed (up to head) before any other messages are processed. Once all of
 * a bootstrap stream's partitions catch up to head, the stream is marked as
 * fully bootstrapped, and it is then treated like a normal prioritized stream.
 *
 * Valid configurations include:
 *
 *   task.chooser.batch.size=100
 *
 * This configuration will just batch up to 100 messages from each
 * SystemStreamPartition. It will use a RoundRobinChooser whenever it needs to
 * find the next SystemStreamPartition to batch.
 *
 *   task.chooser.prioriites.kafka.mystream=1
 *
 * This configuration will prefer messages from kafka.mystream over any other
 * input streams (since other input streams will default to priority 0).
 *
 *   task.chooser.bootstrap.kafka.profiles=true
 *
 * This configuration will process all messages from kafka.profiles up to the
 * current head of the profiles stream before any other messages are processed.
 * From then on, the profiles stream will be preferred over any other stream in
 * cases where incoming envelopes are ready to be processed from it.
 *
 *   task.chooser.batch.size=100
 *   task.chooser.bootstrap.kafka.profiles=true
 *   task.chooser.prioriites.kafka.mystream=1
 *
 * This configuration will read all messages from kafka.profiles from the last
 * checkpointed offset, up to head. It will then prefer messages from profiles
 * over mystream, and prefer messages from mystream over any other stream. In
 * cases where there is more than one envelope available with the same priority
 * (e.g. two envelopes from different partitions in the profiles stream),
 * RoundRobinChooser will be used to break the tie. Once the tie is broken, up
 * to 100 messages will be read from the envelope's SystemStreamPartition,
 * before RoundRobinChooser is consulted again to break the next tie.
 *
 *   task.chooser.bootstrap.kafka.profiles=true
 *   systems.kafka.streams.profiles.samza.reset.offset=true
 *
 * This configuration will bootstrap the profiles stream the same way as the
 * last example, except that it will always start from offset zero, which means
 * that it will always read all messages in the topic from oldest to newest.
 */
class DefaultChooserFactory extends MessageChooserFactory {
  def getChooser(config: Config): MessageChooser = {
    val batchSize = config.getChooserBatchSize

    // Normal streams default to priority 0.
    val defaultPrioritizedStreams = config
      .getInputStreams
      .map((_, 0))
      .toMap

    // Bootstrap streams defaut to Int.MaxValue priority.
    val prioritizedBootstrapStreams = config
      .getBootstrapStreams
      .map((_, Int.MaxValue))
      .toMap

    // Explicitly prioritized streams are set to whatever they were configured to.
    val prioritizedStreams = config.getPriorityStreams

    // Only wire in what we need.
    val useBatching = batchSize.isDefined
    val useBootstrapping = prioritizedBootstrapStreams.size > 0
    val usePriority = useBootstrapping || prioritizedStreams.size > 0

    // A helper method to build either a batched or non-batched chooser, depending on config.
    val maybeBatchingChooser = () => {
      if (useBatching) {
        new BatchingChooser(getWrappedChooser(config), batchSize.get.toInt)
      } else {
        getWrappedChooser(config)
      }
    }

    val chooser = if (usePriority) {
      val allPrioritizedStreams = defaultPrioritizedStreams ++ prioritizedBootstrapStreams ++ prioritizedStreams
      val prioritizedChoosers = allPrioritizedStreams
        .values
        .toSet
        .map((_: Int, maybeBatchingChooser().asInstanceOf[MessageChooser]))
        .toMap
      new TieredPriorityChooser(allPrioritizedStreams, prioritizedChoosers)
    } else {
      maybeBatchingChooser()
    }

    if (useBootstrapping) {
      val latestMessageOffsets = buildLatestOffsets(prioritizedBootstrapStreams.keySet, config)

      new BootstrappingChooser(latestMessageOffsets, chooser)
    } else {
      chooser
    }
  }

  /**
   * Returns a base MessageChooser that the DefaultChooserFactory should use
   * when it's composing its MessageChooser classes.
   *
   * If there are prioritized streams, then this class is used in cases where
   * there are multiple messages that are the same priority, and the
   * BatchingChooser is not preferring a specific SystemStreamPartition.
   *
   * If you wish to over-ride the tie breaking strategy, simply extend the
   * DefaultChooserFactory, and override this method. For example, if you
   * wanted to break ties based on the time field in a message, you could
   * implement a TimeChooser class, and return it here.
   *
   * In cases where streams are not prioritized, and there's no bootstrapping,
   * this is the chooser that's used to make envelope picking decisions.
   */
  protected def getWrappedChooser(config: Config): MessageChooser = new RoundRobinChooser

  /**
   * Given a set of SystemStreams, returns a map of SystemStreamPartition to
   * last offset in each partition.
   */
  private def buildLatestOffsets(bootstrapStreams: Set[SystemStream], config: Config) = {
    val streamsPerSystem = bootstrapStreams.groupBy(_.getSystem())

    streamsPerSystem.flatMap {
      case (systemName, streams) =>
        val systemFactoryClassName = config
          .getSystemFactory(systemName)
          .getOrElse(throw new SamzaException("Trying to fetch system factory for system %s, which isn't defined in config." format systemName))
        val systemFactory = Util.getObj[SystemFactory](systemFactoryClassName)
        val systemAdmin = systemFactory.getAdmin(systemName, config)

        systemAdmin.getLastOffsets(streams.map(_.getStream))
    }
  }
}