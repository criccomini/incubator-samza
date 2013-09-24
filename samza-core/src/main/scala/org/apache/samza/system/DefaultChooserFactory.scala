package org.apache.samza.system

import org.apache.samza.config.Config
import org.apache.samza.config.DefaultChooserConfig._
import org.apache.samza.config.TaskConfig._
import org.apache.samza.config.SystemConfig._
import org.apache.samza.SamzaException
import org.apache.samza.util.Util
import scala.collection.JavaConversions._

// TODO add logging everywhere
// TODO javadocs for DefaultChooser
class DefaultChooserFactory extends MessageChooserFactory {
  def getChooser(config: Config): MessageChooser = {
    // TODO only fully compose in cases where there are bootstrap and prioritized streams.
    val batchSize = config
      .getChooserBatchSize
      .getOrElse("100")
      .toInt
    val prioritizedBootstrapStreams = config
      .getBootstrapStreams
      .map((_, Int.MaxValue))
      .toMap
    val defaultPrioritizedStreams = config
      .getInputStreams
      .map((_, 0))
      .toMap
    val latestMessageOffsets = buildLatestOffsets(prioritizedBootstrapStreams.keySet, config)
    val prioritizedStreams = defaultPrioritizedStreams ++ prioritizedBootstrapStreams ++ config.getPriorityStreams
    val prioritizedChoosers = prioritizedStreams
      .values
      .toSet
      .map((_: Int, new BatchingChooser(getTieBreaker(config), batchSize).asInstanceOf[MessageChooser]))
      .toMap
    val priority = new TieredPriorityChooser(prioritizedStreams, prioritizedChoosers)
    val behind = new StreamsBehindHeadChooser(latestMessageOffsets, priority)
    behind
  }

  /**
   * Returns a MessageChooser that's used in cases where there are multiple
   * messages that are the same priority, and the BatchingChooser is not
   * preferring a specific SystemStreamPartition.
   *
   * If you wish to over-ride the tie breaking strategy, simply extend the
   * DefaultChooserFactory, and override this method. For example, if you
   * wanted to break ties based on the time field in a message, you could
   * implement a TimeChooser class, and return it here.
   */
  protected def getTieBreaker(config: Config): MessageChooser = new RoundRobinChooser

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