package org.apache.samza.config

import org.apache.samza.system.SystemStream
import TaskConfig._

object DefaultChooserConfig {
  val BOOTSTRAP_PREFIX = "task.chooser.bootstrap.%s.%s"
  val PRIORITY_PREFIX = "task.chooser.prioriites.%s.%s"
  val BATCH_SIZE = "task.chooser.batch.size"
  val WRAPPED_CHOOSER_FACTORY = "task.chooser.wrapped.class"

  implicit def Config2DefaultChooser(config: Config) = new DefaultChooserConfig(config)
}

class DefaultChooserConfig(config: Config) extends ScalaMapConfig(config) {
  import DefaultChooserConfig._

  def getChooserBatchSize = getOption(BATCH_SIZE)

  def getWrappedChooserFactory = getOption(WRAPPED_CHOOSER_FACTORY)

  def getBootstrapStreams = config
    .getInputStreams
    .map(systemStream => (systemStream, getOrElse(BOOTSTRAP_PREFIX format (systemStream.getSystem, systemStream.getStream), "false").equals("true")))
    .filter(_._2.equals("true"))
    .map(_._1)

  def getPriorityStreams = config
    .getInputStreams
    .map(systemStream => (systemStream, getOrElse(PRIORITY_PREFIX format (systemStream.getSystem, systemStream.getStream), "-1").toInt))
    .filter(_._2 >= 0)
    .toMap
}