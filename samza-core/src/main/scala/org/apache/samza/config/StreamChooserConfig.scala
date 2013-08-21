package org.apache.samza.config

object StreamChooserConfig {

  /**
   * The order in which streams will be prioritized, from high to low. For
   * example, setting task.chooser.stream.order=system.stream1,system.stream2
   * would cause the StreamChooser to always prioritize incoming messages from
   * stream1 over stream2.
   */
  val PRIORITIZED_STREAM_ORDER = "task.chooser.stream.order"

  implicit def Config2StreamChooser(config: Config) = new StreamChooserConfig(config)
}

class StreamChooserConfig(config: Config) extends ScalaMapConfig(config) {
  def getPrioritizedStreams = getOption(StreamChooserConfig.PRIORITIZED_STREAM_ORDER)
}
