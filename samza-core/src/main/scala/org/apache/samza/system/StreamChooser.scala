package org.apache.samza.system

import org.apache.samza.config.Config
import org.apache.samza.config.StreamChooserConfig._
import org.apache.samza.SamzaException
import org.apache.samza.util.Util

/**
 * A chooser that hard codes priorities based on a priority sorted list of
 * streams. If system.stream1 is listed ahead of system.stream2 in the
 * priority list, stream1's messages will always be chosen before stream2's,
 * if both are available. If an incoming message envelope comes in for a
 * stream that wasn't in the priority list, it's prioritized equally with all
 * other streams that weren't defined in the priority list.
 *
 * Note that this chooser can lead to starvation of a stream in cases where
 * there are always messages available for a higher priority stream.
 */
class StreamChooser(prioritizedStreamLists: Seq[SystemStream]) extends PriorityChooser {
  val prioritizedStreams = prioritizedStreamLists
    .zipWithIndex
    // Do size - idx, because we want 0th element to have highest priority (size - 0).
    .map { case (systemStream, idx) => (systemStream, prioritizedStreamLists.size - idx) }
    .toMap

  def prioritize(envelope: IncomingMessageEnvelope) = {
    prioritizedStreams
      .getOrElse(envelope.getSystemStreamPartition.getSystemStream, -1)
      .toDouble
  }
}

class StreamChooserFactory extends MessageChooserFactory {
  def getChooser(config: Config) = {
    val streams = config
      .getPrioritizedStreams
      .getOrElse(throw new SamzaException("Using StreamChooser, but missing required property: %s" format PRIORITIZED_STREAM_ORDER))
      .split(",")
      .map(Util.getSystemStreamFromNames(_))

    new StreamChooser(streams)
  }
}