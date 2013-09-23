package org.apache.samza.system

/**
 * BootstrappingChooser is a mix-in that allows a MessageChooser to process
 * only SystemStreamPartitions that are behind head. It does this by
 * withholding any messages from SystemStreamPartitions that are already at
 * head until all other "behind" SystemStreamPartitions have caught up.
 *
 * This trait is meant to be mixed in with a MessageChooser.
 */
class BootstrappingChooser(
  /**
   * A map from SSP to latest offset for each bootstrap stream. If a stream
   * does not need to be bootstrapped, it should not be included in this map.
   */
  latestMessageOffsets: Map[SystemStreamPartition, String],

  /**
   * The message chooser that BootstrappingChooser delegates to when it's
   * updating or choosing envelopes.
   */
  wrapped: MessageChooser) extends BaseMessageChooser {

  var laggingSystemStreamPartitions = latestMessageOffsets.keySet
  var updatedSystemStreamPartitions = Set[SystemStreamPartition]()

  override def register(systemStreamPartition: SystemStreamPartition, lastReadOffset: String) {
    val latestOffset = latestMessageOffsets.getOrElse(systemStreamPartition, null)

    // If the last offset read is the same as the latest offset in the SSP, 
    // then we're already at head for this SSP, so remove it from the lag list.
    if (lastReadOffset != null && lastReadOffset.equals(latestOffset)) {
      laggingSystemStreamPartitions -= systemStreamPartition
    }
  }

  def update(envelope: IncomingMessageEnvelope) {
    wrapped.update(envelope)

    updatedSystemStreamPartitions += envelope.getSystemStreamPartition
  }

  /**
   * If choose is called, and the parent MessageChoser has received a
   * message from all lagging SystemStreamPartitions, then the bootstrapper
   * forwards the choose call to the parent. Otherwise, the
   * BootstrappingChooser simply returns null, and waits for more updates.
   */
  def choose = {
    if (laggingSystemStreamPartitions.size == 0 || (laggingSystemStreamPartitions -- updatedSystemStreamPartitions).size == 0) {
      val envelope = wrapped.choose

      if (envelope != null) {
        val systemStreamPartition = envelope.getSystemStreamPartition
        val offset = envelope.getOffset

        updatedSystemStreamPartitions -= systemStreamPartition

        // The stream is no longer lagging if the offset is null (unsupported), 
        // or if the envelope's offset equals the lastOffset map. 
        if (offset == null || offset.equals(latestMessageOffsets.getOrElse(systemStreamPartition, null))) {
          laggingSystemStreamPartitions -= systemStreamPartition
        }
      }

      envelope
    } else {
      null
    }
  }
}
