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
  lastMessageOffsets: Map[SystemStreamPartition, String],
  wrapped: MessageChooser) extends MessageChooser {

  var laggingSystemStreamPartitions = lastMessageOffsets.keySet
  var updatedSystemStreamPartitions = Set[SystemStreamPartition]()

  // TODO need a way to handle the case where we are caught up to an SSP at 
  // startup -- no messages will ever be updated, so we need another way to 
  // remove them.

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
    if ((laggingSystemStreamPartitions -- updatedSystemStreamPartitions).size == 0) {
      val envelope = wrapped.choose

      if (envelope != null) {
        val systemStreamPartition = envelope.getSystemStreamPartition
        val offset = envelope.getOffset

        updatedSystemStreamPartitions -= systemStreamPartition

        // The stream is no longer lagging if the offset is null (unsupported), 
        // or if the envelope's offset equals the lastOffset map. 
        if (offset == null || offset.equals(lastMessageOffsets.getOrElse(systemStreamPartition, null))) {
          laggingSystemStreamPartitions -= systemStreamPartition
        }
      }

      envelope
    } else {
      null
    }
  }
}
