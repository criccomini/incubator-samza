package org.apache.samza.system

import scala.collection.immutable.TreeMap
import org.apache.samza.SamzaException

/**
 * TieredPriorityChooser groups messages into priority tiers. Each priority
 * tier has its own MessageChooser. When an envelope arrives, its priority is
 * determined based on its SystemStream partition, and the MessageChooser for
 * the envelope's priority tier is updated.
 *
 * When choose is called, the TieredPriorityChooser calls choose on each
 * MessageChooser from the highest priority tier down to the lowest.
 * TieredPriorityChooser stops calling choose the first time that it gets a
 * non-null envelope from a MessageChooser.
 *
 * A higher number means a higher priority.
 *
 * For example, suppose that there are two SystemStreams, X and Y. X has a
 * priority of 1, and Y has a priority of 0. In this case, there are two tiers:
 * 1, and 0. Each tier has its own MessageChooser. When an envelope is received
 * via TieredPriorityChooser.update(), TieredPriorityChooser will determine the
 * priority of the stream (1 if X, 0 if Y), and update that tier's
 * MessageChooser. When MessageChooser.choose is called, TieredPriorityChooser
 * will first call choose on tier 1's MessageChooser. If this MessageChooser
 * returns null, then TieredPriorityChooser will call choose on tier 0's
 * MessageChooser. If neither return an envelope, then null is returned.
 *
 * This class is useful in cases where you wish to prioritize streams, and
 * always pick one over another. In such a case, you need a tie-breaker if
 * multiple envelopes exist that are of the same priority. This class uses
 * a tier's MessageChooser as the tie breaker when more than one envelope
 * exists with the same priority.
 */
class TieredPriorityChooser(priorities: Map[SystemStream, Int], choosers: Map[Int, MessageChooser]) extends BaseMessageChooser {

  /**
   * A sorted list of MessageChoosers. Sorting is according to their priority, from high to low.
   */
  val prioritizedChoosers = choosers
    .keys
    .toList
    .sort(_ > _)
    .map(choosers(_))

  /**
   * A map from a SystemStream to the MessageChooser that should be used for it.
   */
  val prioritizedStreams = priorities
    .map(systemStreamPriority => (systemStreamPriority._1, choosers.getOrElse(systemStreamPriority._2, throw new SamzaException("Unable to setup priority chooser. No chooser found for priority: %s" format systemStreamPriority._2))))
    .toMap

  def update(envelope: IncomingMessageEnvelope) {
    val systemStream = envelope.getSystemStreamPartition.getSystemStream
    val chooser = prioritizedStreams.getOrElse(systemStream, throw new SamzaException("Can't update message since no priority defined for system stream: %s" format systemStream))
    chooser.update(envelope)
  }

  /**
   * Choose a message from the highest priority MessageChooser. Keep going to
   * lower priority MessageChoosers until a non-null envelope is returned, or
   * the end of the list is reached. Return null if no envelopes are returned
   * from any MessageChoosers.
   */
  def choose = {
    // TODO is there an idiomatic way to do this in Scala?
    var envelope: IncomingMessageEnvelope = null
    val iter = prioritizedChoosers.iterator

    while (iter.hasNext && envelope == null) {
      envelope = iter.next.choose
    }

    envelope
  }
}
