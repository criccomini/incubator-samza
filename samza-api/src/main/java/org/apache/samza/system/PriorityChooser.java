package org.apache.samza.system;

import java.util.PriorityQueue;

/**
 * An abstract class that makes it easy to write message choosers based on a
 * priority. This class will always choose the message with the highest priority
 * (larger double).
 */
public abstract class PriorityChooser implements MessageChooser {
  private final PriorityQueue<PrioritizedEnvelope> queue;

  public PriorityChooser() {
    this.queue = new PriorityQueue<PrioritizedEnvelope>();
  }

  /**
   * Prioritizes the envelope, and adds it to the priority queue.
   * 
   * @param envelope
   *          Envelope to add to the priority queue.
   */
  @Override
  public void update(IncomingMessageEnvelope envelope) {
    queue.add(new PrioritizedEnvelope(envelope, prioritize(envelope)));
  }

  /**
   * @return Returns null if priority queue is empty, else returns the highest
   *         priority envelope.
   */
  @Override
  public IncomingMessageEnvelope choose() {
    PrioritizedEnvelope prioritizedEnvelope = queue.poll();

    if (prioritizedEnvelope != null) {
      return prioritizedEnvelope.getEnvelope();
    }

    return null;
  }

  /**
   * Returns a priority score for the given envelope.
   * 
   * @param envelope
   *          Envelope that needs to be prioritized.
   * @return A priority score for the envelope.
   */
  protected abstract double prioritize(IncomingMessageEnvelope envelope);

  private class PrioritizedEnvelope implements Comparable<PrioritizedEnvelope> {
    private final IncomingMessageEnvelope envelope;
    private final double priority;

    public PrioritizedEnvelope(IncomingMessageEnvelope envelope, double priority) {
      this.envelope = envelope;
      this.priority = priority;
    }

    public double getPriority() {
      return priority;
    }

    public IncomingMessageEnvelope getEnvelope() {
      return envelope;
    }

    @Override
    public int compareTo(PrioritizedEnvelope prioritizedEnvelope) {
      // Reverse compare because PriorityQueue puts the LEAST element at the
      // head of the queue, using natural ordering.
      return Double.compare(prioritizedEnvelope.getPriority(), priority);
    }
  }
}
