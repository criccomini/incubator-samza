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

package org.apache.samza.system.chooser;

import java.util.PriorityQueue;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;

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

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String lastReadOffset) {
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
