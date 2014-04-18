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

package org.apache.samza.util;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;

/**
 * <p>
 * BlockingEnvelopeMap is a helper class for SystemConsumer implementations.
 * Samza's poll() requirements make implementing SystemConsumers somewhat
 * tricky. BlockingEnvelopeMap is provided to help other developers write
 * SystemConsumers.
 * </p>
 * 
 * <p>
 * SystemConsumers that implement BlockingEnvelopeMap need to add messages using
 * add (or addAll), and update noMoreMessage using setIsAtHead. The
 * noMoreMessage variable is used to determine whether a SystemStreamPartition
 * is "caught up" (has read all possible messages from the underlying system).
 * For example, with a Kafka system, noMoreMessages would be set to true when
 * the last message offset returned is equal to the offset high watermark for a
 * given topic/partition.
 * </p>
 */
public abstract class BlockingEnvelopeMap implements SystemConsumer {
  private final BlockingEnvelopeMapMetrics metrics;
  private final ConcurrentHashMap<SystemStreamPartition, BlockingQueue<IncomingMessageEnvelope>> bufferedMessages;
  private final Map<SystemStreamPartition, Boolean> noMoreMessage;
  private final Clock clock;

  public BlockingEnvelopeMap() {
    this(new NoOpMetricsRegistry());
  }

  public BlockingEnvelopeMap(Clock clock) {
    this(new NoOpMetricsRegistry(), clock);
  }

  public BlockingEnvelopeMap(MetricsRegistry metricsRegistry) {
    this(metricsRegistry, new Clock() {
      public long currentTimeMillis() {
        return System.currentTimeMillis();
      }
    });
  }

  public BlockingEnvelopeMap(MetricsRegistry metricsRegistry, Clock clock) {
    this(metricsRegistry, clock, null);
  }

  public BlockingEnvelopeMap(MetricsRegistry metricsRegistry, Clock clock, String metricsGroupName) {
    metricsGroupName = (metricsGroupName == null) ? this.getClass().getName() : metricsGroupName;
    this.metrics = new BlockingEnvelopeMapMetrics(metricsGroupName, metricsRegistry);
    this.bufferedMessages = new ConcurrentHashMap<SystemStreamPartition, BlockingQueue<IncomingMessageEnvelope>>();
    this.noMoreMessage = new ConcurrentHashMap<SystemStreamPartition, Boolean>();
    this.clock = clock;
  }

  public void register(SystemStreamPartition systemStreamPartition, String offset) {
    metrics.initMetrics(systemStreamPartition);
    bufferedMessages.putIfAbsent(systemStreamPartition, newBlockingQueue());
  }

  protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue() {
    return new LinkedBlockingQueue<IncomingMessageEnvelope>();
  }

  public Map<SystemStreamPartition, Queue<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {
    long stopTime = clock.currentTimeMillis() + timeout;
    Map<SystemStreamPartition, Queue<IncomingMessageEnvelope>> messagesToReturn = new HashMap<SystemStreamPartition, Queue<IncomingMessageEnvelope>>();

    metrics.incPoll();

    for (SystemStreamPartition systemStreamPartition : systemStreamPartitions) {
      Queue<IncomingMessageEnvelope> outgoingQueue = new ArrayDeque<IncomingMessageEnvelope>();
      BlockingQueue<IncomingMessageEnvelope> queue = bufferedMessages.get(systemStreamPartition);

      if (queue.size() > 0) {
        queue.drainTo(outgoingQueue);
      } else if (timeout != 0) {
        IncomingMessageEnvelope envelope = null;

        // How long we can legally block (if timeout > 0)
        long timeRemaining = stopTime - clock.currentTimeMillis();

        if (timeout == SystemConsumer.BLOCK_ON_OUTSTANDING_MESSAGES) {
          // Block until we get at least one message, or until we catch up to
          // the head of the stream.
          while (envelope == null && !isAtHead(systemStreamPartition)) {
            metrics.incBlockingPoll(systemStreamPartition);
            envelope = queue.poll(1000, TimeUnit.MILLISECONDS);
          }
        } else if (timeout > 0 && timeRemaining > 0) {
          // Block until we get at least one message.
          metrics.incBlockingTimeoutPoll(systemStreamPartition);
          envelope = queue.poll(timeRemaining, TimeUnit.MILLISECONDS);
        }

        // If we got a message, add it.
        if (envelope != null) {
          outgoingQueue.offer(envelope);
          // Drain any remaining messages without blocking.
          queue.drainTo(outgoingQueue);
        }
      }

      messagesToReturn.put(systemStreamPartition, outgoingQueue);
    }

    return messagesToReturn;
  }

  protected void put(SystemStreamPartition systemStreamPartition, IncomingMessageEnvelope envelope) throws InterruptedException {
    bufferedMessages.get(systemStreamPartition).put(envelope);
  }

  protected void putAll(SystemStreamPartition systemStreamPartition, List<IncomingMessageEnvelope> envelopes) throws InterruptedException {
    BlockingQueue<IncomingMessageEnvelope> queue = bufferedMessages.get(systemStreamPartition);

    for (IncomingMessageEnvelope envelope : envelopes) {
      queue.put(envelope);
    }
  }

  public int getNumMessagesInQueue(SystemStreamPartition systemStreamPartition) {
    BlockingQueue<IncomingMessageEnvelope> queue = bufferedMessages.get(systemStreamPartition);

    if (queue == null) {
      throw new NullPointerException("Attempting to get queue for " + systemStreamPartition + ", but the system/stream/partition was never registered.");
    } else {
      return queue.size();
    }
  }

  protected Boolean setIsAtHead(SystemStreamPartition systemStreamPartition, boolean isAtHead) {
    metrics.setNoMoreMessages(systemStreamPartition, isAtHead);
    return noMoreMessage.put(systemStreamPartition, isAtHead);
  }

  protected boolean isAtHead(SystemStreamPartition systemStreamPartition) {
    Boolean isAtHead = noMoreMessage.get(systemStreamPartition);

    return getNumMessagesInQueue(systemStreamPartition) == 0 && isAtHead != null && isAtHead.equals(true);
  }

  public class BlockingEnvelopeMapMetrics {
    private final String group;
    private final MetricsRegistry metricsRegistry;
    private final ConcurrentHashMap<SystemStreamPartition, Gauge<Boolean>> noMoreMessageGaugeMap;
    private final ConcurrentHashMap<SystemStreamPartition, Counter> blockingPollCountMap;
    private final ConcurrentHashMap<SystemStreamPartition, Counter> blockingPollTimeoutCountMap;
    private final Counter pollCount;

    public BlockingEnvelopeMapMetrics(String group, MetricsRegistry metricsRegistry) {
      this.group = group;
      this.metricsRegistry = metricsRegistry;
      this.noMoreMessageGaugeMap = new ConcurrentHashMap<SystemStreamPartition, Gauge<Boolean>>();
      this.blockingPollCountMap = new ConcurrentHashMap<SystemStreamPartition, Counter>();
      this.blockingPollTimeoutCountMap = new ConcurrentHashMap<SystemStreamPartition, Counter>();
      this.pollCount = metricsRegistry.newCounter(group, "poll-count");
    }

    public void initMetrics(SystemStreamPartition systemStreamPartition) {
      this.noMoreMessageGaugeMap.putIfAbsent(systemStreamPartition, metricsRegistry.<Boolean> newGauge(group, "no-more-messages-" + systemStreamPartition, false));
      this.blockingPollCountMap.putIfAbsent(systemStreamPartition, metricsRegistry.newCounter(group, "blocking-poll-count-" + systemStreamPartition));
      this.blockingPollTimeoutCountMap.putIfAbsent(systemStreamPartition, metricsRegistry.newCounter(group, "blocking-poll-timeout-count-" + systemStreamPartition));

      metricsRegistry.<Integer> newGauge(group, new BufferGauge(systemStreamPartition, "buffered-message-count-" + systemStreamPartition));
    }

    public void setNoMoreMessages(SystemStreamPartition systemStreamPartition, boolean noMoreMessages) {
      this.noMoreMessageGaugeMap.get(systemStreamPartition).set(noMoreMessages);
    }

    public void incBlockingPoll(SystemStreamPartition systemStreamPartition) {
      this.blockingPollCountMap.get(systemStreamPartition).inc();
    }

    public void incBlockingTimeoutPoll(SystemStreamPartition systemStreamPartition) {
      this.blockingPollTimeoutCountMap.get(systemStreamPartition).inc();
    }

    public void incPoll() {
      this.pollCount.inc();
    }
  }

  public class BufferGauge extends Gauge<Integer> {
    private final SystemStreamPartition systemStreamPartition;

    public BufferGauge(SystemStreamPartition systemStreamPartition, String name) {
      super(name, 0);

      this.systemStreamPartition = systemStreamPartition;
    }

    @Override
    public Integer getValue() {
      Queue<IncomingMessageEnvelope> envelopes = bufferedMessages.get(systemStreamPartition);

      if (envelopes == null) {
        return 0;
      }

      return envelopes.size();
    }
  }
}
