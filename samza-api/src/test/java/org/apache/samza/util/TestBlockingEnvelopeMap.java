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

import static org.junit.Assert.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;

public class TestBlockingEnvelopeMap {
  private static final SystemStreamPartition SSP = new SystemStreamPartition("test", "test", new Partition(0));
  private static final IncomingMessageEnvelope envelope = new IncomingMessageEnvelope(SSP, null, null, null, null);
  private static final Map<SystemStreamPartition, Integer> FETCH = new HashMap<SystemStreamPartition, Integer>();

  static {
    FETCH.put(SSP, 10);
  }

  @Test
  public void testEmptyMapReturnsEmptyList() throws InterruptedException {
    BlockingEnvelopeMap map = new MockBlockingEnvelopeMap();
    map.register(SSP, "0");
    map.poll(FETCH, 0);
    map.poll(FETCH, 30);
    map.setIsAtHead(SSP, true);
    map.poll(FETCH, -1);
  }

  @Test
  public void testShouldBlockAtLeast100Ms() throws InterruptedException {
    BlockingEnvelopeMap map = new MockBlockingEnvelopeMap();
    map.register(SSP, "0");
    long now = System.currentTimeMillis();
    map.poll(FETCH, 100);
    assertTrue(System.currentTimeMillis() - now >= 100);
  }

  @Test
  public void testShouldGetSomeMessages() throws InterruptedException {
    BlockingEnvelopeMap map = new MockBlockingEnvelopeMap();
    map.register(SSP, "0");
    map.put(SSP, envelope);
    List<IncomingMessageEnvelope> envelopes = map.poll(FETCH, 0);
    assertEquals(1, envelopes.size());
    map.put(SSP, envelope);
    map.put(SSP, envelope);
    envelopes = map.poll(FETCH, 0);
    assertEquals(2, envelopes.size());
  }

  @Test
  public void testShouldNotReturnMoreEnvelopesThanAllowed() throws InterruptedException {
    BlockingEnvelopeMap map = new MockBlockingEnvelopeMap();
    int maxMessages = FETCH.get(SSP);

    map.register(SSP, "0");

    for (int i = 0; i < 3 * maxMessages; ++i) {
      map.put(SSP, envelope);
    }

    assertEquals(3 * maxMessages, map.getNumMessagesInQueue(SSP));
    assertEquals(maxMessages, map.poll(FETCH, 0).size());
    assertEquals(2 * maxMessages, map.getNumMessagesInQueue(SSP));
    assertEquals(maxMessages, map.poll(FETCH, 30).size());
    assertEquals(maxMessages, map.getNumMessagesInQueue(SSP));
    assertEquals(maxMessages, map.poll(FETCH, 0).size());
    assertEquals(0, map.getNumMessagesInQueue(SSP));
    assertEquals(0, map.poll(FETCH, 0).size());
  }

  @Test
  public void testShouldBlockWhenNotAtHead() throws InterruptedException {
    MockQueue q = new MockQueue();
    final BlockingEnvelopeMap map = new MockBlockingEnvelopeMap(q);

    map.register(SSP, "0");

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // Should trigger a take() call.
          map.poll(FETCH, -1);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });

    t.setDaemon(true);
    t.start();
    q.awaitPollTimeout();
    t.join(60000);

    // 1000 = blocking timeout constant
    assertEquals(1000, q.timeout);
    assertFalse(t.isAlive());
  }

  @Test
  public void testShouldPollWithATimeout() throws InterruptedException {
    MockQueue q = new MockQueue();
    // Always use the same time in this test so that we can be sure we get a
    // 100ms poll, rather than a 99ms poll (for example). Have to do this
    // because BlockingEnvelopeMap calls clock.currentTimeMillis twice, and
    // uses the second call to determine the actual poll time.
    final BlockingEnvelopeMap map = new MockBlockingEnvelopeMap(q, new Clock() {
      private final long NOW = System.currentTimeMillis();

      public long currentTimeMillis() {
        return NOW;
      }
    });

    map.register(SSP, "0");

    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          // Should trigger a poll(100, TimeUnit.MILLISECONDS) call.
          map.poll(FETCH, 100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    });

    t.setDaemon(true);
    t.start();
    q.awaitPollTimeout();
    t.join(60000);

    assertEquals(100, q.timeout);
    assertFalse(t.isAlive());
  }

  public class MockQueue extends LinkedBlockingQueue<IncomingMessageEnvelope> {
    private static final long serialVersionUID = 1L;
    private final CountDownLatch pollTimeoutBarrier;
    private long timeout;

    public MockQueue() {
      this.pollTimeoutBarrier = new CountDownLatch(1);
    }

    public void awaitPollTimeout() throws InterruptedException {
      pollTimeoutBarrier.await();
    }

    @Override
    public IncomingMessageEnvelope poll(long timeout, TimeUnit unit) {
      this.timeout = timeout;

      pollTimeoutBarrier.countDown();

      return envelope;
    }
  }

  public class MockBlockingEnvelopeMap extends BlockingEnvelopeMap {
    private final BlockingQueue<IncomingMessageEnvelope> injectedQueue;

    public MockBlockingEnvelopeMap() {
      this(null);
    }

    public MockBlockingEnvelopeMap(BlockingQueue<IncomingMessageEnvelope> injectedQueue) {
      this(injectedQueue, new Clock() {
        public long currentTimeMillis() {
          return System.currentTimeMillis();
        }
      });
    }

    public MockBlockingEnvelopeMap(BlockingQueue<IncomingMessageEnvelope> injectedQueue, Clock clock) {
      super(new NoOpMetricsRegistry(), clock);

      this.injectedQueue = injectedQueue;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    protected BlockingQueue<IncomingMessageEnvelope> newBlockingQueue() {
      if (injectedQueue != null) {
        return injectedQueue;
      } else {
        return super.newBlockingQueue();
      }
    }
  }
}
