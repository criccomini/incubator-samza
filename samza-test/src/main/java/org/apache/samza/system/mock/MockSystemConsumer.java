package org.apache.samza.system.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.apache.samza.util.Clock;

public class MockSystemConsumer extends BlockingEnvelopeMap {
  private final int messagesPerBatch;
  private final int threadCount;
  private final Set<SystemStreamPartition> ssps;
  private List<Thread> threads;

  public MockSystemConsumer(int messagesPerBatch, int threadCount) {
    super(new MetricsRegistryMap("test-container-performance"), new Clock() {
      @Override
      public long currentTimeMillis() {
        return System.currentTimeMillis();
      }
    });

    this.messagesPerBatch = messagesPerBatch;
    this.threadCount = threadCount;
    this.ssps = new HashSet<SystemStreamPartition>();
    this.threads = new ArrayList<Thread>(threadCount);
  }

  @Override
  public void start() {
    for (int i = 0; i < threadCount; ++i) {
      // Assign SystemStreamPartitions for this thread.
      Set<SystemStreamPartition> threadSsps = new HashSet<SystemStreamPartition>();

      for (SystemStreamPartition ssp : ssps) {
        if (Math.abs(ssp.hashCode()) % threadCount == i) {
          threadSsps.add(ssp);
        }
      }

      // Start thread.
      Thread thread = new Thread(new MockSystemConsumerRunnable(threadSsps));
      thread.setDaemon(true);
      threads.add(thread);
      thread.start();
    }
  }

  @Override
  public void stop() {
    for (Thread thread : threads) {
      thread.interrupt();
    }

    try {
      for (Thread thread : threads) {
        thread.join();
      }
    } catch (InterruptedException e) {
    }
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String lastReadOffset) {
    super.register(systemStreamPartition, lastReadOffset);
    ssps.add(systemStreamPartition);
    setIsAtHead(systemStreamPartition, true);
  }

  public class MockSystemConsumerRunnable implements Runnable {
    private final Set<SystemStreamPartition> ssps;

    public MockSystemConsumerRunnable(Set<SystemStreamPartition> ssps) {
      this.ssps = ssps;
    }

    @Override
    public void run() {
      try {
        while (!Thread.interrupted() && ssps.size() > 0) {
          Set<SystemStreamPartition> sspsToFetch = new HashSet<SystemStreamPartition>();

          // Only fetch messages when there are no outstanding messages left.
          for (SystemStreamPartition ssp : ssps) {
            if (getNumMessagesInQueue(ssp) <= 0) {
              sspsToFetch.add(ssp);
            }
          }

          // Simulate a broker fetch request's network latency.
          Thread.sleep(1);

          for (SystemStreamPartition ssp : sspsToFetch) {
            for (int i = 0; i < messagesPerBatch; ++i) {
              add(ssp, new IncomingMessageEnvelope(ssp, "0", "key", "value"));
            }
          }
        }
      } catch (InterruptedException e) {
        System.out.println("Got interrupt. Shutting down.");
      }
    }
  }
}
