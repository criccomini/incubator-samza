package org.apache.samza.system.mock;

import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;

public class MockSystemFactory implements SystemFactory {

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    MockSystemConsumerConfig consumerConfig = new MockSystemConsumerConfig(systemName, config);

    return new MockSystemConsumer(consumerConfig.getMessagesPerBatch(), consumerConfig.getConsumerThreadCount());
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    throw new RuntimeException("MockSystemProducer not implemented.");
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    MockSystemConsumerConfig consumerConfig = new MockSystemConsumerConfig(systemName, config);

    return new MockSystemAdmin(consumerConfig.getPartitionsPerStream());
  }

  public static class MockSystemConsumerConfig {
    public static final int DEFAULT_PARTITION_COUNT = 4;
    public static final int DEFAULT_MESSAGES_PER_BATCH = 5000;
    public static final int DEFAULT_CONSUMER_THREAD_COUNT = 12;

    private final String systemName;
    private final Config config;

    public MockSystemConsumerConfig(String systemName, Config config) {
      this.systemName = systemName;
      this.config = config;
    }

    public int getPartitionsPerStream() {
      return config.getInt("systems." + systemName + ".partitions.per.stream", DEFAULT_PARTITION_COUNT);
    }

    public int getMessagesPerBatch() {
      return config.getInt("systems." + systemName + ".messages.per.batch", DEFAULT_MESSAGES_PER_BATCH);
    }

    public int getConsumerThreadCount() {
      return config.getInt("systems." + systemName + ".consumer.thread.count", DEFAULT_CONSUMER_THREAD_COUNT);
    }
  }
}
