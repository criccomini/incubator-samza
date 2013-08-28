---
layout: page
title: Streams
---

The [TaskRunner](task-runner.html) reads and writes messages using the StreamConsumer and StreamProducer interfaces.

```
public interface StreamConsumer {
  StreamConsumerMessageEnvelope getMessage();

  void close();
}

public interface StreamConsumerMessageEnvelope {
  ByteBuffer getMessage();

  String getOffsetId();
}

public interface StreamProducer<K> {
  void send(ByteBuffer bytes);

  void send(K k, ByteBuffer bytes);

  void commit();

  void close();
}
```

Out of the box, Samza supports reads and writes to Kafka (i.e. it has a KafkaStreamConsumer/KafkaStreamProducer), but the stream interfaces are pluggable, and most message bus systems can be plugged in, with some degree of support.

A number of stream-related properties should be defined in your Samza job's configuration file. These properties define systems that Samza can read from, the streams on these systems, and how to serialize and deserialize the messages from the streams. For example, you might wish to read PageViewEvent from a specific Kafka cluster. The system properties in the configuration file would define how to connect to the Kafka cluster. The stream section would define PageViewEvent as an input stream. The serializer in the configuration would define the serde to use to decode PageViewEvent messages.

When the TaskRunner starts up, it will use the stream-related properties in your configuration to instantiate consumers for each stream partition. For example, if your input stream is PageViewEvent, which has 12 partitions, then the TaskRunner would create twelve KafkaStreamConsumers. Each stream consumer will read ByteBuffers from one partition, deserialize the ByteBuffer to an object, and put them into a queue. This queue is what the [event loop](event-loop.html) will use to feed messages to your StreamTask instances.

In the process method in StreamTask, there is a MessageCollector parameter given to use. When the TaskRunner calls process() on one of your StreamTask instances, it provides the collector. After the process() method completes, the TaskRunner takes any output messages that your StreamTask wrote to the collector, serializes the messages, and calls the send() method on the appropriate StreamProducer.

### Message Ordering

If a job is consuming messages from more than one system/stream/partition combination, by default, messages will be processed in a round robin fashion. For example, if a job is reading partitions 1 and 2 of page-view-events from a Kafka system, and there are messages available to be processed from both partitions, your StreamTask will get messages in round robin order.

This behavior can be overridden by implementing a custom MessageChooser. To write a custom MessageChooser, take a look at the Javadocs, and then configure your task with the "task.chooser.class" configuration, which should point to your MessageChooserFactory.

Out of the box, Samza ships with a RoundRobinChooser, which is the default, and a StreamChooser, which lets you favor certain streams over others. Samza also provides a helper class called PriorityChooser which lets you choose which message to process next based on a priority score.

#### Example

You can use the StreamChooser by adding the following configuration to your job.

```
task.chooser.class=org.apache.samza.system.StreamChooserFactory
task.chooser.stream.order=kafka.higher-priority-stream,kafka.lower-priority-stream
```

This configuration would always favor messages from the high priority stream over the low priority stream in cases where messages from both streams are available.

## [Checkpointing &raquo;](checkpointing.html)
