package org.apache.samza.job.coordinator.stream;

import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.codehaus.jackson.map.ObjectMapper;

public class CoordinatorStreamSystemProducer {
  private final ObjectMapper mapper;
  private final SystemStream systemStream;
  private final SystemProducer systemProducer;
  private final SystemAdmin systemAdmin;

  public CoordinatorStreamSystemProducer(SystemStream systemStream, SystemProducer systemProducer, SystemAdmin systemAdmin) {
    this(systemStream, systemProducer, systemAdmin, SamzaObjectMapper.getObjectMapper());
  }

  public CoordinatorStreamSystemProducer(SystemStream systemStream, SystemProducer systemProducer, SystemAdmin systemAdmin, ObjectMapper mapper) {
    this.systemStream = systemStream;
    this.systemProducer = systemProducer;
    this.systemAdmin = systemAdmin;
    this.mapper = mapper;
  }

  public void register(String source) {
    systemProducer.register(source);
  }

  public void start() {
    systemAdmin.createCoordinatorStream(systemStream.getStream());
    systemProducer.start();
  }

  public void stop() {
    systemProducer.stop();
  }

  public void send(CoordinatorStreamMessage message) {
    try {
      String source = message.getSource();
      byte[] key = mapper.writeValueAsString(message.getKey()).getBytes("UTF-8");
      byte[] value = mapper.writeValueAsString(message.getValue()).getBytes("UTF-8");
      OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(systemStream, Integer.valueOf(0), key, value);
      systemProducer.send(source, envelope);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  public void writeConfig(String source, Config config) {
    for (Map.Entry<String, String> configPair : config.entrySet()) {
      send(new CoordinatorStreamMessage.SetConfig(source, configPair.getKey(), configPair.getValue()));
    }
  }

  public void writeCheckpoint(String source, Checkpoint checkpoint) {
    // TODO
  }
}
