package org.apache.samza.job.coordinator.stream;

import java.io.IOException;

import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class CoordinatorStreamSystemProducer {
  private final ObjectMapper mapper;
  private final SystemStream systemStream;
  private final SystemProducer systemProducer;

  public CoordinatorStreamSystemProducer(SystemStream systemStream, SystemProducer systemProducer) {
    this(systemStream, systemProducer, SamzaObjectMapper.getObjectMapper());
  }

  public CoordinatorStreamSystemProducer(SystemStream systemStream, SystemProducer systemProducer, ObjectMapper mapper) {
    this.systemStream = systemStream;
    this.systemProducer = systemProducer;
    this.mapper = mapper;
  }

  public void send(CoordinatorStreamMessage message) throws JsonGenerationException, JsonMappingException, IOException {
    String source = message.getSource();
    String key = mapper.writeValueAsString(message.getKey());
    String value = mapper.writeValueAsString(message.getValue());
    OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(systemStream, Integer.valueOf(0), key, value);
    systemProducer.send(source, envelope);
  }
}
