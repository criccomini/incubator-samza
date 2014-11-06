package org.apache.samza.coordinator.stream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamMessage.SetConfig;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionIterator;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class CoordinatorStreamSystemConsumer {
  private final SystemStreamPartition coordinatorSystemStreamPartition;
  private final SystemConsumer systemConsumer;
  private final SystemAdmin systemAdmin;
  private final Map<String, String> configMap;
  private final ObjectMapper mapper;

  public CoordinatorStreamSystemConsumer(SystemStream coordinatorSystemStream, SystemConsumer systemConsumer, SystemAdmin systemAdmin, ObjectMapper mapper) {
    this.coordinatorSystemStreamPartition = new SystemStreamPartition(coordinatorSystemStream, new Partition(0));
    this.systemConsumer = systemConsumer;
    this.systemAdmin = systemAdmin;
    this.mapper = mapper;
    this.configMap = new HashMap<String, String>();
  }

  public CoordinatorStreamSystemConsumer(SystemStream coordinatorSystemStream, SystemConsumer systemConsumer, SystemAdmin systemAdmin) {
    this(coordinatorSystemStream, systemConsumer, systemAdmin, SamzaObjectMapper.getObjectMapper());
  }

  public void register() {
    Set<String> streamNames = new HashSet<String>();
    String streamName = coordinatorSystemStreamPartition.getStream();
    streamNames.add(streamName);
    Map<String, SystemStreamMetadata> systemStreamMetadataMap = systemAdmin.getSystemStreamMetadata(streamNames);
    SystemStreamMetadata systemStreamMetadata = systemStreamMetadataMap.get(streamName);

    if (systemStreamMetadata == null) {
      throw new SamzaException("Expected " + streamName + " to be in system stream metadata.");
    }

    SystemStreamPartitionMetadata systemStreamPartitionMetadata = systemStreamMetadata.getSystemStreamPartitionMetadata().get(coordinatorSystemStreamPartition.getPartition());

    if (systemStreamPartitionMetadata == null) {
      throw new SamzaException("Expected metadata for " + coordinatorSystemStreamPartition + " to exist.");
    }

    String startingOffset = systemStreamPartitionMetadata.getOldestOffset();
    systemConsumer.register(coordinatorSystemStreamPartition, startingOffset);
  }

  public void start() {
    systemConsumer.start();
  }

  public void stop() {
    systemConsumer.stop();
  }

  public void bootstrap() {
    SystemStreamPartitionIterator iterator = new SystemStreamPartitionIterator(systemConsumer, coordinatorSystemStreamPartition);

    try {
      while (iterator.hasNext()) {
        IncomingMessageEnvelope envelope = iterator.next();
        String keyStr = new String((byte[]) envelope.getKey(), "UTF-8");
        String valueStr = new String((byte[]) envelope.getMessage(), "UTF-8");
        Map<String, Object> keyMap = mapper.readValue(keyStr, new TypeReference<Map<String, String>>() {});
        Map<String, Object> valueMap = mapper.readValue(valueStr, new TypeReference<Map<String, String>>() {});
        CoordinatorStreamMessage coordinatorStreamMessage = new CoordinatorStreamMessage(keyMap, valueMap);
        if (SetConfig.TYPE.equals(coordinatorStreamMessage.getType())) {
          String configKey = coordinatorStreamMessage.getKeyEntry();
          String configValue = new SetConfig(coordinatorStreamMessage).getConfigValue();
          configMap.put(configKey, configValue);
        }
      }
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  public Config getConfig() {
    return new MapConfig(configMap);
  }
}
