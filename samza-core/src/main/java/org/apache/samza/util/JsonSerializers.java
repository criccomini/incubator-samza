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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.ObjectCodec;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.type.TypeReference;

public class JsonSerializers {
  private static final ObjectMapper OBJECT_MAPPER = getObjectMapper();

  public static ObjectMapper getObjectMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule("host-thing", new Version(1, 0, 0, ""));
    module.addDeserializer(Partition.class, new PartitionDeserializer());
    module.addSerializer(Partition.class, new PartitionSerializer());
    module.addDeserializer(TaskName.class, new TaskNameDeserializer());
    module.addSerializer(TaskName.class, new TaskNameSerializer());
    module.addDeserializer(SystemStreamPartition.class, new SystemStreamPartitionDeserializer());
    module.addSerializer(SystemStreamPartition.class, new SystemStreamPartitionSerializer());
    module.addDeserializer(TaskModel.class, new TaskModelDeserializer());
    module.addDeserializer(ContainerModel.class, new ContainerModelDeserializer());
    module.addDeserializer(JobModel.class, new JobModelDeserializer());
    objectMapper.registerModule(module);
    return objectMapper;
  }

  public static class PartitionSerializer extends JsonSerializer<Partition> {
    @Override
    public void serialize(Partition partition, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(Integer.valueOf(partition.getPartitionId()));
    }
  }

  public static class PartitionDeserializer extends JsonDeserializer<Partition> {
    @Override
    public Partition deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      return new Partition(node.getIntValue());
    }
  }

  public static class TaskNameSerializer extends JsonSerializer<TaskName> {
    @Override
    public void serialize(TaskName taskName, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException, JsonProcessingException {
      jsonGenerator.writeObject(taskName.toString());
    }
  }

  public static class TaskNameDeserializer extends JsonDeserializer<TaskName> {
    @Override
    public TaskName deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      return new TaskName(node.get("taskName").getTextValue());
    }
  }

  public static class SystemStreamPartitionSerializer extends JsonSerializer<SystemStreamPartition> {
    @Override
    public void serialize(SystemStreamPartition systemStreamPartition, JsonGenerator jsonGenerator, SerializerProvider provider) throws IOException, JsonProcessingException {
      Map<String, Object> systemStreamPartitionMap = new HashMap<String, Object>();
      systemStreamPartitionMap.put("system", systemStreamPartition.getSystem());
      systemStreamPartitionMap.put("stream", systemStreamPartition.getStream());
      systemStreamPartitionMap.put("partition", systemStreamPartition.getPartition());
      jsonGenerator.writeObject(systemStreamPartitionMap);
    }
  }

  public static class SystemStreamPartitionDeserializer extends JsonDeserializer<SystemStreamPartition> {
    @Override
    public SystemStreamPartition deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      String system = node.get("system").getTextValue();
      String stream = node.get("stream").getTextValue();
      Partition partition = new Partition(node.get("partition").getIntValue());
      return new SystemStreamPartition(system, stream, partition);
    }
  }

  public static class TaskModelDeserializer extends JsonDeserializer<TaskModel> {
    @Override
    public TaskModel deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      TaskName taskName = OBJECT_MAPPER.readValue(node, TaskName.class);
      Set<SystemStreamPartition> systemStreamPartitions = OBJECT_MAPPER.readValue(node.get("systemStreamPartitions"), new TypeReference<Set<SystemStreamPartition>>() {
      });
      Partition changelogPartition = OBJECT_MAPPER.readValue(node.get("changelogPartition"), Partition.class);
      return new TaskModel(taskName, systemStreamPartitions, changelogPartition);
    }
  }

  public static class ContainerModelDeserializer extends JsonDeserializer<ContainerModel> {
    @Override
    public ContainerModel deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      int containerId = node.get("containerId").getIntValue();
      Map<TaskName, TaskModel> tasks = OBJECT_MAPPER.readValue(node.get("tasks"), new TypeReference<Map<TaskName, TaskModel>>() {
      });
      return new ContainerModel(containerId, tasks);
    }
  }

  public static class JobModelDeserializer extends JsonDeserializer<JobModel> {
    @Override
    public JobModel deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException, JsonProcessingException {
      ObjectCodec oc = jsonParser.getCodec();
      JsonNode node = oc.readTree(jsonParser);
      Config config = new MapConfig(OBJECT_MAPPER.<Map<String, String>> readValue(node.get("config"), new TypeReference<Map<String, String>>() {
      }));
      Map<Integer, ContainerModel> containers = OBJECT_MAPPER.readValue(node.get("containers"), new TypeReference<Map<Integer, ContainerModel>>() {
      });
      return new JobModel(config, containers);
    }
  }
}
