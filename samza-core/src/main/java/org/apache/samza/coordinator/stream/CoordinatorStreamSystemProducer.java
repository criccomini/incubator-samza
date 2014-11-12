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

package org.apache.samza.coordinator.stream;

import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * A wrapper around a SystemProducer that reads provides helpful methods for
 * dealing with the coordinator stream.
 */
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

  /**
   * Registers a source with the underlying SystemProducer.
   * 
   * @param source
   *          The source to register.
   */
  public void register(String source) {
    systemProducer.register(source);
  }

  /**
   * Creates the coordinator stream, and starts the system producer.
   */
  public void start() {
    systemAdmin.createCoordinatorStream(systemStream.getStream());
    systemProducer.start();
  }

  /**
   * Stops the underlying SystemProducer.
   */
  public void stop() {
    systemProducer.stop();
  }

  /**
   * Serialize and send a coordinator stream message.
   * 
   * @param message
   *          The message to send.
   */
  public void send(CoordinatorStreamMessage message) {
    try {
      String source = message.getSource();
      byte[] key = mapper.writeValueAsString(message.getKeyMap()).getBytes("UTF-8");
      byte[] value = null;
      if (!message.isDelete()) {
        value = mapper.writeValueAsString(message.getMessageMap()).getBytes("UTF-8");
      }
      OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(systemStream, Integer.valueOf(0), key, value);
      systemProducer.send(source, envelope);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  /**
   * Helper method that sends a series of SetConfig messages to the coordinator
   * stream.
   * 
   * @param source
   *          An identifier to denote which source is sending a message. This
   *          can be any arbitrary string.
   * @param config
   *          The config object to store in the coordinator stream.
   */
  public void writeConfig(String source, Config config) {
    for (Map.Entry<String, String> configPair : config.entrySet()) {
      send(new CoordinatorStreamMessage.SetConfig(source, configPair.getKey(), configPair.getValue()));
    }
  }
}
