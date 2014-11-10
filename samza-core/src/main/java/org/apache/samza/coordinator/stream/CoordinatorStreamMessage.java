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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;

/**
 * <p>
 * Represents a message for the job coordinator. All messages in the coordinator
 * stream must wrap the CoordinatorStreamMessage class. Coordinator stream
 * messages are modeled as key/value pairs, both of which are maps. There are
 * some pre-defined fields (such as type, key, timestamp, host, etc) for the
 * maps, which are common to all messages.
 * </p>
 * 
 * <p>
 * The most important fields are type, key, and values. The type field defines
 * the kind of message, the key defines a key to associate with the values, and
 * the values map defines a set of values associated with the type. A concrete
 * example would be a config message of type "set-config" with key "job.name"
 * and values {"value": "my-job-name"}.
 * </p>
 */
public class CoordinatorStreamMessage {

  /**
   * Protocol version for coordinator stream messages. This version number must
   * be incremented any time new messages are added to the coordinator stream,
   * or changes are made to the key/message headers.
   */
  public static final int VERSION = 1;

  private final Map<String, Object> keyMap;
  private final Map<String, Object> messageMap;

  public CoordinatorStreamMessage(Map<String, Object> keyMap, Map<String, Object> messageMap) {
    this.keyMap = keyMap;
    this.messageMap = messageMap;
  }

  public CoordinatorStreamMessage(String source) {
    this(new HashMap<String, Object>(), new HashMap<String, Object>());
    this.messageMap.put("values", new HashMap<String, String>());
    setSource(source);
    setVersion(VERSION);
    setUsername(System.getProperty("user.name"));
    setTimestamp(System.currentTimeMillis());

    try {
      setHost(InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      throw new SamzaException(e);
    }
  }

  protected void setHost(String host) {
    messageMap.put("host", host);
  }

  protected void setUsername(String username) {
    messageMap.put("username", username);
  }

  protected void setSource(String source) {
    messageMap.put("source", source);
  }

  protected void setTimestamp(long timestamp) {
    messageMap.put("timestamp", Long.valueOf(timestamp));
  }

  protected void setVersion(int version) {
    this.keyMap.put("version", Integer.valueOf(version));
  }

  protected void setType(String type) {
    this.keyMap.put("type", type);
  }

  protected void setKey(String key) {
    this.keyMap.put("key", key);
  }

  @SuppressWarnings("unchecked")
  protected String getValue(String key) {
    return ((Map<String, String>) this.messageMap.get("values")).get(key);
  }

  @SuppressWarnings("unchecked")
  protected void putValue(String key, String value) {
    Map<String, String> values = (Map<String, String>) messageMap.get("values");
    values.put(key, value);
  }

  /**
   * The type of the message is used to convert a generic
   * CoordinatorStreaMessage into a specific message, such as a SetConfig
   * message.
   * 
   * @return The type of the message.
   */
  public String getType() {
    return (String) this.keyMap.get("type");
  }

  /**
   * @return The whole key map including both the key and type of the message.
   */
  public Map<String, Object> getKeyMap() {
    return Collections.unmodifiableMap(keyMap);
  }

  /**
   * @return The whole message map including header information.
   */
  @SuppressWarnings("unchecked")
  public Map<String, Object> getMessageMap() {
    Map<String, Object> immutableMap = new HashMap<String, Object>(messageMap);
    immutableMap.put("values", Collections.unmodifiableMap((Map<String, String>) messageMap.get("values")));
    return Collections.unmodifiableMap(immutableMap);
  }

  /**
   * @return The source that sent the coordinator message. This is a string
   *         defined by the sender.
   */
  public String getSource() {
    return (String) messageMap.get("source");
  }

  /**
   * @return The key for a message. The key's meaning is defined by the type of
   *         the message.
   */
  public String getKey() {
    return (String) this.keyMap.get("key");
  }

  /**
   * A coordinator stream message that tells the job coordinator to set a
   * specific configuration.
   */
  public static class SetConfig extends CoordinatorStreamMessage {
    public static final String TYPE = "set-config";

    public SetConfig(CoordinatorStreamMessage message) {
      super(message.getKeyMap(), message.getMessageMap());
    }

    public SetConfig(String source, String key, String value) {
      super(source);
      setType(TYPE);
      setKey(key);
      putValue("value", value);
    }

    public String getConfigValue() {
      return (String) getValue("value");
    }
  }
}
