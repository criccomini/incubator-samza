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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * The full structure for a CoordinatorStreamMessage is:
 * </p>
 * 
 * <pre>
 * key =&gt; {
 *   "version": 1,
 *   "type": "set-config",
 *   "key": "job.name"
 * }
 * 
 * message =&gt; {
 *   "host": "192.168.0.1",
 *   "username": "criccomini",
 *   "source": "job-runner",
 *   "timestamp": 123456789,
 *   "values": {
 *     "value": "my-job-name"
 *   } 
 * }
 * </pre>
 * 
 * <p>
 * The "values" map in the message is defined on a per-message-type basis. For
 * set-config messages, there is just a single key/value pair, where the "value"
 * key is defined. For offset messages, there will be multiple key/values pairs
 * in "values" (one for each SystemStreamPartition/offset pair for a given
 * TaskName).
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
  private static final Logger log = LoggerFactory.getLogger(CoordinatorStreamMessage.class);

  /**
   * Protocol version for coordinator stream messages. This version number must
   * be incremented any time new messages are added to the coordinator stream,
   * or changes are made to the key/message headers.
   */
  public static final int VERSION = 1;

  /**
   * Contains all key fields. Currently, this includes the type of the message,
   * the key associated with the type (e.g. type: set-config key: job.name), and
   * the version of the protocol.
   */
  private final Map<String, Object> keyMap;

  /**
   * Contains all fields for the message. This includes who sent the message,
   * the host, etc. It also includes a "values" map, which contains all values
   * associated with the key of the message. If set-config/job.name were used as
   * the type/key of the message, then values would contain
   * {"value":"my-job-name"}.
   */
  private final Map<String, Object> messageMap;
  private boolean isDelete;

  public CoordinatorStreamMessage(Map<String, Object> keyMap, Map<String, Object> messageMap) {
    this.keyMap = keyMap;
    this.messageMap = messageMap;
    this.isDelete = messageMap == null;
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
      log.warn("Unable to retrieve host for current machine. Setting coordinator stream message host field to an empty string.");
      setHost("");
    }
  }

  protected void setIsDelete(boolean isDelete) {
    this.isDelete = isDelete;
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
  protected Map<String, String> getMessageValues() {
    return (Map<String, String>) this.messageMap.get("values");
  }

  protected String getMessageValue(String key) {
    return getMessageValues().get(key);
  }

  protected void putMessageValue(String key, String value) {
    getMessageValues().put(key, value);
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
   * @return Whether the message signifies a delete or not.
   */
  public boolean isDelete() {
    return isDelete;
  }

  /**
   * @return The whole message map including header information.
   */
  public Map<String, Object> getMessageMap() {
    if (!isDelete) {
      Map<String, Object> immutableMap = new HashMap<String, Object>(messageMap);
      immutableMap.put("values", Collections.unmodifiableMap(getMessageValues()));
      return Collections.unmodifiableMap(immutableMap);
    } else {
      return null;
    }
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

  @Override
  public String toString() {
    return "CoordinatorStreamMessage [keyMap=" + keyMap + ", messageMap=" + messageMap + ", isDelete=" + isDelete + "]";
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
      putMessageValue("value", value);
    }

    public String getConfigValue() {
      return (String) getMessageValue("value");
    }
  }

  public static class Delete extends CoordinatorStreamMessage {
    public Delete(String source, String key, String type) {
      this(source, key, type, VERSION);
    }

    public Delete(String source, String key, String type, int version) {
      super(source);
      setType(type);
      setKey(key);
      setVersion(version);
      setIsDelete(true);
    }
  }
}
