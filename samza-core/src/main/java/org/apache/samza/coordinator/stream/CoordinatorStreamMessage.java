package org.apache.samza.coordinator.stream;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.samza.SamzaException;

public class CoordinatorStreamMessage {
  public static final int VERSION = 1;

  private final Map<String, Object> keyMap;
  private final Map<String, Object> valueMap;

  public CoordinatorStreamMessage(Map<String, Object> key, Map<String, Object> value) {
    this.keyMap = key;
    this.valueMap = value;
  }

  public CoordinatorStreamMessage(String source) {
    this(new HashMap<String, Object>(), new HashMap<String, Object>());
    this.valueMap.put("values", new HashMap<String, String>());
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
    valueMap.put("host", host);
  }

  protected void setUsername(String username) {
    valueMap.put("username", username);
  }

  protected void setSource(String source) {
    valueMap.put("source", source);
  }

  protected void setTimestamp(long timestamp) {
    valueMap.put("timestamp", Long.valueOf(timestamp));
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
  protected Object getValueEntry(String key) {
    return ((Map<String, Object>) this.valueMap.get("values")).get(key);
  }

  @SuppressWarnings("unchecked")
  protected void putValue(String key, String value) {
    Map<String, String> values = (Map<String, String>) valueMap.get("values");
    values.put(key, value);
  }

  public String getType() {
    return (String) this.keyMap.get("type");
  }

  public Map<String, Object> getKey() {
    return Collections.unmodifiableMap(keyMap);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getValue() {
    Map<String, Object> immutableMap = new HashMap<String, Object>(valueMap);
    immutableMap.put("values", Collections.unmodifiableMap((Map<String, String>) valueMap.get("values")));
    return Collections.unmodifiableMap(immutableMap);
  }

  public String getSource() {
    return (String) valueMap.get("source");
  }

  public String getKeyEntry() {
    return (String) this.keyMap.get("key");
  }

  public static class SetConfig extends CoordinatorStreamMessage {
    public static final String TYPE = "set-config";

    public SetConfig(CoordinatorStreamMessage message) {
      super(message.getKey(), message.getValue());
    }

    public SetConfig(String source, String key, String value) {
      super(source);
      setType(TYPE);
      setKey(key);
      putValue("value", value);
    }

    public String getConfigValue() {
      return (String) getValueEntry("value");
    }
  }
}
