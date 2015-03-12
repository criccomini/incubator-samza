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

package org.apache.samza.logging.log4j.serializers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;

public class LoggingEventJsonSerde implements Serde<LoggingEvent> {
  public static final int VERSION = 1;
  public static final Format DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

  // Have to wrap rather than extend due to type collisions between
  // Serde<LoggingEvent> and SerdeObject>.
  private final JsonSerde jsonSerde;
  private final boolean includeLocationInfo;

  public LoggingEventJsonSerde() {
    this(false);
  }

  public LoggingEventJsonSerde(boolean includeLocationInfo) {
    this.includeLocationInfo = includeLocationInfo;
    this.jsonSerde = new JsonSerde();
  }

  @Override
  public byte[] toBytes(LoggingEvent loggingEvent) {
    Map<String, Object> loggingEventMap = encodeToMap(loggingEvent, includeLocationInfo);
    return jsonSerde.toBytes(loggingEventMap);
  }

  @SuppressWarnings("unchecked")
  @Override
  public LoggingEvent fromBytes(byte[] loggingEventMapBytes) {
    Map<String, Object> loggingEventMap = (Map<String, Object>) jsonSerde.fromBytes(loggingEventMapBytes);
    return decodeFromMap(loggingEventMap);
  }

  @SuppressWarnings("rawtypes")
  public static Map<String, Object> encodeToMap(LoggingEvent loggingEvent, boolean includeLocationInfo) {
    Map<String, Object> logstashEvent = new LoggingEventMap();
    String threadName = loggingEvent.getThreadName();
    long timestamp = loggingEvent.getTimeStamp();
    HashMap<String, Object> exceptionInformation = new HashMap<String, Object>();
    Map mdc = loggingEvent.getProperties();
    String ndc = loggingEvent.getNDC();

    logstashEvent.put("@version", VERSION);
    logstashEvent.put("@timestamp", dateFormat(timestamp));
    logstashEvent.put("source_host", getHostname());
    logstashEvent.put("message", loggingEvent.getRenderedMessage());

    if (loggingEvent.getThrowableInformation() != null) {
      final ThrowableInformation throwableInformation = loggingEvent.getThrowableInformation();
      if (throwableInformation.getThrowable().getClass().getCanonicalName() != null) {
        exceptionInformation.put("exception_class", throwableInformation.getThrowable().getClass().getCanonicalName());
      }
      if (throwableInformation.getThrowable().getMessage() != null) {
        exceptionInformation.put("exception_message", throwableInformation.getThrowable().getMessage());
      }
      if (throwableInformation.getThrowableStrRep() != null) {
        StringBuilder stackTrace = new StringBuilder();
        for (String line : throwableInformation.getThrowableStrRep()) {
          stackTrace.append(line);
          stackTrace.append("\n");
        }
        exceptionInformation.put("stacktrace", stackTrace);
      }
      logstashEvent.put("exception", exceptionInformation);
    }

    if (includeLocationInfo) {
      LocationInfo info = loggingEvent.getLocationInformation();
      logstashEvent.put("file", info.getFileName());
      logstashEvent.put("line_number", info.getLineNumber());
      logstashEvent.put("class", info.getClassName());
      logstashEvent.put("method", info.getMethodName());
    }

    logstashEvent.put("logger_name", loggingEvent.getLoggerName());
    logstashEvent.put("mdc", mdc);
    logstashEvent.put("ndc", ndc);
    logstashEvent.put("level", loggingEvent.getLevel().toString());
    logstashEvent.put("thread_name", threadName);

    return logstashEvent;
  }

  public static LoggingEvent decodeFromMap(Map<String, Object> loggingEventMap) {
    return null;
  }

  public static String dateFormat(long time) {
    return DATE_FORMAT.format(new Date(time));
  }

  public static String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return "unknown-host";
    }
  }

  @SuppressWarnings("serial")
  public static final class LoggingEventMap extends HashMap<String, Object> {
    public Object put(String key, Object value) {
      if (value == null) {
        return get(key);
      } else {
        return super.put(key, value);
      }
    }
  }
}
