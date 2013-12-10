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

package org.apache.samza.storage.kv

import scala.collection.JavaConversions._

class NullSafeKeyValueStore[K, V](store: KeyValueStore[K, V]) extends KeyValueStore[K, V] {
  def get(key: K): V = {
    notNull(key)
    store.get(key)
  }

  def put(key: K, value: V) {
    notNull(key)
    notNull(value)
    store.put(key, value)
  }

  def putAll(entries: java.util.List[Entry[K, V]]) {
    entries.foreach(entry => {
      notNull(entry.getKey)
      notNull(entry.getValue)
    })
    store.putAll(entries)
  }

  def delete(key: K) {
    notNull(key)
    store.delete(key)
  }

  def range(from: K, to: K): KeyValueIterator[K, V] = {
    notNull(from)
    notNull(to)
    store.range(from, to)
  }

  def all(): KeyValueIterator[K, V] = {
    store.all
  }

  def flush {
    store.flush
  }

  def close {
    store.close
  }

  def notNull[T](obj: T) = if (obj == null) {
    throw new NullPointerException("Null is not a valid key or value.")
  }
}