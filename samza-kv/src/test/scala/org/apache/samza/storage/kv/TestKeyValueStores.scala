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

import java.io.File
import java.util.Arrays
import java.util.Random

import scala.collection.JavaConversions._

import org.apache.samza.serializers.IntegerSerde
import org.iq80.leveldb.Options
import org.junit.After
import org.junit.Assert._
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

@RunWith(value = classOf[Parameterized])
class TestKeyValueStores(cache: Boolean) {

  import TestKeyValueStores._

  val letters = "abcdefghijklmnopqrstuvwxyz".map(_.toString)
  val dir = new File(System.getProperty("java.io.tmpdir"), "leveldb-test-" + new Random().nextInt(Int.MaxValue))
  var store: KeyValueStore[Array[Byte], Array[Byte]] = null

  @Before
  def setup() {
    dir.mkdirs()
    val leveldb = new LevelDbKeyValueStore(dir, new Options)
    if (cache)
      store = new CachedStore(leveldb, CacheSize, BatchSize)
    else
      store = leveldb
  }

  @After
  def teardown() {
    for (file <- dir.listFiles)
      file.delete()
    dir.delete()
  }

  @Test
  def getNonExistantIsNull() {
    assertNull(store.get(b("hello")))
  }

  @Test
  def putAndGet() {
    store.put(b("k"), b("v"))
    assertTrue(Arrays.equals(b("v"), store.get(b("k"))))
  }

  @Test
  def doublePutAndGet() {
    val k = b("k2")
    store.put(k, b("v1"))
    store.put(k, b("v2"))
    store.put(k, b("v3"))
    assertTrue(Arrays.equals(b("v3"), store.get(k)))
  }

  @Test
  def testPutAll() {
    // Use CacheSize - 1 so we fully fill the cache, but don't write any data 
    // out. Our check (below) uses == for cached entries, and using 
    // numEntires >= CacheSize would result in the LRU cache dropping some 
    // entries. The result would be that we get the correct byte array back 
    // from the cache's underlying store (leveldb), but that == would fail.
    val numEntries = CacheSize - 1
    val entries = (0 until numEntries).map(i => new Entry(b("k" + i), b("v" + i)))
    store.putAll(entries)
    if (cache) {
      assertTrue("All values should be found and cached.", entries.forall(e => store.get(e.getKey) == e.getValue))
    } else {
      assertTrue("All values should be found.", entries.forall(e => Arrays.equals(store.get(e.getKey), e.getValue)))
    }
  }

  @Test
  def testIterateAll() {
    for (letter <- letters)
      store.put(b(letter.toString), b(letter.toString))
    val iter = store.all
    checkRange(letters, iter)
    iter.close()
  }

  @Test
  def testRange() {
    val from = 5
    val to = 20
    for (letter <- letters)
      store.put(b(letter.toString), b(letter.toString))
    val iter = store.range(b(letters(from)), b(letters(to)))
    checkRange(letters.slice(from, to + 1), iter)
    iter.close()
  }

  @Test
  def testDelete() {
    val a = b("a")
    store.put(a, a)
    store.delete(a)
    assertNull(store.get(a))
  }

  @Test
  def testSimpleScenario() {
    val vals = letters.map(b(_))
    for (v <- vals) {
      assertNull(store.get(v))
      store.put(v, v)
      assertTrue(Arrays.equals(v, store.get(v)))
    }
    vals.foreach(v => assertTrue(Arrays.equals(v, store.get(v))))
    vals.foreach(v => store.delete(v))
    vals.foreach(v => assertNull(store.get(v)))
  }

  @Test
  def testSerializedValueIsNull {
    val serializedStore = new SerializedKeyValueStore(
      store,
      new IntegerSerde,
      new IntegerSerde)

    serializedStore.putAll(List(new Entry[java.lang.Integer, java.lang.Integer](0, null)))
  }

  /**
   * This test specifically targets an issue in Scala 2.8.1's DoubleLinkedList
   * implementation. The issue is that it doesn't work. More specifically,
   * creating a DoubleLinkedList from an existing list does not update the
   * "prev" field of the existing list's head to point to the new head. As a
   * result, in Scala 2.8.1, every DoulbeLinkedList node's prev field is null.
   * Samza gets around this by manually updating the field itself. See SAMZA-80
   * for details.
   *
   * This issue is exposed in Samza's KV cache implementation, which uses
   * DoubleLinkedList, so all comments in this method are discussing the cached
   * implementation, but the test is still useful as a sanity check for
   * non-cached stores.
   */
  @Test
  def testBrokenScalaDoubleLinkedList() {
    val something = b("")
    val keys = letters
      .map(b(_))
      .toArray

    // Load the cache to capacity.
    letters
      .slice(0, TestKeyValueStores.CacheSize)
      .map(b(_))
      .foreach(store.put(_, something))

    // Now keep everything in the cache, but with an empty dirty list.
    store.flush

    // Dirty list is now empty, and every CacheEntry has dirty=null.

    // Corrupt the dirty list by creating two dirty lists that toggle back and 
    // forth depending on whether the last dirty write was to 1 or 0. The trick
    // here is that every element in the cache is treated as the "head" of the
    // DoulbeLinkedList (prev==null), even though it's not necessarily. Thus,
    // You can end up with multiple nodes each having their own version of the 
    // dirty list with different elements in them.
    store.put(keys(1), something)
    store.put(keys(0), something)
    store.put(keys(1), something)
    store.flush
    // The dirty list is now empty, but 0's dirty field actually has 0 and 1.
    store.put(keys(0), something)
    // The dirty list now has 0 and 1, but 1's dirty field is null in the 
    // cache because it was just flushed.

    // Get rid of 1 from the cache by reading every other element, and then 
    // putting one new element.
    letters
      .slice(2, TestKeyValueStores.CacheSize)
      .map(b(_))
      .foreach(store.get(_))
    store.put(keys(TestKeyValueStores.CacheSize), something)

    // Now try and trigger an NPE since the dirty list has an element (1) 
    // that's no longer in the cache.
    store.flush
  }
  
  @Test
  def testRandomReadWriteRemove() {
    // Make test deterministic by seeding the random number generator.
    val rand = new Random(12345)
    val keys = letters
      .map(b(_))
      .toArray
    var expected = Map[String, String]()

    (0 until 100).foreach(loop => {
      System.err.println("---")
      (0 until 30).foreach(i => {
        val idx = rand.nextInt(keys.length)
        val randomValue = letters(rand.nextInt(keys.length))
        val key = keys(idx)
        val currentVal = store.get(key)
        store.put(key, b(randomValue))
        System.err.println("put: " + letters(idx))
        expected += letters(idx) -> randomValue
      })

      for ((k, v) <- expected) {
        System.err.println("check: " + k + " = " + v)
        val bytes = store.get(b(k))
        assertNotNull(bytes)
        assertEquals(v, new String(bytes, "UTF-8"))
      }

      val iterator = store.all

      while (iterator.hasNext) {
        val key = iterator.next.getKey
        store.delete(key)
        expected -= new String(key, "UTF-8")
      }

      iterator.close

      assertEquals(0, expected.size)
    })
  }

  def checkRange(vals: IndexedSeq[String], iter: KeyValueIterator[Array[Byte], Array[Byte]]) {
    for (v <- vals) {
      assertTrue(iter.hasNext)
      val entry = iter.next()
      assertEquals(v, s(entry.getKey))
      assertEquals(v, s(entry.getValue))
    }
    assertFalse(iter.hasNext)
  }

  /**
   * Convert string to byte buffer
   */
  def b(s: String) =
    s.getBytes

  /**
   * Convert byte buffer to string
   */
  def s(b: Array[Byte]) =
    new String(b)

}

object TestKeyValueStores {
  val CacheSize = 10
  val BatchSize = 5
  @Parameters
  def parameters: java.util.Collection[Array[java.lang.Boolean]] = Arrays.asList(Array(java.lang.Boolean.TRUE), Array(java.lang.Boolean.FALSE))
}
