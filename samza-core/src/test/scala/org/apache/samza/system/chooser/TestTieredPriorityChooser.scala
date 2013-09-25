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

package org.apache.samza.system.chooser

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.system.IncomingMessageEnvelope
import scala.collection.immutable.Queue
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.Partition

class TestTieredPriorityChooser {
  val envelope1 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), null, null, 1);
  val envelope2 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(1)), null, null, 2);
  val envelope3 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream1", new Partition(0)), null, null, 3);
  val envelope4 = new IncomingMessageEnvelope(new SystemStreamPartition("kafka", "stream", new Partition(0)), "123", null, 4);

  @Test
  def testChooserWithSingleStream {
    val mock = new MockMessageChooser
    val chooser = new TieredPriorityChooser(
      Map(envelope1.getSystemStreamPartition.getSystemStream -> 0),
      Map(0 -> mock))

    chooser.register(envelope1.getSystemStreamPartition, null)
    assertEquals(null, chooser.choose)

    chooser.update(envelope1)
    chooser.update(envelope4)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope4, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope4)
    chooser.update(envelope1)
    assertEquals(envelope4, chooser.choose)
    assertEquals(envelope1, chooser.choose)
    assertEquals(null, chooser.choose)
  }

  @Test
  def testChooserWithSingleStreamWithTwoPartitions {
    val mock = new MockMessageChooser
    val chooser = new TieredPriorityChooser(
      Map(envelope2.getSystemStreamPartition.getSystemStream -> 0),
      Map(0 -> mock))

    chooser.register(envelope2.getSystemStreamPartition, null)
    chooser.register(envelope3.getSystemStreamPartition, null)
    assertEquals(null, chooser.choose)

    chooser.update(envelope2)
    chooser.update(envelope3)
    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope3, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope3)
    chooser.update(envelope2)
    assertEquals(envelope3, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)
  }

  @Test
  def testChooserWithTwoStreamsOfEqualPriority {
    val mock = new MockMessageChooser
    val chooser = new TieredPriorityChooser(
      Map(
        envelope1.getSystemStreamPartition.getSystemStream -> 0,
        envelope2.getSystemStreamPartition.getSystemStream -> 0),
      Map(0 -> mock))

    chooser.register(envelope1.getSystemStreamPartition, null)
    chooser.register(envelope2.getSystemStreamPartition, null)
    assertEquals(null, chooser.choose)

    chooser.update(envelope1)
    chooser.update(envelope4)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope4, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope4)
    chooser.update(envelope1)
    assertEquals(envelope4, chooser.choose)
    assertEquals(envelope1, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope2)
    chooser.update(envelope4)
    assertEquals(envelope2, chooser.choose)
    assertEquals(envelope4, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope1)
    chooser.update(envelope2)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)
  }

  @Test
  def testChooserWithTwoStreamsOfDifferentPriority {
    val mock0 = new MockMessageChooser
    val mock1 = new MockMessageChooser
    val chooser = new TieredPriorityChooser(
      Map(
        envelope1.getSystemStreamPartition.getSystemStream -> 1,
        envelope2.getSystemStreamPartition.getSystemStream -> 0),
      Map(
        0 -> mock0,
        1 -> mock1))

    chooser.register(envelope1.getSystemStreamPartition, null)
    chooser.register(envelope2.getSystemStreamPartition, null)
    assertEquals(null, chooser.choose)

    chooser.update(envelope1)
    chooser.update(envelope4)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope4, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope4)
    chooser.update(envelope1)
    assertEquals(envelope4, chooser.choose)
    assertEquals(envelope1, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope2)
    chooser.update(envelope4)
    // Reversed here because envelope4.SSP=envelope1.SSP which is higher 
    // priority.
    assertEquals(envelope4, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)

    chooser.update(envelope1)
    chooser.update(envelope2)
    assertEquals(envelope1, chooser.choose)
    assertEquals(envelope2, chooser.choose)
    assertEquals(null, chooser.choose)
  }
}
