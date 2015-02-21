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

package org.apache.samza.job.standalone.controller;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.zk.ZkJsonSerde;
import org.apache.samza.system.mock.MockSystemFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestStandaloneZkCoordinatorController {
  public static final String zkConnect = "127.0.0.1:2181";
  StandaloneZkCoordinatorController controller = null;
  EmbeddedZookeeper zookeeper = null;
  ZkClient zkClient = null;

  @BeforeClass
  public void before() {
    zookeeper = new EmbeddedZookeeper(zkConnect);
    zkClient = new ZkClient(zkConnect + "/", 6000, 6000, new ZkJsonSerde());
    Map<String, String> configMap = new HashMap<String, String>();
    configMap.put("task.inputs", "mock.foo");
    configMap.put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName());
    Config config = new MapConfig(configMap);
    controller = new StandaloneZkCoordinatorController(config, zkClient);
  }

  @Test
  public void test() throws InterruptedException {
    System.err.println("Starting controller.");
    controller.start();
    Thread.sleep(5000);

    System.err.println("Creating container.");
    String c1EphemeralNode = zkClient.createEphemeralSequential(StandaloneZkCoordinatorController.CONTAINER_PATH + "/", Collections.emptyList());
    String c1ContainerSequentialId = new File(c1EphemeralNode).getName();
    Thread.sleep(5000);
    Map<String, Object> assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
    assertTrue(assignments.containsKey(c1ContainerSequentialId));
    assertEquals(2, assignments.size());
    assertEquals(0, assignments.get(c1ContainerSequentialId));
    System.err.println("Assignments: " + assignments);

    List<String> taskNames = new ArrayList<String>();
    taskNames.add("Partition 0");
    taskNames.add("Partition 1");
    taskNames.add("Partition 2");
    taskNames.add("Partition 3");
    System.err.println("Claiming assignment for container (0): " + taskNames);
    zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + c1ContainerSequentialId, taskNames);
    Thread.sleep(5000);

    System.err.println("Creating second container.");
    String c2EphemeralNode = zkClient.createEphemeralSequential(StandaloneZkCoordinatorController.CONTAINER_PATH + "/", Collections.emptyList());
    String c2ContainerSequentialId = new File(c2EphemeralNode).getName();
    Thread.sleep(5000);
    assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
    assertEquals(0, assignments.size());
    System.err.println("Assignments: " + assignments);

    System.err.println("Releasing assignment for container (0): " + taskNames);
    zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + c1ContainerSequentialId, Collections.emptyList());
    Thread.sleep(5000);
    assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
    assertTrue(assignments.containsKey(c1ContainerSequentialId));
    assertTrue(assignments.containsKey(c2ContainerSequentialId));
    assertEquals(3, assignments.size());
    assertEquals(1, assignments.get(c1ContainerSequentialId));
    assertEquals(0, assignments.get(c2ContainerSequentialId));
    System.err.println("Assignments: " + assignments);

    taskNames = new ArrayList<String>();
    taskNames.add("Partition 1");
    taskNames.add("Partition 3");
    System.err.println("Claiming assignment for container (0): " + taskNames);
    zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + c1ContainerSequentialId, taskNames);
    Thread.sleep(5000);
    assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
    System.err.println("Assignments: " + assignments);

    taskNames = new ArrayList<String>();
    taskNames.add("Partition 0");
    taskNames.add("Partition 2");
    System.err.println("Claiming assignment for container (1): " + taskNames);
    zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + c2ContainerSequentialId, taskNames);
    Thread.sleep(5000);
    assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
    System.err.println("Assignments: " + assignments);

    System.err.println("Deleting first container.");
    zkClient.delete(c1EphemeralNode);
    Thread.sleep(5000);
    assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
    assertEquals(0, assignments.size());
    System.err.println("Assignments: " + assignments);

    System.err.println("Releasing assignment for container (1): " + taskNames);
    zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + c2ContainerSequentialId, Collections.emptyList());
    Thread.sleep(5000);
    assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
    assertTrue(assignments.containsKey(c2ContainerSequentialId));
    assertEquals(2, assignments.size());
    assertEquals(0, assignments.get(c2ContainerSequentialId));
    System.err.println("Assignments: " + assignments);

    taskNames = new ArrayList<String>();
    taskNames.add("Partition 0");
    taskNames.add("Partition 1");
    taskNames.add("Partition 2");
    taskNames.add("Partition 3");
    System.err.println("Claiming assignment for container (1): " + taskNames);
    zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + c2ContainerSequentialId, taskNames);
    Thread.sleep(5000);

    System.err.println("Deleting second container.");
    zkClient.delete(c2EphemeralNode);
    Thread.sleep(5000);
    assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
    assertEquals(0, assignments.size());
    System.err.println("Assignments: " + assignments);
  }

  @AfterClass
  public void after() {
    if (controller != null) {
      controller.stop();
    }
    if (zkClient != null) {
      zkClient.close();
    }
    if (zookeeper != null) {
      zookeeper.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    TestStandaloneZkCoordinatorController test = new TestStandaloneZkCoordinatorController();
    test.before();
    test.test();
    test.after();
  }
}
