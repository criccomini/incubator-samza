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

public class TestStandaloneZkCoordinatorController {
  public static void main(String[] args) throws Exception {
    StandaloneZkCoordinatorController controller = null;
    String zkConnect = "127.0.0.1:2181";
    EmbeddedZookeeper zookeeper = new EmbeddedZookeeper(zkConnect);
    ZkClient zkClient = new ZkClient(zkConnect + "/", 6000, 6000, new ZkJsonSerde());
    try {
      Map<String, String> configMap = new HashMap<String, String>();
      configMap.put("task.inputs", "mock.foo");
      configMap.put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName());
      Config config = new MapConfig(configMap);
      controller = new StandaloneZkCoordinatorController(config, zkClient);
      System.err.println("Starting controller.");
      controller.start();
      Thread.sleep(5000);

      System.err.println("Creating container.");
      String c1EphemeralNode = zkClient.createEphemeralSequential(StandaloneZkCoordinatorController.CONTAINER_PATH + "/", Collections.emptyList());
      Thread.sleep(5000);
      Map<String, Object> assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
      System.err.println("Assignments: " + assignments);

      String c1ContainerSequentialId = new File(c1EphemeralNode).getName();
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
      Thread.sleep(5000);
      assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
      System.err.println("Assignments: " + assignments);

      System.err.println("Releasing assignment for container (0): " + taskNames);
      zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + c1ContainerSequentialId, Collections.emptyList());
      Thread.sleep(5000);
      assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
      System.err.println("Assignments: " + assignments);

      taskNames = new ArrayList<String>();
      taskNames.add("Partition 1");
      taskNames.add("Partition 3");
      System.err.println("Claiming assignment for container (0): " + taskNames);
      zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + c1ContainerSequentialId, taskNames);
      Thread.sleep(5000);
      assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
      System.err.println("Assignments: " + assignments);

      String c2ContainerSequentialId = new File(c2EphemeralNode).getName();
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
      System.err.println("Assignments: " + assignments);

      System.err.println("Releasing assignment for container (1): " + taskNames);
      zkClient.writeData(StandaloneZkCoordinatorController.CONTAINER_PATH + "/" + c2ContainerSequentialId, Collections.emptyList());
      Thread.sleep(5000);
      assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
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
      System.err.println("Assignments: " + assignments);
    } finally {
      if (controller != null) {
        controller.stop();
      }
      zkClient.close();
      zookeeper.shutdown();
    }
  }
}
