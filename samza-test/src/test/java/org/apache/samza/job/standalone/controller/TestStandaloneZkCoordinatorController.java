package org.apache.samza.job.standalone.controller;

import java.util.Collections;
import java.util.HashMap;
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
      System.err.println("Creating second container.");
      String c2EphemeralNode = zkClient.createEphemeralSequential(StandaloneZkCoordinatorController.CONTAINER_PATH + "/", Collections.emptyList());
      Thread.sleep(5000);
      assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
      System.err.println("Assignments: " + assignments);
      Thread.sleep(5000);
      System.err.println("Deleting first container.");
      zkClient.delete(c1EphemeralNode);
      Thread.sleep(5000);
      assignments = zkClient.readData(StandaloneZkCoordinatorController.ASSIGNMENTS_PATH);
      System.err.println("Assignments: " + assignments);
      System.err.println("Deleting second container.");
      zkClient.delete(c1EphemeralNode);
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
