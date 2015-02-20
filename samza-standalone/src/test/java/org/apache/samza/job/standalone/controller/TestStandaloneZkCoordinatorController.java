package org.apache.samza.job.standalone.controller;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import kafka.zk.EmbeddedZookeeper;

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.zk.ZkJsonSerde;

public class TestStandaloneZkCoordinatorController {
  public static void main(String[] args) throws Exception {
    String zkConnect = "127.0.0.1:2181";
    EmbeddedZookeeper zookeeper = new EmbeddedZookeeper(zkConnect);
    ZkClient zkClient = new ZkClient(zkConnect + "/", 6000, 6000, new ZkJsonSerde());
    try {
      Map<String, String> map = new HashMap<String, String>();
      Config config = new MapConfig(map);
      StandaloneZkCoordinatorController controller = new StandaloneZkCoordinatorController(config, zkClient);
      controller.start();
      System.err.println("sleeping");
      Thread.sleep(5000);
      System.err.println("creating container");
      zkClient.createEphemeralSequential(StandaloneZkCoordinatorController.CONTAINER_PATH + "/", Collections.emptyList());
      Thread.sleep(5000);
    } finally {
      zkClient.close();
      zookeeper.shutdown();
    }
  }
}
