package org.apache.samza.coordinator.server;

import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.SamzaCoordinatorContainerManager;

public class SamzaCoordinatorServer extends HttpServer {
  private Config config;
  private Map<TaskName, Integer> taskPartitionMapping;
  
  public SamzaCoordinatorServer(SamzaJobModel containerManager) {
  }

  public void register(Config config, Map<TaskName, Integer> taskPartitionMapping) {
    this.config = config;
    this.taskPartitionMapping = taskPartitionMapping;
  }

  @Override
  public void start() {
    super.addServlet("/config/*", new ServletConfig(config));
    super.addServlet("/tasks/*", new ServletTaskMapping(taskPartitionMapping));
    super.start();
  }
}
