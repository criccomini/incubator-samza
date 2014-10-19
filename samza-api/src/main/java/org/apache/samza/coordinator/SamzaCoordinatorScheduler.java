package org.apache.samza.coordinator;

import java.net.URI;

public interface SamzaCoordinatorScheduler {
  public void register(URI serverUri, int containerCount);

  public void start();

  public void stop();

  public void awaitShutdown();
}
