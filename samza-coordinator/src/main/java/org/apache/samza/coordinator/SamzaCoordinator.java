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

package org.apache.samza.coordinator;

import org.apache.samza.coordinator.server.HttpServer;

public class SamzaCoordinator {
  private final SamzaCoordinatorScheduler scheduler;
  private final HttpServer server;

  public SamzaCoordinator(SamzaCoordinatorScheduler scheduler, HttpServer server) {
    this.scheduler = scheduler;
    this.server = server;
  }

  public void run() {
    try {
      startScheduler();
      startServer();
    } finally {
      shutdownServer();
      shutdownScheduler();
    }
  }

  public void startScheduler() {
    scheduler.start();
  }

  public void shutdownScheduler() {
    scheduler.stop();
  }

  public void startServer() {
    server.start();
  }

  public void shutdownServer() {
    server.stop();
  }
}
