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

package org.apache.samza.job.standalone;

import org.apache.samza.SamzaException;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.apache.samza.job.standalone.controller.StandaloneZkContainerController;
import org.apache.samza.job.standalone.controller.StandaloneZkCoordinatorController;

public class StandaloneJob implements StreamJob {
  private final StandaloneZkCoordinatorController coordinatorController;
  private final StandaloneZkContainerController containerController;

  public StandaloneJob(StandaloneZkCoordinatorController coordinatorController, StandaloneZkContainerController containerController) {
    this.coordinatorController = coordinatorController;
    this.containerController = containerController;
  }

  @Override
  public StreamJob submit() {
    this.coordinatorController.start();
    this.containerController.start();
    return this;
  }

  @Override
  public StreamJob kill() {
    try {
      this.containerController.stop();
    } catch (InterruptedException e) {
      throw new SamzaException(e);
    } finally {
      this.coordinatorController.stop();
    }
    return this;
  }

  @Override
  public ApplicationStatus waitForFinish(long timeoutMs) {
    ApplicationStatus status = this.containerController.getStatus();
    try {
      while (!(status.equals(ApplicationStatus.SuccessfulFinish) || status.equals(ApplicationStatus.UnsuccessfulFinish))) {
        Thread.sleep(100);
        status = this.containerController.getStatus();
      }
    } catch (Exception e) {
      // Break sleep loop, and reset interrupt flag.
      Thread.currentThread().interrupt();
    }
    return status;
  }

  @Override
  public ApplicationStatus waitForStatus(ApplicationStatus expectedStatus, long timeoutMs) {
    ApplicationStatus status = this.containerController.getStatus();
    long currentWaitTimeMs = 0;
    try {
      while (currentWaitTimeMs < timeoutMs && !status.equals(expectedStatus)) {
        Thread.sleep(100);
        currentWaitTimeMs += 100;
        status = this.containerController.getStatus();
      }
    } catch (Exception e) {
      // Break sleep loop, and reset interrupt flag.
      Thread.currentThread().interrupt();
    }
    return status;
  }

  @Override
  public ApplicationStatus getStatus() {
    return this.containerController.getStatus();
  }
}
