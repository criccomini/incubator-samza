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

package org.apache.samza.system;

/**
 * A mix-in interface for SystemAdmins that can handle the creation of
 * coordinator streams. If a system is used as a coordinator system, its
 * SystemAdmin must implement this interface.
 */
public interface CoordinatorSystemAdmin extends SystemAdmin {
  /**
   * Create a stream for the job coordinator. If the stream already exists, this
   * call should simply return.
   * 
   * @param streamName
   *          The name of the coordinator stream to create.
   */
  void createCoordinatorStream(String streamName);
}
