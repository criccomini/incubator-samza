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

package org.apache.samza.util;

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.serializers.zk.ZkJsonSerde;

public class ZkUtil {
  public static void setupZkEnvironment(String zkConnect) {
    int chrootIdx = zkConnect.indexOf("/");
    String chroot = (chrootIdx > 0) ? zkConnect.substring(chrootIdx) : null;
    ZkClient zkClient = null;
    try {
      if (chroot != null) {
        String zkConnForChrootCreation = zkConnect.substring(0, chrootIdx);
        zkClient = new ZkClient(zkConnForChrootCreation, 6000, 6000, new ZkJsonSerde());
        zkClient.createPersistent(chroot, true);
      }
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }
}
