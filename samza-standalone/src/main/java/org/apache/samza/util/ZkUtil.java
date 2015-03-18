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

import java.util.Collections;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.samza.serializers.zk.ZkJsonSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkUtil {
  private static final Logger log = LoggerFactory.getLogger(ZkUtil.class);

  public static void setupZkEnvironment(String zkConnect) {
    log.debug("Setting up ZK environment for {}.", zkConnect);
    int chrootIdx = zkConnect.indexOf("/");
    String chroot = (chrootIdx > 0) ? zkConnect.substring(chrootIdx) : null;
    ZkClient zkClient = null;
    try {
      if (chroot != null) {
        String zkConnForChrootCreation = zkConnect.substring(0, chrootIdx);
        zkClient = new ZkClient(zkConnForChrootCreation, 6000, 6000, new ZkJsonSerde());
        log.info("Creating '{}' with base '{}'.", chroot, zkConnForChrootCreation);
        zkClient.createPersistent(chroot, true);
      }
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }

  public static String getLeader(List<String> children) {
    String coordinatorChild = null;
    if (children != null && children.size() > 0) {
      Collections.sort(children);
      coordinatorChild = children.get(0);
    }
    return coordinatorChild;
  }
}
