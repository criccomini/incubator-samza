#!/bin/bash -e
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This script will download, setup, start, and stop servers for Kafka, YARN, and ZooKeeper,
# as well as downloading, building and locally publishing Samza

install() {
  echo "Installing Yarn"
  echo "Downloading YARN from $DOWNLOAD_URL"
  cd $BASE_DIR
  tarfile=$(basename $DOWNLOAD_URL)
  curl $DOWNLOAD_URL -o $tarfile
  mkdir -p tmp
  tar -xvf $tarfile -C tmp
  mv tmp/* yarn
  rm -rf tmp
}

start() {
  if [ -f $YARN_HOME_DIR/sbin/yarn-daemon.sh ]; then
    $YARN_HOME_DIR/sbin/yarn-daemon.sh start resourcemanager
    $YARN_HOME_DIR/sbin/yarn-daemon.sh start nodemanager
  else
    echo 'YARN is not installed. Run: bin/grid install yarn'
  fi
}


stop() {
  if [ -f $YARN_HOME_DIR/sbin/yarn-daemon.sh ]; then
    $YARN_HOME_DIR/sbin/yarn-daemon.sh stop resourcemanager
    $YARN_HOME_DIR/sbin/yarn-daemon.sh stop nodemanager
  else
    echo 'YARN is not installed. Run: bin/grid install yarn'
  fi
}

export JAVA_HOME=/export/apps/jdk/JDK-1_8_0_5

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

YARN_HOME_DIR=$BASE_DIR/yarn

COMMAND=$1
DOWNLOAD_URL=$2

if [ "$COMMAND" = "install" ]; then
 if [ -z "$DOWNLOAD_URL" ]; then
  echo "Usage: \t yarn install DOWNLOAD_URL \n\t yarn start"
 else
  install
 fi
elif [[ "$COMMAND" = "start" || "$COMMAND" = "stop" ]]; then
    "$COMMAND"
  else
    echo "Usage: \t yarn install DOWNLOAD_URL \n\t yarn start"
    exit
fi