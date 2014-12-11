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
installer() {
  SYSTEM=$1
  DOWNLOAD_URL=$2
  echo "Installing $SYSTEM"
  echo "Downloading $SYSTEM from $DOWNLOAD_URL"
  cd $BASE_DIR
  tarfile=$(basename $DOWNLOAD_URL)
  curl $DOWNLOAD_URL -o $tarfile
  mkdir -p tmp
  tar -xvf $tarfile -C tmp
  mv tmp/* $SYSTEM
  rm -rf tmp
}

install() {
  installer zookeeper $ZOOKEEPER_DOWNLOAD_URL
  cp "$ZOOKEEPER_HOME_DIR/conf/zoo_sample.cfg" "$ZOOKEEPER_HOME_DIR/conf/zoo.cfg"

  installer kafka $KAFKA_DOWNLOAD_URL
  # have to use SIGTERM since nohup on appears to ignore SIGINT
  # and Kafka switched to SIGINT in KAFKA-1031.
  sed -i.bak 's/SIGINT/SIGTERM/g' $KAFKA_HOME_DIR/bin/kafka-server-stop.sh
  # in order to simplify the wikipedia-stats example job, set topic to have just 1 partition by default
  sed -i.bak 's/^num\.partitions *=.*/num.partitions=1/' $KAFKA_HOME_DIR/config/server.properties
}

start_zookeeper() {
  if [ -f $ZOOKEEPER_HOME_DIR/bin/zkServer.sh ]; then
    cd $ZOOKEEPER_HOME_DIR
    bin/zkServer.sh start
    cd - > /dev/null
  else
    echo 'Zookeeper is not installed. Run: kafka.sh'
  fi
}

start_kafka() {
  if [ -f $KAFKA_HOME_DIR/bin/kafka-server-start.sh ]; then
    mkdir -p $KAFKA_HOME_DIR/logs
    cd $KAFKA_HOME_DIR
    nohup bin/kafka-server-start.sh config/server.properties > logs/kafka.log 2>&1 &
    cd - > /dev/null
  else
    echo 'Kafka is not installed. Run: kafka.sh'
  fi
}

start() {
  start_zookeeper
  start_kafka
}

stop_kafka() {
  if [ -f $KAFKA_HOME_DIR/bin/kafka-server-stop.sh ]; then
    cd $KAFKA_HOME_DIR
    bin/kafka-server-stop.sh || true # tolerate nonzero exit status if Kafka isn't running
    cd - > /dev/null
  else
    echo 'Kafka is not installed. Run: kafka.sh'
  fi
}

stop_zookeeper() {
  if [ -f $ZOOKEEPER_HOME_DIR/bin/zkServer.sh ]; then
    cd $ZOOKEEPER_HOME_DIR
    bin/zkServer.sh stop
    cd - > /dev/null
  else
    echo 'Zookeeper is not installed. Run: kafka.sh'
  fi
}

stop() {
  stop_kafka
  stop_zookeeper
}

export JAVA_HOME=/export/apps/jdk/JDK-1_8_0_5

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ZOOKEEPER_HOME_DIR=$BASE_DIR/zookeeper
KAFKA_HOME_DIR=$BASE_DIR/kafka

COMMAND=$1
ZOOKEEPER_DOWNLOAD_URL=$2
KAFKA_DOWNLOAD_URL=$3

if [ "$COMMAND" = "install" ]; then
 if [[ -z "$ZOOKEEPER_DOWNLOAD_URL" || -z "$KAFKA_DOWNLOAD_URL" ]]; then
  echo "Usage: \t kafka.sh install ZOOKEEPER_DOWNLOAD_URL KAFKA_DOWNLOAD_URL\n\t kafka.sh start \n\t kafka.sh stop"
 else
  install
 fi
elif [[ "$COMMAND" = "start" || "$COMMAND" = "stop" ]]; then
    "$COMMAND"
  else
    echo "Usage: \t kafka.sh install ZOOKEEPER_DOWNLOAD_URL KAFKA_DOWNLOAD_URL\n\t kafka.sh start \n\t kafka.sh stop"
    exit
fi