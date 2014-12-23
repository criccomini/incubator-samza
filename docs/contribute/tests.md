---
layout: page
title: Tests
---
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

Samza's unit tests are written on top of [JUnit](http://junit.org/), and license checking is done with [Apache Rat](http://creadur.apache.org/rat/). Samza's integration tests are written on top of [Zopkio](https://github.com/linkedin/Zopkio).

### Running Unit Tests Locally

To run all tests, and license checks:

    ./gradlew clean check

To run a single test:

    ./gradlew clean :samza-core:test -Dtest.single=TestSamzaContainer

Test results are located in:

    <module name>/build/reports/tests/index.html

#### Testing Scala and Java

Samza's unit tests can also be run against all supported permutations of Scala and Java. 

To run the tests against a specific combination:

    ./gradlew -PscalaVersion=2.10 -PyarnVersion=2.4.0 clean check

To run Samza's unit tests against all permutations, run:

    bin/check-all.sh

When run on Linux, this command requires you to set three environment variables:

    JAVA6_HOME is not set.
    JAVA7_HOME is not set.
    JAVA8_HOME is not set.

On Mac, check-all.sh will default to the appropriate path for each environment variable if it's not already set:

    JAVA6_HOME is not set.
    JAVA6_HOME defaulted to /System/Library/Java/JavaVirtualMachines/1.6.0.jdk/Contents/Home
    JAVA7_HOME is not set.
    JAVA7_HOME defaulted to /Library/Java/JavaVirtualMachines/jdk1.7.0_67.jdk/Contents/Home
    JAVA8_HOME is not set.
    JAVA8_HOME defaulted to /Library/Java/JavaVirtualMachines/jdk1.8.0_20.jdk/Contents/Home

### Travis CI

[Travis CI](https://travis-ci.org/apache/incubator-samza) has been configured to run Samza's unit tests after every commit to Samza's [master branch](https://git-wip-us.apache.org/repos/asf?p=incubator-samza.git;a=tree). The test results are mailed to the [developer mailing list](/community/mailing-lists.html), and posted in the [IRC channel](/community/irc.html).

[![Build Status](https://travis-ci.org/apache/incubator-samza.svg?branch=master)](https://travis-ci.org/apache/incubator-samza)

### Running Integration Tests Locally

Samza uses [Zopkio](https://github.com/linkedin/Zopkio) to deploy and execute its integration tests. Integration tests can be executed by running:

    ./bin/integration-tests.sh /tmp/samza-tests

The parameter defines where the integration tests should install packages locally. Executing this command will:

1. Build a samza-test job tarball.
2. Download and install YARN, Kafka, and ZooKeeper.
3. Deploy the samza-test job tarball to all YARN NM machines.
4. Start all Samza integration test jobs.
5. Feed input data to the jobs, and verify the results.
6. Open a report, and aggregate all remote logs.

The default configurations that ship with Samza deploy all software, and run all tests locally on the machine from which the `integration-tests.sh` command was executed.