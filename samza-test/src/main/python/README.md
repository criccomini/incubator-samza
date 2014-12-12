## Running integration tests

To start Samza's integration test, run:

  $ bin/integration-tests.sh <directory to run tests in>

This will setup a [virtualenv](https://virtualenv.readthedocs.org/en/latest/), in the directory.

2. You need to place your test job tar ball in a remotely accessible server.
If you want to quickly spin up a local server, you can do the following:
 a. mkdir <HTTP_SERVER_HOME>
 b. cd <HTTP_SERVER_HOME>
 c. python -m SimpleHTTPServer

This will start a simple HTTP Server on port 8000 on your local machine.
Files in the HTTP_SERVER_HOME can be accessed using the HTTP GET as follows:
curl "http://localhost:8000/<FILE_PATH_ON_SERVER>"

3. Generate the Tarball for Test Samza Jobs located in :samza-test-jobs
  cd <INCUBATOR-SAMZA-MASTER>
  ./gradlew clean :samza-test-jobs:releaseTestJobs

  cp <INCUBATOR-SAMZA-MASTER>/build/distributions/samza-test-jobs-*.tgz <HTTP_SERVER_HOME>/

4. Start Integration Test using DTF command
  cd <INCUBATOR-SAMZA-MASTER>/samza-test/src/main/python
  dtf simple-integration-test.py

This will setup the test environment for the test suite and run through the tests.
Once the tests have completed, it will open up the test report in the browser.
If the report does not automatically pop-up, it can be located at <INCUBATOR-SAMZA-MASTER>/samza-test/src/main/python/reports/*

