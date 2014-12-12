## Running integration tests

To start Samza's integration test in a specific directory, run:

  $ bin/integration-tests.sh <dir>

This command will:

# Build a test job tar.gz file, and put it in the test directory.
# Setup a [virtualenv](https://virtualenv.readthedocs.org/en/latest/).
# Install [Zopkio](https://github.com/linkedin/zopkio).
# Start an HTTP server (port 8000) to serve the tar.gz file to YARN.
# Run a simple integration test.
# Open a test report in the browser.

If the report does not automatically pop-up, it can be located at <INCUBATOR-SAMZA-MASTER>/samza-test/src/main/python/reports/*.

Logs are located in <dir>/logs. Test reports are located in <dir>/reports.
