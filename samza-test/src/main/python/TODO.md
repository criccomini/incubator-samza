* come up with a better suite name than "single execution"
* make naraad work
* update yarn_instance_0 logs in perf.py to point to userlogs dir when https://github.com/linkedin/Zopkio/issues/17 is resolved.
* write proper multi-node yarn, kafka, and zk deployers to contribute back to zopkio 
  * eliminate yarn.sh and kafka.sh
  * download should happen once on driver machine, and should sftp/scp files to other boxes
* docs for simple-integration-test.py
* fix samza_executable to work with proper host for http:// server, not hard coded to localhost
* use a proper python setup.py install package instead of cp'ing the raw scripts
* remove http server from bin/integration-tests.sh and sftp copy the job tarball instead
* update test docs with integration test description
* license header for deployment.py et al
* directories in deployment.py teardown should not be hard coded
* pipe all output in deployers to a specific directory, and copy the entire directory to logs in perf.py
* store all temp files relative to a configurable sub directory (don't dump numbers.txt into /tmp hard coded)
* write util helper functions for kafka to read/write data
* use kafka python client rather than CLI for integration tests
* write util helper functions for yarn (await job, etc)
