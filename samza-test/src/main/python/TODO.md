* license header for deployment.py et al
* make naraad work
* update yarn_instance_0 logs in perf.py to point to userlogs dir when https://github.com/linkedin/Zopkio/issues/17 is resolved.
* write proper multi-node yarn, kafka, and zk deployers to contribute back to zopkio 
  * eliminate yarn.sh and kafka.sh
  * download should happen once on driver machine, and should sftp/scp files to other boxes
* docs for simple-integration-test.py
* use a proper python setup.py install package instead of cp'ing the raw scripts
* update test docs with integration test description
* directories in deployment.py teardown should not be hard coded
* pipe all output in deployers to a specific directory, and copy the entire directory to logs in perf.py
* store all temp files relative to a configurable sub directory (don't dump numbers.txt into /tmp hard coded)
* write util helper functions for kafka to read/write data
* write util helper functions for yarn (await job, etc)
* convert msg payload to JSON instead of string
* convert __get_kafka_client method in smoke tests to use a kafka context manager
* add usr/bin/python to all python files
