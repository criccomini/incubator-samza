* update test docs with integration test description

* make naraad work
* add usr/bin/python to all python files
* update yarn_instance_0 logs in perf.py to point to userlogs dir when https://github.com/linkedin/Zopkio/issues/17 is resolved.
* use a proper python setup.py install package instead of cp'ing the raw scripts
* write util helper functions for kafka to read/write data
* write util helper functions for yarn (await job, etc)
* convert msg payload to JSON instead of string
* convert _get_kafka_client method in smoke tests to use a kafka context manager
* remove all TODOs from python source
