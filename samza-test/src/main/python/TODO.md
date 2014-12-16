* make naraad work
* fix logs link
* write proper multi-node yarn, kafka, and zk deployers to contribute back to zopkio 
  * eliminate yarn.sh and kafka.sh
  * download should happen once on driver machine, and should sftp/scp files to other boxes
* docs for simple-integration-test.py
* fix samza_executable to work with proper host for http:// server, not hard coded to localhost
* use a proper python setup.py install package instead of cp'ing the raw scripts
* remove http server and sftp copy the job tarball
* update test docs with integration test description
* make log4j.xml work properly for negate numbers job (so we see logs in containers)
* report is not opened in chrome after integration test finishes
* license header for deployment.py et al
* directories in deployment.py teardown should not be hard coded
