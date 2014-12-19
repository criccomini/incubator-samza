import os

# Location on driver machine where remote logs will be SCP'd to.
LOGS_DIRECTORY = '/tmp/samza-integration-test-logs/'
# Location on driver machine where other output will be SCP'd to (mostly Naarad output).
OUTPUT_DIRECTORY = '/tmp/samza-integration-test-output/'

def machine_logs():
  return {
    'zookeeper_instance_0': [
      '/tmp/samza-integration-test/deploy/zookeeper/zookeeper.out',
    ],
    'kafka_instance_0': [
      '/tmp/samza-integration-test/deploy/kafka/log-cleaner.log',
      '/tmp/samza-integration-test/deploy/kafka/logs/controller.log',
      '/tmp/samza-integration-test/deploy/kafka/logs/kafka-request.log',
      '/tmp/samza-integration-test/deploy/kafka/logs/kafka.log',
      '/tmp/samza-integration-test/deploy/kafka/logs/kafkaServer-gc.log',
      '/tmp/samza-integration-test/deploy/kafka/logs/server.log',
      '/tmp/samza-integration-test/deploy/kafka/logs/state-change.log',
    ],
    'yarn_rm_instance_0': [
      # TODO uncomment this when zopkio supports copying directories instead of files
      # '/tmp/samza-integration-test/deploy/yarn/logs/userlogs',
    ],
    'yarn_nm_instance_0': [
      # TODO uncomment this when zopkio supports copying directories instead of files
      # '/tmp/samza-integration-test/deploy/yarn/logs/userlogs',
    ],
  }

def naarad_logs():
  return {
    'zookeeper_instance_0': [],
    'kafka_instance_0': [],
    'samza_instance_0': [],
    'yarn_rm_instance_0': [],
    'yarn_nm_instance_0': [],
  }

def naarad_config(config, test_name=None):
  return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'naarad.cfg')
