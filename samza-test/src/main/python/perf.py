import os

# Location on driver machine where remote logs will be SCP'd to.
LOGS_DIRECTORY = "/tmp/samza-integration-test-logs/"
# Location on driver machine where other output will be SCP'd to (mostly Naarad output).
OUTPUT_DIRECTORY = "/tmp/samza-integration-test-output/"

def machine_logs():
  return {
    "kafka_instance_0": [
      "/tmp/samza-integration-test/deploy/zookeeper/zookeeper.out"
      "/tmp/samza-integration-test/deploy/kafka/log-cleaner.log",
      "/tmp/samza-integration-test/deploy/kafka/logs/controller.log",
      "/tmp/samza-integration-test/deploy/kafka/logs/kafka-request.log",
      "/tmp/samza-integration-test/deploy/kafka/logs/kafka.log",
      "/tmp/samza-integration-test/deploy/kafka/logs/kafkaServer-gc.log",
      "/tmp/samza-integration-test/deploy/kafka/logs/server.log",
      "/tmp/samza-integration-test/deploy/kafka/logs/state-change.log"
    ],
    "samza_job_0": [
      "/tmp/job-runner-output.txt", 
      "/tmp/job-runner-error.txt"
    ],
    "yarn_instance_0": ["/tmp/samza-integration-test/deploy/yarn/logs/userlogs"]
  }

def naarad_logs():
  return {}

def naarad_config(config, test_name=None):
  return os.path.join(os.path.dirname(os.path.abspath(__file__)), "naarad.cfg")
