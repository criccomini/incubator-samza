import os
import logging
from time import sleep
import dtf.remote_host_helper as remote_host_helper
import dtf.runtime as runtime

logger = logging.getLogger(__name__)

CWD = os.path.dirname(os.path.abspath(__file__))
HOME_DIR = os.path.join(CWD, os.pardir)
DATA_DIR = os.path.join(HOME_DIR, "data")

def test_samza_job():
  """
  Tests if the Samza Job is reading Kafka input to produce the correct output
  """
  print "Running test_samza_job"
  kafka_hostname = runtime.get_active_config("kafka_hostname")
  kafka_home = os.path.join(runtime.get_active_config("kafka_install_path"), "kafka")
  kafka_bin = os.path.join(kafka_home, "bin")
  kafka_producer = os.path.join(kafka_bin, "kafka-console-producer.sh")
  with remote_host_helper.get_ssh_client(kafka_hostname) as ssh:
    with remote_host_helper.get_sftp_client(kafka_hostname) as ftp:
      ftp.put(os.path.join(DATA_DIR, "numbers.txt"), "/tmp/numbers.txt")
    remote_host_helper.better_exec_command(ssh, "{0} --topic samza-test-topic --broker-list {1}:9092 < /tmp/numbers.txt".format(kafka_producer, kafka_hostname), "Failed to write data to Kafka topic")
  sleep(60)

def validate_samza_job():
  """
  Validates that the Samza job produced the correct output
  """
  print "Running validate_samza_job"
  kafka_hostname = runtime.get_active_config("kafka_hostname")
  zookeeper_hostname = kafka_hostname
  kafka_home = os.path.join(runtime.get_active_config("kafka_install_path"), "kafka")
  kafka_bin = os.path.join(kafka_home, "bin")
  kafka_consumer = os.path.join(kafka_bin, "kafka-console-consumer.sh")
  with remote_host_helper.get_ssh_client(kafka_hostname) as ssh:
    remote_host_helper.better_exec_command(ssh, "{0} --topic samza-test-topic-output --zookeeper {1}:2181 --from-beginning --consumer-timeout-ms 600 --max-messages 50 > /tmp/kafka_consumer_output.txt".format(kafka_consumer, zookeeper_hostname), "Failed to consume from kafka")
  with remote_host_helper.get_sftp_client(kafka_hostname) as ftp:
    ftp.get("/tmp/kafka_consumer_output.txt", "/tmp/validate_samza_job.out")
  with open("/tmp/validate_samza_job.out") as fh:
    for i in range(50):
      line = fh.readline()
      assert int(line) < 0 , "Expected negative integer but received {0}".format(line)
