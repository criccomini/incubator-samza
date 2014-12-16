import os
import logging
from time import sleep
import zopkio.remote_host_helper as remote_host_helper
import zopkio.runtime as runtime

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
  expected_lines = 50
  has_fifty_lines = False
  attempts = 0
  # try and read the output of the samza job over and over again. keep going 
  # until we've tried 10 times, or we get the expected (expected_lines) line 
  # count.
  while not has_fifty_lines and attempts < 10:
    attempts += 1
    with remote_host_helper.get_ssh_client(kafka_hostname) as ssh:
      remote_host_helper.better_exec_command(ssh, "{0} --topic samza-test-topic-output --zookeeper {1}:2181 --from-beginning --consumer-timeout-ms 600 --max-messages {2} > /tmp/kafka_consumer_output.txt".format(kafka_consumer, zookeeper_hostname, expected_lines), "Failed to consume from kafka")
    with remote_host_helper.get_sftp_client(kafka_hostname) as ftp:
      ftp.get("/tmp/kafka_consumer_output.txt", "/tmp/validate_samza_job.out")
    # if the output has 50 lines in it, then break loop and validate, else sleep
    # and try again.
    if expected_lines == sum(1 for line in open('/tmp/validate_samza_job.out')):
      has_fifty_lines = True
    else:
      sleep(10)
  with open("/tmp/validate_samza_job.out") as fh:
    for i in range(expected_lines):
      line = fh.readline()
      assert int(line) < 0 , "Expected negative integer but received {0}".format(line)
