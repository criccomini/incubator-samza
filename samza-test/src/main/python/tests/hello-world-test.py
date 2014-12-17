import os
import logging
from time import sleep
from kafka import KafkaClient, SimpleProducer
import zopkio.remote_host_helper as remote_host_helper
import zopkio.runtime as runtime

logger = logging.getLogger(__name__)

CWD = os.path.dirname(os.path.abspath(__file__))
HOME_DIR = os.path.join(CWD, os.pardir)
DATA_DIR = os.path.join(HOME_DIR, "data")
TEST_TOPIC = "samza-test-topic"

def test_samza_job():
  """
  Sends 50 messages (1 .. 50) to samza-test-topic.
  """
  logger.info("Running test_samza_job")
  kafka_hostname = runtime.get_active_config("kafka_hostname")
  kafka_hostport = runtime.get_active_config("kafka_hostport")
  kafka = KafkaClient("{0}:{1}".format(kafka_hostname, kafka_hostport))
  kafka.ensure_topic_exists(TEST_TOPIC)
  producer = SimpleProducer(kafka,
    async=False,
    req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT,
    ack_timeout=30000)
  for i in range(1, 51):
    producer.send_messages(TEST_TOPIC, str(i))
  kafka.close()

def validate_samza_job():
  """
  Validates that the Samza job produced the correct output
  """
  logger.info("Running validate_samza_job")
  kafka_hostname = runtime.get_active_config("kafka_hostname")
  zookeeper_hostname = kafka_hostname
  kafka_home = os.path.join(runtime.get_active_config("kafka_install_path"), "kafka")
  kafka_bin = os.path.join(kafka_home, "bin")
  kafka_consumer = os.path.join(kafka_bin, "kafka-console-consumer.sh")
  expected_lines = sum(1 for line in open(os.path.join(DATA_DIR, "numbers.txt")))
  has_fifty_lines = False
  attempts = 0
  line_count = 0
  assert expected_lines > 0, "Found an empty numbers.txt file. Can't run test without input."
  # try and read the output of the samza job over and over again. keep going 
  # until we've tried 10 times, or we get the expected (expected_lines) line 
  # count.
  while not has_fifty_lines and attempts < 10:
    attempts += 1
    with remote_host_helper.get_ssh_client(kafka_hostname) as ssh:
      remote_host_helper.better_exec_command(ssh, "{0} --topic samza-test-topic-output --zookeeper {1}:2181 --from-beginning --consumer-timeout-ms 600 --max-messages {2} > /tmp/kafka_consumer_output.txt".format(kafka_consumer, zookeeper_hostname, expected_lines), "Failed to consume from kafka")
    with remote_host_helper.get_sftp_client(kafka_hostname) as ftp:
      ftp.get("/tmp/kafka_consumer_output.txt", "/tmp/validate_samza_job.out")
    # if the output has expected lines in it, then break loop and validate, else
    # sleep and try again.
    line_count = sum(1 for line in open('/tmp/validate_samza_job.out'))
    if expected_lines <= line_count:
      has_fifty_lines = True
    else:
      logger.info("Unable to validate samza job output. Waiting and retrying.")
      sleep(10)
  with open("/tmp/validate_samza_job.out") as fh:
    for i in range(expected_lines):
      line = fh.readline()
      line_int = -1
      try:
        line_int = int(line)
      except:
        assert False, "Expected an integer in validation file, but got {0}".format(line)
      assert line_int < 0 , "Expected negative integer but received {0}".format(line)
  assert expected_lines <= line_count, "Expected {0} lines, but found {1}".format(expected_lines, line_count)
