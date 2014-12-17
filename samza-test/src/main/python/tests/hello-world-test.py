import os
import logging
from time import sleep
from kafka import KafkaClient, SimpleProducer, SimpleConsumer
import zopkio.remote_host_helper as remote_host_helper
import zopkio.runtime as runtime

logger = logging.getLogger(__name__)

CWD = os.path.dirname(os.path.abspath(__file__))
HOME_DIR = os.path.join(CWD, os.pardir)
DATA_DIR = os.path.join(HOME_DIR, "data")
TEST_TOPIC = "samza-test-topic"
TEST_OUTPUT_TOPIC = "samza-test-topic-output"
NUM_MESSAGES = 50

def test_samza_job():
  """
  Sends 50 messages (1 .. 50) to samza-test-topic.
  """
  logger.info("Running test_samza_job")
  kafka = __get_kafka_client()
  kafka.ensure_topic_exists(TEST_TOPIC)
  producer = SimpleProducer(kafka,
    async=False,
    req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT,
    ack_timeout=30000)
  for i in range(1, NUM_MESSAGES + 1):
    producer.send_messages(TEST_TOPIC, str(i))
  kafka.close()

def validate_samza_job():
  """
  Validates that negate-number negated all messages, and sent the output to 
  samza-test-topic-output.
  """
  logger.info("Running validate_samza_job")
  kafka = __get_kafka_client()
  kafka.ensure_topic_exists(TEST_OUTPUT_TOPIC)
  consumer = SimpleConsumer(kafka, "samza-test-group", TEST_OUTPUT_TOPIC)
  messages = consumer.get_messages(count=NUM_MESSAGES, block=True, timeout=60)
  message_count = len(messages)
  assert NUM_MESSAGES == message_count, "Expected {0} lines, but found {1}".format(NUM_MESSAGES, message_count)
  for message in map(lambda m: m.message.value, messages):
    assert int(message) < 0 , "Expected negative integer but received {0}".format(message)

def __get_kafka_client():
  kafka_hostname = runtime.get_active_config("kafka_hostname")
  kafka_hostport = runtime.get_active_config("kafka_hostport")
  return KafkaClient("{0}:{1}".format(kafka_hostname, kafka_hostport))

