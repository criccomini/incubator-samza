import os
import logging

import zopkio.runtime as runtime
import yarn_deployer as yd
import samza_on_yarn_deployer as syd
import kafka_deployer as kd

logger = logging.getLogger(__name__)
yarn_deployer = None
samza_deployer = None
kafka_deployer = None

def setup_suite():
  CWD = os.path.dirname(os.path.abspath(__file__))
  logger.info("Current workding directory {0}".format(CWD))

  ##################
  #  Install YARN  #
  ##################
  yarn_executable = runtime.get_active_config("yarn_executable")
  global yarn_deployer
  yarn_deployer = yd.YarnDeployer("yarn", {
    "pid_keyword": "yarn",
    "executable": yarn_executable,
    "control_script": runtime.get_active_config("yarn_control_script")
    })
  logger.info("Installing YARN")
  yarn_deployer.install("yarn_instance_0", {
    "hostname": runtime.get_active_config("yarn_hostname"),
    "install_path": runtime.get_active_config("yarn_install_path"),
    "yarn-site": runtime.get_active_config("yarn_yarn-site"),
    "install_commands": ["install {0}".format(yarn_executable)]
  })
  runtime.set_deployer("yarn", yarn_deployer)
  #############################
  # Install Kafka / Zookeeper #
  #############################
  kafka_hostname = runtime.get_active_config("kafka_hostname")
  global kafka_deployer
  kafka_deployer = kd.KafkaDeployer("kafka", {
    "pid_keyword": "kafka_job_0",
    "hostname": kafka_hostname
  })
  logger.info("Installing Kafka")
  zookeeper_executable = runtime.get_active_config("kafka_zookeeper_executable")
  kafka_executable = runtime.get_active_config("kafka_executable")
  kafka_deployer.install("kafka_instance_0", {
    "install_path": runtime.get_active_config("kafka_install_path"),
    "install_commands": ["install {0} {1}".format(zookeeper_executable, kafka_executable)],
    "control_script": runtime.get_active_config("kafka_control_script")
  })
  runtime.set_deployer("kafka", kafka_deployer)
  #####################
  # Install Samza Job #
  #####################
  yarn_process = yarn_deployer.get_process("yarn_instance_0")
  global samza_deployer
  samza_deployer = syd.SamzaOnYarnDeployer("samza", {
    "pid_keyword": "samza_job_0",
    "yarn_host": yarn_process.hostname,
    "yarn_port": yarn_process.port,
    "yarn_home": yarn_process.get_yarn_home()
  })
  logger.info("Installing Samza Job")
  samza_deployer.install("samza_instance_0", {
    "hostname": runtime.get_active_config("samza_hostname"),
    "executable": runtime.get_active_config("samza_executable"),
    "install_path": runtime.get_active_config("samza_install_path")
  })
  runtime.set_deployer("samza", samza_deployer)
  ###############
  # Start Kafka #
  ###############
  logger.info("Starting Kafka")
  kafka_deployer.start("kafka_instance_0", {
    "start_command": "start"
  })

  ###############
  # Start YARN  #
  ###############
  logger.info("Starting YARN")
  yarn_deployer.start("yarn_instance_0", {
    "start_command": "start"
  })

  ###################
  # Start Samza Job #
  ###################
  logger.info("Starting Samza Job")
  properties = []
  for prop_key,prop_value in runtime.get_active_config("samza_properties").iteritems():
    properties.append((prop_key, prop_value))
  samza_deployer.start("samza_instance_0", {
    "config-factory": runtime.get_active_config("samza_config-factory"),
    "config-file": runtime.get_active_config("samza_config-file"),
    "properties": properties
  })
  logger.info("Ready to run tests")

def setup():
  pass

def teardown():
  pass

def teardown_suite():
  logger.info("Stopping Samza Job")
  samza_deployer.stop("samza_instance_0", {})
  samza_deployer.uninstall("samza_instance_0", {})

  logger.info("Stopping Kafka")
  kafka_deployer.stop("kafka_instance_0", {
    'stop_command': 'stop'
  })
  kafka_process = kafka_deployer.get_process("kafka_instance_0")
  kafka_deployer.uninstall("kafka_instance_0", {
    'additional_directories': [kafka_process.zookeeper_home, "/tmp/kafka-logs", "/tmp/zookeeper"]
  })

  logger.info("Stopping YARN")
  yarn_deployer.stop("yarn_instance_0", {
    "stop_command": "stop"
  })
  yarn_deployer.uninstall("yarn_instance_0", {
    "additional_directories": ["/tmp/samza-integration-test"]
  })

