import os
import logging
import shutil
import urllib
import zopkio.runtime as runtime
import zopkio.adhoc_deployer as adhoc_deployer
import samza_on_yarn_deployer as syd
import kafka_deployer as kd
from zopkio.runtime import get_active_config as c

logger = logging.getLogger(__name__)
samza_deployer = None
kafka_deployer = None
ssh_deployer = None

def _download_packages():
  for url_key in ["url_hadoop", "url_kafka", "url_zookeeper"]:
    logger.debug("Getting download URL for: {0}".format(url_key))
    url = c(url_key)
    filename = os.path.basename(url)
    if os.path.exists(filename):
      logger.debug("Using cached file: {0}".format(filename))
    else:
      logger.info("Downloading: {0}".format(url))
      urllib.urlretrieve(url, filename)

def _install_yarn(deployer, unique_id):
  logger.debug("Installing YARN.")
  deployer.install(unique_id, {
    'hostname': c("yarn_rm_hostname"),
    'install_path': os.path.join(c("remote_install_path"), c("yarn_install_path")),
    'executable': c("yarn_executable"),
  })

def _start_yarn_rm(deployer, unique_id):
  _start_yarn(deployer, unique_id, "rm")

def _start_yarn_nm(deployer, unique_id):
  _start_yarn(deployer, unique_id, "nm")

def _start_yarn(deployer, unique_id, type="nm"):
  logger.debug("Starting YARN.")
  deployer.start(unique_id, {
    "start_command": c("yarn_{0}_start_cmd".format(type)),
    "sync": True,
  })

def _stop_yarn(deployer, unique_id, type="nm"):
  logger.debug("Stopping YARN.")
  deployer.stop(unique_id, {
    "stop_command": c("yarn_{0}_stop_cmd".format(type)),
  })
  logger.debug("Uninstalling YARN.")
  deployer.uninstall(unique_id)

def setup_suite():
  global ssh_deployer
  CWD = os.path.dirname(os.path.abspath(__file__))
  logger.info("Current working directory: {0}".format(CWD))

  logger.debug("Creating a generic SSH deployer that can be used to deploy ZooKeeper/YARN/Kafka.")
  ssh_deployer = adhoc_deployer.SSHDeployer("deployer", {
    'extract': True
  })
  runtime.set_deployer("deployer", ssh_deployer)

  _download_packages()
  _install_yarn(ssh_deployer, "yarn_rm_instance_0")
  _install_yarn(ssh_deployer, "yarn_nm_instance_0")
  _start_yarn_rm(ssh_deployer, "yarn_rm_instance_0")
  _start_yarn_nm(ssh_deployer, "yarn_nm_instance_0")

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
  yarn_process = ""#TODO yarn_deployer.get_process("yarn_instance_0")
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

  _stop_yarn(ssh_deployer, "yarn_rm_instance_0", "rm")
  _stop_yarn(ssh_deployer, "yarn_nm_instance_0", "nm")
