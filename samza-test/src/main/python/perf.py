import os
from zopkio.runtime import get_active_config as c

LOGS_DIRECTORY = 'logs'
OUTPUT_DIRECTORY = 'output'

def machine_logs():
  log_paths = {}

  # Attach proper path to all logs.
  for config_prefix in ['zookeeper', 'yarn_rm', 'yarn_nm', 'kafka']:
    deployed_path = os.path.join(c('remote_install_path'), c(config_prefix + '_install_path'))
    relative_log_paths = c(config_prefix + '_logs')
    log_paths[config_prefix] = map(lambda l: os.path.join(deployed_path, l), relative_log_paths)

  return {
    'zookeeper_instance_0': log_paths['zookeeper'],
    'kafka_instance_0': log_paths['kafka'],
    'yarn_rm_instance_0': log_paths['yarn_rm'],
    'yarn_nm_instance_0': log_paths['yarn_nm'],
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
