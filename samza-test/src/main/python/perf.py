import os

LOGS_DIRECTORY = "/tmp/yarn/yarn_instance_0/logs/"
OUTPUT_DIRECTORY = "/tmp/yarn/yarn_instance_0/results/"

def machine_logs():
  return {
    "yarn_instance_0": [os.path.join("/tmp/yarn/yarn_instance_0", "logs/yarn_instance_0.log")]
  }

def naarad_logs():
  return {}


def naarad_config(config, test_name=None):
  return os.path.join(os.path.dirname(os.path.abspath(__file__)), "naarad.cfg")
