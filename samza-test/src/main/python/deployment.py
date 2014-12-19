import os
import logging
import shutil
import urllib
import zopkio.runtime as runtime
import zopkio.adhoc_deployer as adhoc_deployer
from zopkio.runtime import get_active_config as c

logger = logging.getLogger(__name__)
deployers = None

def _download_packages():
  for url_key in ['url_hadoop', 'url_kafka', 'url_zookeeper']:
    logger.debug('Getting download URL for: {0}'.format(url_key))
    url = c(url_key)
    filename = os.path.basename(url)
    if os.path.exists(filename):
      logger.debug('Using cached file: {0}'.format(filename))
    else:
      logger.info('Downloading: {0}'.format(url))
      urllib.urlretrieve(url, filename)

def _new_ssh_deployer(config_prefix, name=None):
  deployer_name = config_prefix if name == None else name
  return adhoc_deployer.SSHDeployer(deployer_name, {
    'install_path': os.path.join(c('remote_install_path'), c(config_prefix + '_install_path')),
    'executable': c(config_prefix + '_executable'),
    'post_install_cmds': c(config_prefix + '_post_install_cmds', []),
    'start_command': c(config_prefix + '_start_cmd'),
    'stop_command': c(config_prefix + '_stop_cmd'),
    'extract': True,
    'sync': True,
  })

def setup_suite():
  global deployers
  CWD = os.path.dirname(os.path.abspath(__file__))
  logger.info('Current working directory: {0}'.format(CWD))

  _download_packages()

  deployers = {
    'zookeeper': _new_ssh_deployer('zookeeper'),
    'yarn_rm': _new_ssh_deployer('yarn_rm'),
    'yarn_nm': _new_ssh_deployer('yarn_nm'),
    'kafka': _new_ssh_deployer('kafka'),
  }

  # enforce install order through list
  for name in ['zookeeper', 'yarn_rm', 'yarn_nm', 'kafka']:
    deployer = deployers[name]
    runtime.set_deployer(name, deployer)
    for instance, host in c(name + '_hosts').iteritems():
      logger.info('Deploying {0} on host: {1}'.format(instance, host))
      deployer.deploy(instance, {
        'hostname': host
      })

# TODO do we need these?
def setup():
  pass

def teardown():
  pass

def teardown_suite(): 
  # TODO properly teardown
  return
  for name, deployer in deployers.iteritems():
    unique_id = name + '_instance_0'
    deployer.undeploy(unique_id, {
      'additional_directories': ['/tmp/kafka-logs', '/tmp/zookeeper']
    })

