# Copyright 2014 LinkedIn Corp.
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import logging
import os

import zopkio.constants as constants
from zopkio.deployer import Deployer, Process
from zopkio.remote_host_helper import better_exec_command, DeploymentError, get_sftp_client, get_ssh_client, open_remote_file

logger = logging.getLogger(__name__)

class KafkaDeployer(Deployer):
  """
  A simple deployer that installs a Kafka instance with Zookeeper
  """
  def __init__(self, service_name, configs=None):
    """
    :param service_name: an arbitrary name that can be used to describe the executable
    :param configs: default configurations for the other methods
    :return:
    """
    logging.getLogger("paramiko").setLevel(logging.ERROR)
    self.service_name = service_name
    self.default_configs = {} if configs is None else configs
    Deployer.__init__(self)

  def install(self, unique_id, configs=None):
    """
    Copies the executable to the remote machine under install path. Inspects the configs for te possible keys
    'hostname': host on which you want to install the system
    'install_script_name': 'the install script that will do all the functions of setting up the environment
                      and installing the system'
    'install_script_location': the location of the kafka install script on remote host
    'install_path': the Kafka install location on the remote host

    If the unique_id is already installed on a different host, this will perform the cleanup action first.
    If either 'install_path' or 'executable' are provided the new value will become the default.

    :param unique_id:
    :param configs:
    :return:
    """

    # the following is necessay to set the configs for this function as the combination of the
    # default configurations and the parameter with the parameter superceding the defaults but
    # not modifying the defaults
    if configs is None:
      configs = {}
    tmp = self.default_configs.copy()
    tmp.update(configs)
    configs = tmp

    hostname = configs.get('hostname') or self.default_configs.get('hostname')
    if hostname is None:
      logger.error("Hostname was not provided for unique_id {0} . Cannot proceed with Kafka Job installation".format(unique_id))
      raise DeploymentError("Hostname was not provided for unique_id {0} . Cannot proceed with Kafka Job installation".format(unique_id))

    # TODO - Add test to see if Zookeeper/Kafka is already running. If it is already running, raise deployment error
    install_path = configs.get('install_path') or self.default_configs.get('install_path')
    control_script = configs.get('control_script') or self.default_configs.get('control_script')
    if control_script is None or install_path is None:
      logger.error("Please provide install_path AND control_script for the Kafka Deployer")
      raise DeploymentError("Please provide install_path AND control_script for the Kafka Deployer")
    kafka_home = os.path.join(install_path, "kafka")
    zookeeper_home = os.path.join(install_path, "zookeeper")

    with get_ssh_client(hostname) as ssh:
      better_exec_command(ssh, "mkdir -p {0}".format(install_path), "Failed to create path {0}".format(install_path))
      better_exec_command(ssh, "chmod a+w {0}".format(install_path), "Failed to make path {0} writeable".format(install_path))
      target_install_file_name = os.path.join(install_path, os.path.basename(control_script))
      with get_sftp_client(hostname) as ftp:
        ftp.put(control_script, target_install_file_name)
      better_exec_command(ssh, "chmod a+x {0}".format(target_install_file_name), "Failed to make install script executable")
      install_commands = configs.get('install_commands') or self.default_configs.get('install_commands')
      if install_commands is not None:
        for command in install_commands:
          better_exec_command(ssh, "{0} {1}".format(target_install_file_name, command), "Failed to execute install command {0} {1}".format(target_install_file_name, command))

      self.processes[unique_id] = KafkaProcess(unique_id, self.service_name, hostname, install_path, kafka_home, zookeeper_home, target_install_file_name)

  def start(self, unique_id, configs=None):
    """
    Uses install script to start kafka
    'start_command':
    """
    # the following is necessay to set the configs for this function as the combination of the
    # default configurations and the parameter with the parameter superceding the defaults but
    # not modifying the defaults
    if configs is None:
      configs = {}
    tmp = self.default_configs.copy()
    tmp.update(configs)
    configs = tmp

    # TODO - Add check to see if it is already running
    if unique_id not in self.processes:
      self.install(unique_id, configs)

    hostname = self.processes[unique_id].hostname
    control_script = self.processes[unique_id].control_script
    start_command = configs.get('start_command') or self.default_configs.get('start_command')
    if start_command is None:
      logger.error("No start command provided. Not starting processes for {0}".format(unique_id))
      raise DeploymentError("No start command provided. Not starting processes for {0}".format(unique_id))
    with get_ssh_client(hostname) as ssh:
      better_exec_command(ssh, "{0} {1}".format(control_script, start_command), "Failed to start with command {0} {1}".format(control_script, start_command))


  def stop(self, unique_id, configs=None):
    """Stop the service.  If the deployer has not started a service with`unique_id` the deployer will raise an Exception
    'stop_command':
    """
    # the following is necessay to set the configs for this function as the combination of the
    # default configurations and the parameter with the parameter superceding the defaults but
    # not modifying the defaults
    if configs is None:
      configs = {}
    tmp = self.default_configs.copy()
    tmp.update(configs)
    configs = tmp

    if unique_id not in self.processes:
      logger.error("Can't stop {0}: process not known".format(unique_id))
      raise DeploymentError("Can't stop {0}: process not known".format(unique_id))
    hostname = self.processes[unique_id].hostname
    control_script = self.processes[unique_id].control_script
    stop_command = configs.get('stop_command') or self.default_configs.get('stop_command')
    if stop_command is None:
      logger.error("stop_command not specified for unique_id {0}".format(unique_id))
      raise DeploymentError("stop_command not specified for unique_id {0}".format(unique_id))
    with get_ssh_client(hostname) as ssh:
      better_exec_command(ssh, '{0} {1}'.format(control_script, stop_command), "Failed to stop process using {0} {1}".format(control_script, stop_command))

  def uninstall(self, unique_id, configs=None):
    """uninstall the service.  If the deployer has not started a service with
    `unique_id` this will raise a DeploymentError.  This considers one config:
    'additional_directories': a list of directories to remove in addition to those provided in the constructor plus
     the install path. This will update the directories to remove but does not override it
    :param unique_id:
    :param configs:
    :return:
    """
    # the following is necessay to set the configs for this function as the combination of the
    # default configurations and the parameter with the parameter superceding the defaults but
    # not modifying the defaults
    if configs is None:
      configs = {}
    tmp = self.default_configs.copy()
    tmp.update(configs)
    configs = tmp

    if unique_id in self.processes:
      hostname = self.processes[unique_id].hostname
    else:
      logger.error("Can't uninstall {0}: process not known".format(unique_id))
      raise DeploymentError("Can't uninstall {0}: process not known".format(unique_id))

    kafka_home = self.processes[unique_id].kafka_home
    zookeeper_home = self.processes[unique_id].zookeeper_home
    directories_to_remove = self.default_configs.get('directories_to_clean', [])
    directories_to_remove.extend(configs.get('additional_directories', []))
    if kafka_home not in directories_to_remove:
      directories_to_remove.append(kafka_home)
    if zookeeper_home not in directories_to_remove:
      directories_to_remove.append(zookeeper_home)
    with get_ssh_client(hostname) as ssh:
      for directory_to_remove in directories_to_remove:
        better_exec_command(ssh, "rm -rf {0}".format(directory_to_remove), "Failed to remove {0}".format(directory_to_remove))

  def get_pid(self, unique_id, configs=None):
    """Gets the pid of the process with `unique_id`.  If the deployer does not know of a process
    with `unique_id` then it should return a value of constants.PROCESS_NOT_RUNNING_PID
    """
    RECV_BLOCK_SIZE = 16
    # the following is necessay to set the configs for this function as the combination of the
    # default configurations and the parameter with the parameter superceding the defaults but
    # not modifying the defaults
    if configs is None:
      configs = {}
    tmp = self.default_configs.copy()
    tmp.update(configs)
    configs = tmp

    if unique_id in self.processes:
      hostname = self.processes[unique_id].hostname
    else:
      return constants.PROCESS_NOT_RUNNING_PID

    if self.processes[unique_id].start_command is None:
      return constants.PROCESS_NOT_RUNNING_PID

    if 'pid_file' in configs.keys():
      with open_remote_file(hostname, configs['pid_file']) as pid_file:
        full_output = pid_file.read()
    else:
      pid_keyword = self.processes[unique_id].start_command
      if self.processes[unique_id].args is not None:
        pid_keyword = "{0} {1}".format(pid_keyword, ' '.join(self.processes[unique_id].args))
      pid_keyword = configs.get('pid_keyword', pid_keyword)
      # TODO(jehrlich): come up with a simpler approach to this
      pid_command = "ps aux | grep '{0}' | grep -v grep | tr -s ' ' | cut -d ' ' -f 2 | grep -Eo '[0-9]+'".format(pid_keyword)
      pid_command = configs.get('pid_command', pid_command)
      non_failing_command = "{0}; if [ $? -le 1 ]; then true;  else false; fi;".format(pid_command)
      with get_ssh_client(hostname) as ssh:
        chan = better_exec_command(ssh, non_failing_command, "Failed to get PID")
      output = chan.recv(RECV_BLOCK_SIZE)
      full_output = output
      while len(output) > 0:
        output = chan.recv(RECV_BLOCK_SIZE)
        full_output += output
    if len(full_output) > 0:
      pids = [int(pid_str) for pid_str in full_output.split('\n') if pid_str.isdigit()]
      if len(pids) > 0:
        return pids

    return constants.PROCESS_NOT_RUNNING_PID

  def get_host(self, unique_id):
    """Gets the host of the process with `unique_id`.  If the deployer does not know of a process
    with `unique_id` then it should return a value of SOME_SENTINAL_VALUE

    :Parameter unique_id: the name of the process
    :raises NameError if the name is not  valid process
    """
    if unique_id in self.processes:
      return self.processes[unique_id].hostname
    logger.error("{0} not a known process".format(unique_id))
    raise NameError("{0} not a known process".format(unique_id))

  def get_processes(self):
    """ Gets all processes that have been started by this deployer

    :Returns: A list of Processes
    """
    return self.processes.values()

  def get_process(self, unique_id):
    if unique_id in self.processes:
      return self.processes[unique_id]
    logger.error("{0} not a known process".format(unique_id))
    raise NameError("{0} not a known process".format(unique_id))

class KafkaProcess(Process):
  def __init__(self, unique_id, service_name, hostname, install_path, kafka_home, zookeeper_home, control_script):
    Process.__init__(self, unique_id, service_name, hostname, install_path)
    self.kafka_home = kafka_home
    self.zookeeper_home = zookeeper_home
    self.control_script = control_script
