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
import sys
from subprocess import call

import dtf.constants as constants
from dtf.deployer import Deployer, Process
from dtf.remote_host_helper import better_exec_command, DeploymentError, get_sftp_client, get_ssh_client, open_remote_file

logger = logging.getLogger(__name__)

class YarnDeployer(Deployer):
  """
  A simple deployer that installs a YARN start_yarn
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
    'hostname': the host to install YARN on
    'install_path': the YARN install location on the remote host
    'executable': remote location of the YARN executable to copy
    'extract': [optional]
    'yarn-site': [optional] will be copied to $YARN_HOME/conf

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

    hostname = None
    if unique_id in self.processes:
      process = self.processes[unique_id]
      prev_hostname = process.hostname
      if 'hostname' in configs:
        if prev_hostname is not configs['hostname']:
          self.uninstall(unique_id, configs)
          hostname = configs['hostname']
        else:
          self.uninstall(unique_id, configs)
          hostname = prev_hostname
    elif 'hostname' in configs:
      hostname = configs['hostname']
    else:
      # we have not installed this unique_id before and no hostname is provided in the configs so raise an error
      logger.error("hostname was not provided for unique_id: " + unique_id)
      raise DeploymentError("hostname was not provided for unique_id: " + unique_id)

    install_path = configs.get('install_path') or self.default_configs.get('install_path')
    if install_path is None:
      logger.error("install_path was not provided for unique_id: " + unique_id)
      raise DeploymentError("install_path was not provided for unique_id: " + unique_id)

    # TODO - Support local file to remote file copy
    executable = configs.get('executable') or self.default_configs.get('executable')
    if executable is None:
      logger.error("executable was not provided for unique_id: " + unique_id)
      raise DeploymentError("executable was not provided for unique_id: " + unique_id)

    control_script = configs.get('control_script') or self.default_configs.get('control_script')
    if control_script is None:
      logger.error("No control script provided. YARN may not be installing completely!")
      raise DeploymentError("No control script provided. YARN may not be installing completely!")
    control_script_file = os.path.join(install_path, os.path.basename(control_script))

    yarn_home = os.path.join(install_path, "yarn")
    with get_ssh_client(hostname) as ssh:
      better_exec_command(ssh, "mkdir -p {0}".format(install_path), "Failed to create path {0}".format(install_path))
      better_exec_command(ssh, "chmod a+w {0}".format(install_path), "Failed to make path {0} writeable".format(install_path))
      with get_sftp_client(hostname) as ftp:
        if control_script is not None:
          ftp.put(control_script, control_script_file)
          better_exec_command(ssh, "chmod a+x {0}".format(control_script_file), "Failed to make {0} executable".format(control_script_file))

      install_commands = configs.get('install_commands') or self.default_configs.get('install_commands')
      if install_commands is None:
        logger.warn("No install_commands provided. YARN may be downloaded but not installed")
      else:
        for command in install_commands:
          better_exec_command(ssh, "{0} {1}".format(control_script_file, command), "Failed to run {0} {1}".format(control_script_file, command))
        yarn_site_location = configs.get('yarn-site')
        if yarn_site_location is not None:
          yarn_conf = os.path.join(yarn_home, "conf")
          better_exec_command(ssh, "mkdir -p {0}".format(yarn_conf), "Failed to create yarn config directory {0}".format(yarn_conf))
          with get_sftp_client(hostname) as ftp:
            ftp.put(yarn_site_location, os.path.join(yarn_conf, "yarn-site.xml"))
            yarn_etc_hadoop = os.path.join(os.path.join(yarn_home, 'etc'), "hadoop")
            ftp.put(yarn_site_location, os.path.join(yarn_etc_hadoop, "yarn-site.xml"))
    self.processes[unique_id] = YarnProcess(unique_id, self.service_name, hostname, install_path, yarn_home, control_script_file)

  def start(self, unique_id, configs=None):
    """
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
    control_script = self.processes[unique_id].yarn_control_script
    start_command = configs.get('start_command') or self.default_configs.get('start_command')
    if start_command is None:
      logger.error("No start_command provided. Not sure how to start YARN with control_script {0}".format(control_script))
      raise DeploymentError("No start_command provided. Not sure how to start YARN with control_script {0}".format(control_script))
    with get_ssh_client(hostname) as ssh:
      better_exec_command(ssh, "{0} {1} > /tmp/yarn.out 2> /tmp/yarn.err".format(control_script, start_command), "Failed to start YARN with commmand {0} {1}".format(control_script, start_command))

  def stop(self, unique_id, configs=None):
    """Stop the service.  If the deployer has not started a service with`unique_id` the deployer will raise an Exception
    There are two configs that will be considered:
    'terminate_only': if this config is passed in then this method is the same as terminate(unique_id) (this is also the
    behvaior if stop_command is None and not overridden)
    'stop_command': overrides the default stop_command

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
      logger.error("Can't stop {0}: process not known".format(unique_id))
      raise DeploymentError("Can't stop {0}: process not known".format(unique_id))
    control_script = self.processes[unique_id].yarn_control_script
    stop_command = configs.get('stop_command') or self.default_configs.get('stop_command')
    if stop_command is None:
      logger.error("stop_command not provided for {0}".format(unique_id))
      raise DeploymentError("stop_command not provided for {0}".format(unique_id))
    else:
      with get_ssh_client(hostname) as ssh:
        better_exec_command(ssh, "{0} {1}".format(control_script, stop_command), "Failed to stop {0} {1}".format(control_script, unique_id))

  def uninstall(self, unique_id, configs=None):
    """uninstall the service. This considers one config:
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

    install_path = self.processes[unique_id].install_path
    directories_to_remove = self.default_configs.get('directories_to_clean', [])
    directories_to_remove.extend(configs.get('additional_directories', []))
    if install_path not in directories_to_remove:
      directories_to_remove.append(install_path)
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

class YarnProcess(Process):
  def __init__(self, unique_id, service_name, hostname, install_path, yarn_home, control_script_file):
    Process.__init__(self, unique_id, service_name, hostname, install_path)
    self.yarn_home = yarn_home
    self.yarn_conf = os.path.join(yarn_home, "conf")
    self.yarn_etc_hadoop = os.path.join(os.path.join(yarn_home, "etc"), "hadoop")
    self.yarn_control_script = control_script_file
    self.port = 8088

  def get_yarn_home(self):
    return self.yarn_home