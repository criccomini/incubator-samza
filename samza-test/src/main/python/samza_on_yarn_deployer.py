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
from subprocess import call
import json
import requests

import zopkio.constants as constants
from zopkio.deployer import Deployer, Process
from zopkio.remote_host_helper import better_exec_command, DeploymentError, get_sftp_client, get_ssh_client, open_remote_file

logger = logging.getLogger(__name__)

class SamzaOnYarnDeployer(Deployer):
  """
  A simple deployer that can install a SAMZA job and run it in YARN
  """
  list_apps_command = "ws/v1/cluster/apps"
  def __init__(self, service_name, configs=None):
    """
    :param service_name: an arbitrary name that can be used to describe the executable
    :param configs: default configurations for the other methods
    :return:
    """
    logging.getLogger("paramiko").setLevel(logging.ERROR)
    self.service_name = service_name
    if configs is None or "yarn_host" not in configs or "yarn_port" not in configs or "yarn_home" not in configs:
      logger.error("Need to specify yarn_host, yarn_port and yarn_home to run Samza on Yarn")
      raise DeploymentError("Need to specify yarn_host, yarn_port and yarn_home to run Samza on Yarn")
    self.default_configs = configs
    Deployer.__init__(self)

  def install(self, unique_id, configs=None):
    """
    Copies the executable to the remote machine under install path. Inspects the configs for te possible keys
    'yarn_hostname': the host to install YARN on
    'install_path': the YARN install location on the remote host
    'executable': remote location of the YARN executable to copy
    'yarn_home':

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

    yarn_hostname = self.default_configs["yarn_host"]
    yarn_port = self.default_configs["yarn_port"]
    yarn_home = self.default_configs.get('yarn_home')

    samza_hostname = configs.get('hostname') or self.default_configs.get('hostname')
    if samza_hostname is None:
      logger.error("Hostname was not provided for Samza Job")
      raise DeploymentError("Hostname was not provided for Samza Job")

    install_path = configs.get('install_path') or self.default_configs.get('install_path')
    if install_path is None:
      logger.error("install_path was not provided for unique_id: " + unique_id)
      raise DeploymentError("install_path was not provided for unique_id: " + unique_id)

    # TODO - Support local file to remote file copy
    executable = configs.get('executable') or self.default_configs.get('executable')
    if executable is None:
      logger.error("executable was not provided for unique_id: " + unique_id)
      raise DeploymentError("executable was not provided for unique_id: " + unique_id)

    samza_home = os.path.join(install_path, "samza")
    with get_ssh_client(yarn_hostname) as ssh:
      exec_file_name = os.path.basename(executable)
      exec_file_dir = exec_file_name.replace(".tgz", "")
      exec_file_location = os.path.join(install_path, exec_file_name)
      exec_file_install_path = os.path.join(install_path, exec_file_dir)
      better_exec_command(ssh, "mkdir -p {0}".format(exec_file_install_path), "Failed to create path {0}".format(install_path))
      better_exec_command(ssh, "chmod a+w {0}".format(install_path), "Failed to make path {0} writeable".format(install_path))
      better_exec_command(ssh, "curl " + executable + " -o " + exec_file_location, "Failed to download from " + executable)
      better_exec_command(ssh, "tar -zxvf {0} -C {1}".format(exec_file_location, exec_file_install_path), "Unable to extract samza job")
      better_exec_command(ssh, "mv {0} {1}".format(exec_file_install_path, samza_home), "Failed to move samza job")

      self.processes[unique_id] = SamzaOnYarnProcess(unique_id, self.service_name, samza_hostname, install_path, yarn_hostname, yarn_port, yarn_home)


  def start(self, unique_id, configs=None):
    """
    Uses "run-job" script to start a job
    'config-factory':
    'config-file':
    'properties": (optional) [(property-name,property-value)]
    TODO: Add option to allow Key-Value config pairs
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
    config_factory = configs.get('config-factory') or self.default_configs.get('config-factory')
    config_file = configs.get('config-file') or self.default_configs.get('config-file')
    properties = configs.get('properties') or self.default_configs.get('properties')
    if config_factory is None or config_file is None:
      logger.error("Missing config details to start a Samza Job for unique_id {0}".format(unique_id))
      raise DeploymentError("Missing config details to start a Samza Job for unique_id {0}".format(unique_id))

    hostname = self.processes[unique_id].hostname
    samza_home = self.processes[unique_id].samza_home
    samza_config = os.path.join(samza_home, "config")
    samza_bin = os.path.join(samza_home, "bin")
    with get_ssh_client(hostname) as ssh:
      command = "{0} --config-factory={1} --config-path={2}".format(os.path.join(samza_bin, "run-job.sh"), config_factory, os.path.join(samza_config, config_file))
      if properties is not None:
        for (property_name, property_value) in properties:
          command += " --config {0}={1}".format(property_name, property_value)
          self.processes[unique_id].properties[property_name] = property_value
      better_exec_command(ssh, "{0} > /tmp/output.txt 2> /tmp/error.txt".format(command), "Failed starting Samza Job")

  def stop(self, unique_id, configs=None):
    """Stop the service.  If the deployer has not started a service with`unique_id` the deployer will raise an Exception
    """
    if unique_id not in self.processes:
      logger.error("Can't stop {0}: process not known".format(unique_id))
      raise DeploymentError("Can't stop {0}: process not known".format(unique_id))
    current_process = self.processes[unique_id]
    yarn_hostname = current_process.yarn_host
    yarn_port = current_process.yarn_port

    job_name = current_process.properties['job.name']
    job_id = current_process.properties['job.id']
    if job_name is None or job_id is None:
      logger.error("Samza Job Name / Id not provided. Can't stop process {0}".format(unique_id))
      raise DeploymentError("Samza Job Name / Id not provided. Can't stop process {0}".format(unique_id))

    json_list_apps = self.queryYarn(yarn_hostname, yarn_port, self.list_apps_command).json()
    list_apps = json_list_apps['apps'].values()
    apps = reduce(list.__add__, map(lambda x: list(x), list_apps)) # Flattening the list
    app_name = job_name + "_" + job_id
    appId = None
    for app in apps:
      if app['name'] == app_name:
        appId = app['id']
        break
    if appId is None:
      logger.error("Did not find a job {0} on this YARN grid".format(app_name))
      return
    with get_ssh_client(yarn_hostname) as ssh:
      better_exec_command(ssh, os.path.join(current_process.install_path, "samza/bin/kill-yarn-job.sh {0}".format(appId)), "Failed to kill Samza job {0}".format(appId))

  def queryYarn(self, yarn_host, yarn_port, command):
    app_id_request = "http://{0}:{1}/{2}".format(yarn_host, yarn_port, command)
    response = requests.get(app_id_request)
    return response

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

    samza_home = self.processes[unique_id].samza_home
    directories_to_remove = self.default_configs.get('directories_to_clean', [])
    directories_to_remove.extend(configs.get('additional_directories', []))
    if samza_home not in directories_to_remove:
      directories_to_remove.append(samza_home)
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

class SamzaOnYarnProcess(Process):
  def __init__(self, unique_id, service_name, hostname, install_path, yarn_host, yarn_port, yarn_home):
    Process.__init__(self, unique_id, service_name, hostname, install_path)
    self.samza_home = os.path.join(install_path, "samza")
    self.yarn_host = yarn_host
    self.yarn_home = yarn_home
    self.yarn_port = yarn_port
    self.properties = {}
