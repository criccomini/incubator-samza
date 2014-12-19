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

import os
import re
import logging
import json
import requests
import shutil
import tarfile
import zopkio.constants as constants

from subprocess import PIPE, Popen
from zopkio.deployer import Deployer, Process
from zopkio.remote_host_helper import better_exec_command, DeploymentError, get_sftp_client, get_ssh_client, open_remote_file

logger = logging.getLogger(__name__)

class SamzaJobYarnDeployer(Deployer):
  def __init__(self, configs={}):
    logging.getLogger("paramiko").setLevel(logging.ERROR)
    # map from job_id to app_id
    self.app_ids = {}
    self.default_configs = configs
    Deployer.__init__(self)

  def install(self, package_id, configs={}):
    """
    TODO docs
    yarn_nm_hosts
    install_path
    executable
    """
    configs = self._get_merged_configs(configs)
    self._validate_configs(configs, ['yarn_nm_hosts', 'install_path', 'executable'])

    # Get configs.
    nm_hosts = configs.get('yarn_nm_hosts')
    install_path = configs.get('install_path')
    executable = configs.get('executable')

    # FTP and decompress job tarball to all NMs.
    exec_file_location = os.path.join(install_path, self._get_package_tgz_name(package_id))
    exec_file_install_path = os.path.join(install_path, package_id)
    for host in nm_hosts:
      with get_ssh_client(host) as ssh:
        better_exec_command(ssh, "mkdir -p {0}".format(install_path), "Failed to create path: {0}".format(install_path))
      with get_sftp_client(host) as ftp:
        ftp.put(executable, exec_file_location)

    # Extract archive locally so we can use run-job.sh.
    executable_tgz = tarfile.open(executable, 'r:gz')
    executable_tgz.extractall(package_id)

  def start(self, job_id, configs={}):
    """
    TODO docs
    TODO it's kind of weird to have package_id as config. seems like it should be in method. Discuss with jehrlich.
    'package_id':
    'config_factory':
    'config_file':
    'install_path':
    'properties": (optional) [(property-name,property-value)]
    """
    configs = self._get_merged_configs(configs)
    self._validate_configs(configs, ['package_id', 'config_factory', 'config_file', 'install_path'])

    # Get configs.
    package_id = configs.get('package_id')
    config_factory = configs.get('config_factory')
    config_file = configs.get('config_file')
    install_path = configs.get('install_path')
    properties = configs.get('properties', {})
    properties['yarn.package.path'] = 'file:' + os.path.join(install_path, self._get_package_tgz_name(package_id))

    # Execute bin/run-job.sh locally from driver machine.
    command = "{0} --config-factory={1} --config-path={2}".format(os.path.join(package_id, "bin/run-job.sh"), config_factory, os.path.join(package_id, config_file))
    for property_name, property_value in properties.iteritems():
      command += " --config {0}={1}".format(property_name, property_value)
    p = Popen(command.split(' '), stdin=PIPE, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    assert p.returncode == 0, "Command returned non-zero exit code ({0}): {1}".format(p.returncode, command)

    # Save application_id for job_id so we can kill the job later.
    regex = r'.*Submitted application (\w*)'
    match = re.match(regex, output.replace("\n", ' '))
    assert match, "Job ({0}) appears not to have started. Expected to see a log line matching regex: {1}".format(job_id, regex)
    app_id = match.group(1)
    logger.debug("Got application_id {0} for job_id {1}.".format(app_id, job_id))
    self.app_ids[job_id] = app_id

  def stop(self, job_id, configs={}):
    """
    TODO docs
    TODO it's kind of weird to have package_id as config. seems like it should be in method. Discuss with jehrlich.
    'package_id':
    'install_path':
    """
    configs = self._get_merged_configs(configs)
    self._validate_configs(configs, ['package_id', 'install_path'])

    # Get configs.
    package_id = configs.get('package_id')
    install_path = configs.get('install_path')

    # Get the application_id for the job.
    application_id = self.app_ids.get(job_id)

    # Kill the job, if it's been started, or WARN and return if it's wasn't.
    if not application_id:
      logger.warn("Can't stop a job that was never started: {0}".format(job_id))
    else:
      command = "{0} {1}".format(os.path.join(package_id, "bin/kill-yarn-job.sh"), application_id)
      p = Popen(command.split(' '), stdin=PIPE, stdout=PIPE, stderr=PIPE)
      p.wait()
      assert p.returncode == 0, "Command returned non-zero exit code ({0}): {1}".format(p.returncode, command)

  def uninstall(self, package_id, configs={}):
    """
    TODO docs
    yarn_nm_hosts 
    install_path
    """
    configs = self._get_merged_configs(configs)
    self._validate_configs(configs, ['yarn_nm_hosts', 'install_path'])
    
    # Get configs.
    nm_hosts = configs.get('yarn_nm_hosts')
    install_path = configs.get('install_path')

    # Delete job package on all NMs.
    exec_file_install_path = os.path.join(install_path, package_id)
    for host in nm_hosts:
      with get_ssh_client(host) as ssh:
        better_exec_command(ssh, "rm -rf {0}".format(exec_file_install_path), "Failed to remove {0}".format(exec_file_install_path))

    # Delete job pacakge directory from local driver box.
    shutil.rmtree(package_id)

  # TODO we should implement the below helper methods over time, as we need them.

  def get_pid(self, container_id, configs={}):
    raise NotImplementedError

  def get_host(self, container_id):
    raise NotImplementedError

  def get_containers(self, job_id):
    raise NotImplementedError

  def get_jobs(self):
    raise NotImplementedError

  def sleep(self, container_id, delay, configs={}):
    raise NotImplementedError

  def pause(self, container_id, configs={}):
    raise NotImplementedError

  def resume(self, container_id, configs={}):
    raise NotImplementedError

  def kill(self, container_id, configs={}):
    raise NotImplementedError

  def terminate(self, container_id, configs={}):
    raise NotImplementedError

  def get_logs(self, container_id, logs, directory):
    raise NotImplementedError

  def _validate_configs(self, configs, config_keys):
    for required_config in config_keys:
      assert configs.get(required_config), 'Required config is undefined: {0}'.format(required_config)

  def _get_merged_configs(self, configs):
    tmp = self.default_configs.copy()
    tmp.update(configs)
    return tmp

  def _get_package_tgz_name(self, package_id):
    return '{0}.tgz'.format(package_id)

