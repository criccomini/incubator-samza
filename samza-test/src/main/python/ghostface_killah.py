#################################################################################################################################
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
#################################################################################################################################

import sys
import paramiko
import os.path
import random
from time import sleep
from optparse import OptionParser

#################################################################################################################################
# A "chaos monkey"-like script that periodically kills parts of Samza, including YARN (RM, NM), Kafka, and Samza (AM, container)
# This script depends on paramiko, an ssh library
#################################################################################################################################

# Create an ssh connection to the given host
def connect(host):
 client = paramiko.SSHClient()
 client.load_system_host_keys()
 client.set_missing_host_key_policy(paramiko.WarningPolicy)
 client.connect(host)
 return client

# Run the given command line using the ssh connection (throw an error if anything on stderr)
def execute(conn, cmd):
 stdin, stdout, stderr = conn.exec_command(cmd)
 output = stdout.read()
 err = stderr.read()
 if output:
   print output
 if err:
   print >> sys.stderr, err
   raise Exception("Error executing command: %s" %err)

# Unfortunately pkill on mac seems to have a length limit which prevents it working with out epic java command line arguments
def pkill(pattern):
 return "ps aux | grep " + pattern + " | grep -v 'grep' | awk -F' *' '{print $2}' | xargs kill"

# Kill the kafka broker
def kill_kafka(conn, options):
 print "Killing kafka broker...",
 execute(conn, pkill("kafka.Kafka"))
 sleep(10)
 print "restarting...", 
 execute(conn, "nohup " + options.kafka_dir + "/bin/kafka-server-start.sh " + options.kafka_dir + "/config/server.properties `</dev/null` > " + options.kafka_dir + "/nohup.out `2>&1` &")
 print 'done.'

# Kill the samza container instances
def kill_containers(conn, options):
 print "Killing samza containers"
 execute(conn, pkill("org.apache.samza.container.SamzaContainer"))

# Kill the application master
def kill_am(conn, options):
 print 'Killing application master'
 execute(conn, pkill("org.apache.samza.job.yarn.SamzaAppMaster"))

# Kill the node manager process
def kill_nm(conn, options):
 # may need to be restarted
 print "Killing node manager...",
 execute(conn, pkill("org.apache.hadoop.yarn.server.nodemanager.NodeManager"))
 sleep(10)
 print "restarting...", 
 execute(conn, "nohup " + options.yarn_dir + "/bin/yarn nodemanager `2>&1` `>` " + options.yarn_dir + "/logs/nm.log `</dev/null` >nohup.out &")
 print 'done.'

# kill the resource manager
def kill_rm(conn, options):
 print 'Killing resource manager...',
 execute(conn, pkill("org.apache.hadoop.yarn.server.resourcemanager.ResourceManager"))
 sleep(10)
 print "restarting...", 
 execute(conn, "nohup " + options.yarn_dir + "/bin/yarn resourcemanager `2>&1` `>` " + options.yarn_dir + "/logs/rm.log `</dev/null` >nohup.out &")
 print 'done.'
 
def require_arg(options, name):
 if not hasattr(options, name) or getattr(options, name) is None:
   print >> sys.stderr, "Missing required property:", name
   sys.exit(1)

# Command line options
parser = OptionParser()
parser.add_option("--node-list", dest="filename", help="A list of nodes", metavar="nodes.txt")
parser.add_option("--kill-time", dest="kill_time", help="The time in ms to sleep between killings", metavar="ms", default=60000)
parser.add_option("--kafka-dir", dest="kafka_dir", help="The directory in which to find kafka", metavar="dir")
parser.add_option("--yarn-dir", dest="yarn_dir", help="The directory in which to find yarn", metavar="dir")
parser.add_option("--kill-kafka", action="store_true", dest="kill_kafka", default=False, help="Should we kill Kafka?")
parser.add_option("--kill-rm", action="store_true", dest="kill_rm", default=False, help="Should we kill the YARN resource manager?")
parser.add_option("--kill-nm", action="store_true", dest="kill_nm", default=False, help="Should we kill the YARN node manager?")
parser.add_option("--kill-am", action="store_true", dest="kill_am", default=False, help="Should we kill the Samza application master?")

(options, args) = parser.parse_args()

if not options.filename:
 print >> sys.stderr, "Missing required option --node-list"
 parser.print_help()
 sys.exit(1)
if not os.path.exists(options.filename):
 print >> sys.stderr, "File '%s' does not exist." % options.filename
 sys.exit(1)

hosts = [line.strip() for line in open(options.filename).readlines()]
if len(hosts) < 1:
 print >> sys.stderr, "No hosts in host file."
 sys.exit(1)
 
connections = [(host, connect(host)) for host in hosts]
kill_cmds = [kill_containers]
if options.kill_kafka:
 require_arg(options, 'kafka_dir')
 kill_cmds.append(kill_kafka)
if options.kill_rm:
 require_arg(options, 'yarn_dir')
 kill_cmds.append(kill_rm)
if options.kill_nm:
 require_arg(options, 'yarn_dir')
 kill_cmds.append(kill_nm)
if options.kill_am:
 kill_cmds.append(kill_am)

while True:
 host, connection = connections[random.randint(0, len(connections) - 1)]
 kill_cmd = kill_cmds[random.randint(0, len(kill_cmds) - 1)]
 print 'Chose', host, 'for killing...' 
 kill_cmd(connection, options)
 print 'Night night...'
 sleep(60)
 
