# Copyright 2014 Ahmed El-Hassany
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import unittest

from sts.entities.base import SSHEntity


class SSHEntityTest(unittest.TestCase):
  def get_ssh_config(self):
    host = "localhost"
    port = 22
    username = None
    password = None
    return host, port, username, password

  def test_connect(self):
    # Arrange
    host, port, username, password = self.get_ssh_config()
    # Act
    ssh = SSHEntity(host, port, username, password)
    ssh_client = ssh.ssh_client
    # Assert
    self.assertIsNotNone(ssh_client)
    # clean up
    ssh_client.close()

  def test_session(self):
    # Arrange
    host, port, username, password = self.get_ssh_config()
    # Act
    ssh = SSHEntity(host, port, username, password)
    session = ssh.get_new_session()
    # Assert
    self.assertIsNotNone(session)

  def test_execute_remote_command(self):
    # Arrange
    host, port, username, password = self.get_ssh_config()
    home_dir = "/"
    # Act
    ssh = SSHEntity(host, port, username, password)
    ret = ssh.execute_remote_command("ls " + home_dir)
    # Assert
    self.assertIsInstance(ret, basestring)
    self.assertTrue(len(ret) > 0)
