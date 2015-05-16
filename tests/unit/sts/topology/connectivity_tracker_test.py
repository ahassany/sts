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


from mock import Mock
import unittest
from sts.topology.connectivity_tracker import ConnectivityTracker

class ConnectivityTrackerTest(unittest.TestCase):

  def test_init(self):
    # Arrange
    # Act
    tracker1 = ConnectivityTracker(True)
    tracker2 = ConnectivityTracker(False)
    # Assert
    self.assertIsNotNone(tracker1)
    self.assertIsNotNone(tracker2)
    self.assertTrue(tracker1.default_connected)
    self.assertFalse(tracker2.default_connected)

  def test_add_connected_hosts(self):
    # Arrange
    h1 = Mock(name='h1')
    h2 = Mock(name='h2')
    eth0 = Mock(name='h1-eth0')
    eth1 = Mock(name='h2-eth0')
    policy = Mock(name='p1')
    tracker = ConnectivityTracker(False)
    # Act
    tracker.add_connected_hosts(h1, eth0, h2, eth1, policy)
    # Assert
    self.assertTrue(tracker.is_connected(h1, h2))
    self.assertEquals(len(tracker.connected_pairs[h1][h2]), 1)
    self.assertEquals(len(tracker.disconnected_pairs[h1][h2]), 0)
    self.assertIn(policy, tracker.policies)

  def test_add_disconnected_hosts(self):
    # Arrange
    h1 = Mock(name='h1')
    h2 = Mock(name='h2')
    eth0 = Mock(name='h1-eth0')
    eth1 = Mock(name='h2-eth0')
    policy = Mock(name='p1')
    tracker = ConnectivityTracker(True)
    # Act
    tracker.add_disconnected_hosts(h1, eth0, h2, eth1, policy)
    # Assert
    self.assertFalse(tracker.is_connected(h1, h2))
    self.assertEquals(len(tracker.connected_pairs[h1][h2]), 0)
    self.assertEquals(len(tracker.disconnected_pairs[h1][h2]), 1)
    self.assertIn(policy, tracker.policies)

  def test_remove_connected_hosts(self):
    # Arrange
    h1 = Mock(name='h1')
    h2 = Mock(name='h2')
    eth0 = Mock(name='h1-eth0')
    eth1 = Mock(name='h2-eth0')
    policy = Mock(name='p1')
    tracker1 = ConnectivityTracker(False)
    tracker2 = ConnectivityTracker(False)
    tracker1.add_connected_hosts(h1, eth0, h2, eth1, policy)
    tracker2.add_connected_hosts(h1, eth0, h2, eth1, policy)
    # Act
    tracker1.remove_connected_hosts(h1, eth0, h2, eth1, True)
    tracker2.remove_connected_hosts(h1, eth0, h2, eth1, False)
    # Assert
    self.assertFalse(tracker1.is_connected(h1, h2))
    self.assertEquals(len(tracker1.connected_pairs[h1][h2]), 0)
    self.assertEquals(len(tracker1.disconnected_pairs[h1][h2]), 0)
    self.assertNotIn(policy, tracker1.policies)
    self.assertFalse(tracker2.is_connected(h1, h2))
    self.assertEquals(len(tracker2.connected_pairs[h1][h2]), 0)
    self.assertEquals(len(tracker2.disconnected_pairs[h1][h2]), 0)
    self.assertIn(policy, tracker2.policies)

  def test_remove_connected_hosts_wildcard(self):
    # Arrange
    h1 = Mock(name='h1')
    h2 = Mock(name='h2')
    eth0 = Mock(name='h1-eth0')
    eth1 = Mock(name='h2-eth0')
    policy = Mock(name='p1')
    tracker = ConnectivityTracker(False)
    tracker.add_connected_hosts(h1, eth0, h2, eth1, policy)
    # Act
    tracker.remove_connected_hosts(h1, None, h2, None, True)
    # Assert
    self.assertFalse(tracker.is_connected(h1, h2))
    self.assertEquals(len(tracker.connected_pairs[h1][h2]), 0)
    self.assertEquals(len(tracker.disconnected_pairs[h1][h2]), 0)
    self.assertNotIn(policy, tracker.policies)

  def test_remove_disconnected_hosts(self):
    # Arrange
    h1 = Mock(name='h1')
    h2 = Mock(name='h2')
    eth0 = Mock(name='h1-eth0')
    eth1 = Mock(name='h2-eth0')
    policy = Mock(name='p1')
    tracker1 = ConnectivityTracker(True)
    tracker1.add_disconnected_hosts(h1, eth0, h2, eth1, policy)
    tracker2 = ConnectivityTracker(True)
    tracker2.add_disconnected_hosts(h1, eth0, h2, eth1, policy)
    # Act
    tracker1.remove_disconnected_hosts(h1, eth0, h2, eth1, True)
    tracker2.remove_disconnected_hosts(h1, eth0, h2, eth1, False)
    # Assert
    self.assertTrue(tracker1.is_connected(h1, h2))
    self.assertEquals(len(tracker1.connected_pairs[h1][h2]), 0)
    self.assertEquals(len(tracker1.disconnected_pairs[h1][h2]), 0)
    self.assertNotIn(policy, tracker1.policies)
    self.assertTrue(tracker2.is_connected(h1, h2))
    self.assertEquals(len(tracker2.connected_pairs[h1][h2]), 0)
    self.assertEquals(len(tracker2.disconnected_pairs[h1][h2]), 0)
    self.assertIn(policy, tracker2.policies)

  def test_remove_disconnected_hosts_wildcard(self):
    # Arrange
    h1 = Mock(name='h1')
    h2 = Mock(name='h2')
    eth0 = Mock(name='h1-eth0')
    eth1 = Mock(name='h2-eth0')
    policy = Mock(name='p1')
    tracker = ConnectivityTracker(True)
    tracker.add_disconnected_hosts(h1, eth0, h2, eth1, policy)
    # Act
    tracker.remove_disconnected_hosts(h1, None, h2, None, True)
    # Assert
    self.assertTrue(tracker.is_connected(h1, h2))
    self.assertEquals(len(tracker.connected_pairs[h1][h2]), 0)
    self.assertEquals(len(tracker.disconnected_pairs[h1][h2]), 0)
    self.assertNotIn(policy, tracker.policies)

  def test_remove_policy(self):
    # Arrange
    h1 = Mock(name='h1')
    h2 = Mock(name='h2')
    eth0 = Mock(name='h1-eth0')
    eth1 = Mock(name='h2-eth0')
    policy = Mock(name='p1')
    tracker = ConnectivityTracker(True)
    tracker.add_disconnected_hosts(h1, eth0, h2, eth1, policy)
    # Act
    tracker.remove_policy(policy)
    # Assert
    self.assertTrue(tracker.is_connected(h1, h2))
    self.assertNotIn(policy, tracker.policies)
