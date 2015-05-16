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

import mock
import unittest

from sts.entities.base import BiDirectionalLinkAbstractClass
from sts.entities.base import DirectedLinkAbstractClass

from sts.topology.graph import Graph
from sts.topology.graph import TopologyGraph


class GraphTest(unittest.TestCase):
  """
  Testing sts.topology.base.Graph
  """

  def test_init(self):
    # Arrange
    vertices = {1: None, 2: {'a': 'b'}, 3: None}
    edges = {1: {1: {}, 2: {'a': 'b'}}, 3: {1: None}}
    # Act
    graph1 = Graph()
    graph2 = Graph(vertices, edges)
    # Assert
    self.assertEquals(len(graph1.nodes), 0)
    self.assertEquals(len(graph1.edges), 0)
    self.assertEquals(len(graph2.nodes), len(vertices))
    self.assertEquals(len(graph2.edges), 3)
    self.assertEquals(graph2.nodes[1], {})
    self.assertEquals(graph2.nodes[2], vertices[2])
    self.assertEquals(graph2.nodes[3], {})
    self.assertEquals(graph2.edges[0], (1, 1, {}))
    self.assertEquals(graph2.edges[1], (1, 2, edges[1][2]))
    self.assertEquals(graph2.edges[2], (3, 1, {}))

  def test_add_vertex(self):
    # Arrange
    vertices = {1: None, 2: {'a': 'b'}, 3: None}
    edges = {1: {1: {}, 2: {'a': 'b'}}, 3: {1: None}}
    # Act
    graph = Graph(vertices, edges)
    graph.add_node(4, c='d')
    graph.add_node(5)
    # Assert
    self.assertEquals(len(graph.nodes), len(vertices) + 2)
    self.assertEquals(len(graph.edges), 3)
    self.assertEquals(graph.nodes[1], {})
    self.assertEquals(graph.nodes[2], vertices[2])
    self.assertEquals(graph.nodes[3], {})
    self.assertEquals(graph.nodes[4], {'c': 'd'})
    self.assertEquals(graph.nodes[5], {})
    self.assertTrue(graph.has_node(1))
    self.assertTrue(graph.has_node(2))
    self.assertTrue(graph.has_node(3))
    self.assertTrue(graph.has_node(4))
    self.assertTrue(graph.has_node(5))
    self.assertFalse(graph.has_node(6))

  def test_has_vertex(self):
    # Arrange
    vertices = {1: None, 2: {'a': 'b'}, 3: None}
    edges = {1: {1: {}, 2: {'a': 'b'}}, 3: {1: None}}
    # Act
    graph = Graph(vertices, edges)
    graph.add_node(4, c='d')
    graph.add_node(5)
    # Assert
    self.assertTrue(graph.has_node(1))
    self.assertTrue(graph.has_node(2))
    self.assertTrue(graph.has_node(3))
    self.assertTrue(graph.has_node(4))
    self.assertTrue(graph.has_node(5))
    self.assertFalse(graph.has_node(6))

  def test_get_vertex(self):
     # Arrange
    vertices = {1: None, 2: {'a': 'b'}, 3: None}
    edges = {1: {1: {}, 2: {'a': 'b'}}, 3: {1: None}}
    # Act
    graph = Graph(vertices, edges)
    graph.add_node(4, c='d')
    graph.add_node(5)
    # Assert
    self.assertEquals(graph.get_node(1), {})
    self.assertEquals(graph.get_node(2), {'a': 'b'})
    self.assertEquals(graph.get_node(3), {})
    self.assertEquals(graph.get_node(4), {'c': 'd'})
    self.assertEquals(graph.get_node(5), {})

  def test_edges_iter(self):
    # Arrange
    edges = {1: {1: {}, 2: {'a': 'b'}}, 3: {1: {}}}
    graph = Graph(nodes=None, edges=edges)
    # Act
    edges1 = list(graph.edges_iter(data=False))
    edges2 = list(graph.edges_iter(data=True))
    # Assert
    for edge in edges1:
      self.assertEquals(len(edge), 2)
      self.assertIn(edge[0], edges)
      self.assertIn(edge[1], edges[edge[0]])
    for edge in edges2:
      self.assertIn(edge[0], edges)
      self.assertIn(edge[1], edges[edge[0]])
      self.assertEquals(edges[edge[0]][edge[1]], edge[2])

  def test_edges_iter_with_check(self):
    # Arrange
    edges = {1: {1: {}, 2: {'a': 'b'}}, 3: {1: {}}}
    graph = Graph(nodes=None, edges=edges)
    check = lambda v1, v2, attrs: attrs.get('a', None) is not None
    # Act
    edges1 = list(graph.edges_iter_with_check(check, data=False))
    edges2 = list(graph.edges_iter_with_check(check, data=True))
    # Assert
    self.assertEquals(edges1, [(1, 2)])
    self.assertEquals(edges2, [(1, 2, {'a': 'b'})])

  def test_nodes_iter(self):
    # Arrange
    vertices = {1: None, 2: {'a': 'b'}, 3: None, 4: None, 5: None}
    graph = Graph(vertices)
    # Act
    vertices1 = list(graph.nodes_iter(data=False))
    vertices2 = list(graph.nodes_iter(data=True))
    # Assert
    for vertex in vertices1:
      self.assertTrue(vertex in vertices)
    for vertex, value in vertices2:
      value = value if value != {} else None
      self.assertEquals(vertices[vertex], value)

  def test_nodes_iter_with_check(self):
    # Arrange
    vertices = {1: None, 2: {'a': 'b'}, 3: None, 4: None, 5: None}
    graph = Graph(vertices)
    check = lambda v, attrs: attrs.get('a', None) is not None
    # Act
    vertices1 = list(graph.nodes_iter_with_check(check, data=False))
    vertices2 = list(graph.nodes_iter_with_check(check, data=True))
    # Assert
    self.assertEquals(vertices1, [2])
    self.assertEquals(vertices2, [(2, vertices[2])])

  def test_add_edge(self):
    # Arrange
    vertices = {1: None, 2: {'a': 'b'}, 3: None}
    edges = {1: {1: {}, 2: {'a': 'b'}}, 3: {1: None}}
    expected = [(1, 1, {}), (1, 1, {}), (1, 2, edges[1][2]), (3, 1, {}),
                (1, 3, {}), (1, 4, {'c': 'd'})]
    # Act
    graph = Graph(vertices, edges)
    graph.add_edge(1, 3)
    graph.add_edge(1, 4, c='d')
    # Assert
    self.assertEquals(len(graph.nodes), len(vertices) + 1)
    self.assertEquals(len(graph.edges), 3 + 2)
    self.assertEquals(graph.nodes[1], {})
    self.assertEquals(graph.nodes[2], vertices[2])
    self.assertEquals(graph.nodes[3], {})
    self.assertEquals(graph.nodes[4], {})
    self.assertTrue(graph.has_edge(1, 2))
    self.assertFalse(graph.has_edge(2, 4))
    self.assertFalse(graph.has_edge(9, 6))
    for edge in expected:
      self.assertTrue(edge in graph.edges)

  def test_remove_edge(self):
    # Arrange
    graph = Graph()
    edge1 = graph.add_edge(1, 2)
    edge2 = graph.add_edge(2, 3)
    edge3 = graph.add_edge(2, 4)
    # Act
    graph.remove_edge(*edge1)
    graph.remove_edge(*edge2)
    # Assert
    self.assertRaises(AssertionError, graph.remove_edge, 10, 20)
    self.assertFalse(graph.has_edge(*edge1))
    self.assertFalse(graph.has_edge(*edge2))
    self.assertTrue(graph.has_edge(*edge3))

  def test_remove_vertex(self):
    # Arrange
    graph = Graph()
    v1, v2, v3, v4, v5, v6, v7 = 1, 2, 3, 4, 5, 6, 7
    graph.add_node(v1)
    graph.add_node(v2)
    graph.add_node(v3)
    graph.add_node(v4)
    graph.add_node(v5)
    graph.add_node(v6)
    graph.add_node(v7)
    e1 = graph.add_edge(v1, v2)
    e2 = graph.add_edge(v3, v4)
    e3 = graph.add_edge(v3, v5)
    # Act
    graph.remove_node(v1, remove_edges=True)
    graph.remove_node(v6, remove_edges=False)
    self.assertRaises(AssertionError, graph.remove_node, v3, remove_edges=False)
    graph.remove_node(v3, remove_edges=True)
    # Assert
    self.assertFalse(graph.has_node(v1))
    self.assertTrue(graph.has_node(v2))
    self.assertFalse(graph.has_node(v3))
    self.assertTrue(graph.has_node(v4))
    self.assertTrue(graph.has_node(v5))
    self.assertFalse(graph.has_node(v6))
    self.assertTrue(graph.has_node(v7))
    self.assertFalse(graph.has_edge(*e1))
    self.assertFalse(graph.has_edge(*e2))
    self.assertFalse(graph.has_edge(*e3))

  def test_edges_src(self):
    # Arrange
    v1, v2, v3, v4 = 1, 2, 3, 4
    g = Graph()
    e1 = g.add_edge(v1, v2)
    e2 = g.add_edge(v2, v3)
    e3 = g.add_edge(v2, v4)
    # Act
    v1_src = g.out_edges(v1)
    v2_src = g.out_edges(v2)
    v3_src = g.out_edges(v3)
    # Assert
    self.assertItemsEqual([e1], v1_src)
    self.assertItemsEqual([e2, e3], v2_src)
    self.assertItemsEqual([], v3_src)

  def test_edges_dst(self):
    # Arrange
    v1, v2, v3, v4 = 1, 2, 3, 4
    g = Graph()
    e1 = g.add_edge(v1, v2)
    e2 = g.add_edge(v1, v3)
    e3 = g.add_edge(v2, v3)
    g.add_node(v4)
    # Act
    v1_dst = g.in_edges(v1)
    v2_dst = g.in_edges(v2)
    v3_dst = g.in_edges(v3)
    v4_dst = g.in_edges(v4)
    # Assert
    self.assertItemsEqual([], v1_dst)
    self.assertItemsEqual([e1], v2_dst)
    self.assertItemsEqual([e2, e3], v3_dst)
    self.assertEquals(v4_dst, [])


class TopologyGraphTest(unittest.TestCase):

  def _get_switch(self, sid, num_ports):
    """Helper method to mock switch with given number of ports"""
    name = "s%d" % sid
    switch = mock.Mock(name=name)
    switch.name = name
    switch.dpid = sid
    switch.ports = dict()
    for i in range(1, num_ports + 1):
      switch.ports[i] = mock.Mock(name="%s-%d" % (name, i))
      switch.ports[i].name = str(i)
      switch.ports[i].port_no = i
    return switch

  def _get_host(self, hid, num_interfaces):
    """Helper method to mock host with given number of interfaces"""
    name = "h%d" % hid
    interfaces = []
    for i in range(1, num_interfaces + 1):
      iface_name = "%s-eth%d" % (name, i)
      iface = mock.Mock(name=iface_name)
      iface.port_no = i
      iface.name = iface_name
      interfaces.append(iface)
    host = mock.Mock(name=name)
    host.interfaces = interfaces
    host.name = name
    host.hid = hid
    return host

  def _get_access_link(self, name, host, iface, switch, port):
    """Helper method to create bidirectional access link"""
    link = mock.Mock(name=name, spec=BiDirectionalLinkAbstractClass)
    link.node1 = host
    link.port1 = iface
    link.node2 = switch
    link.port2 = port
    return link

  def _get_network_link(self, name, switch1, port1, switch2, port2):
    """Helper method to create uni-directional link between two switches"""
    link = mock.Mock(name=name, spec=DirectedLinkAbstractClass)
    link.start_node = switch1
    link.start_port = port1
    link.end_node = switch2
    link.end_port = port2
    return link

  def test_init(self):
    # Arrange
    h1 = self._get_host(1, 1)
    h2 = self._get_host(2, 1)
    s1 = self._get_switch(3, 2)
    s2 = self._get_switch(4, 2)
    l1 = self._get_access_link('l1', h1, h1.interfaces[0], s1, s1.ports[1])
    l2 = self._get_network_link('l1', s1, s1.ports[2], s2, s2.ports[1])
    l3 = self._get_access_link('l3', h2, h2.interfaces[0], s2, s2.ports[2])
    hosts = [h1, h2]
    switches = [s1, s2]
    links = [l1, l2, l3]
    # Act
    graph = TopologyGraph(hosts, switches, links)
    # Assert
    self.assertItemsEqual([h1.name, h2.name], list(graph.hosts_iter(False)))
    self.assertItemsEqual([s1.name, s2.name], list(graph.switches_iter(False)))
    self.assertTrue(graph.has_link(l1))
    self.assertTrue(graph.has_link(l2))
    self.assertTrue(graph.has_link(l3))

  def test_add_host(self):
    # Arrange
    h1 = self._get_host(1, 1)
    h2 = self._get_host(2, 1)
    h3 = self._get_host(3, 0)
    graph = TopologyGraph()
    # Act
    graph.add_host(h1)
    graph.add_host(h2)
    # Assert
    self.assertItemsEqual([h1.name, h2.name], list(graph.hosts_iter(False)))
    self.assertTrue(graph.has_host(h1.name))
    self.assertTrue(graph.has_host(h2.name))
    self.assertFalse(graph.has_host(h3.name))

  def test_remove_host(self):
    # Arrange
    h1 = self._get_host(1, 1)
    h2 = self._get_host(2, 1)
    h3 = self._get_host(3, 0)
    graph = TopologyGraph()
    graph.add_host(h1)
    graph.add_host(h2)
    # Act
    graph.remove_host(h1.name)
    graph.remove_host(h2.name)
    remove_h3 = lambda: graph.remove_host(h3.name)
    # Assert
    self.assertRaises(AssertionError, remove_h3)
    self.assertFalse(graph.hosts)
    self.assertFalse(graph.has_host(h1.name))
    self.assertFalse(graph.has_host(h2.name))
    self.assertFalse(graph.has_host(h3.name))

  def test_add_switch(self):
    # Arrange
    s1 = self._get_switch(1, 1)
    s2 = self._get_switch(2, 1)
    s3 = self._get_switch(3, 0)
    graph = TopologyGraph()
    # Act
    graph.add_switch(s1)
    graph.add_switch(s2)
    # Assert
    self.assertItemsEqual([s1.name, s2.name], list(graph.switches_iter(False)))
    self.assertTrue(graph.has_switch(s1.name))
    self.assertTrue(graph.has_switch(s2.name))
    self.assertFalse(graph.has_switch(s3.name))

  def test_get_switch(self):
    # Arrange
    s1 = self._get_switch(1, 1)
    graph = TopologyGraph()
    graph.add_switch(s1)
    # Act
    sw = graph.get_switch('s1')
    # Assert
    self.assertEquals(s1, sw)

  def test_remove_switch(self):
    # Arrange
    s1 = self._get_switch(1, 1)
    s2 = self._get_switch(2, 1)
    s3 = self._get_switch(3, 0)
    graph = TopologyGraph()
    graph.add_switch(s1)
    graph.add_switch(s2)
    # Act
    graph.remove_switch(s1.name)
    graph.remove_switch(s2)
    remove_s3 = lambda: graph.remove_switch(s3.dpid)
    # Assert
    self.assertRaises(AssertionError, remove_s3)
    self.assertFalse(graph.switches)
    self.assertFalse(graph.has_host(s1.dpid))
    self.assertFalse(graph.has_host(s2.dpid))
    self.assertFalse(graph.has_host(s3.dpid))

  def test_add_link(self):
    # Arrange
    s1 = self._get_switch(1, 2)
    s2 = self._get_switch(2, 2)
    mocked_port = mock.Mock(name='s20-')
    mocked_port.port_no = 3
    mocked_port.name = 's2-3'
    l1 = mock.Mock(name='l1')
    l1.start_node = s1
    l1.start_port = s1.ports[1]
    l1.end_node = s2
    l1.end_port = s2.ports[1]
    l2 = mock.Mock(name='l2')
    l2.start_node = s1
    l2.start_port = s1.ports[2]
    l2.end_node = s2
    l2.end_port = mocked_port
    graph = TopologyGraph()
    graph.add_switch(s1)
    graph.add_switch(s2)
    # Act
    link = graph.add_link(l1)
    fail_add = lambda: graph.add_link(l2)
    # Assert
    self.assertEquals(link, l1)
    self.assertTrue(graph.has_link(l1))
    self.assertFalse(graph.has_link(l2))
    self.assertIsNotNone(graph.get_link('s1-1', 's2-1'))
    self.assertIsNone(graph.get_link('s1-2', 's2-2'))
    self.assertRaises(AssertionError, fail_add)

  def test_remove_link(self):
    # Arrange
    s1 = self._get_switch(1, 2)
    s2 = self._get_switch(2, 3)
    h1 = self._get_host(1, 1)
    l1 = self._get_network_link('NL1', s1, s1.ports[1], s2, s2.ports[1])
    l2 = self._get_network_link('NL2', s1, s1.ports[2], s2, s2.ports[2])
    l3 = self._get_access_link('AL1', h1, h1.interfaces[0], s2, s2.ports[3])
    graph = TopologyGraph()
    graph.add_switch(s1)
    graph.add_switch(s2)
    graph.add_host(h1)
    graph.add_link(l1)
    graph.add_link(l3, bidir=True)
    # Act
    graph.remove_link(l1)
    graph.remove_link(l3)
    fail_remove = lambda: graph.remove_link(l2)
    # Assert
    self.assertFalse(graph.has_link(l1))
    self.assertFalse(graph.has_link(l2))
    self.assertFalse(graph.has_link(l3))
    self.assertIsNone(graph.get_link("s1-1", "s2-1"))
    self.assertIsNone(graph.get_link("s1-2", "s2-2"))
    self.assertRaises(AssertionError, fail_remove)

  def test_get_host_links(self):
    # Arrange
    h1 = self._get_host(1, 2)
    h2 = self._get_host(2, 2)
    s1 = self._get_switch(1, 3)
    s2 = self._get_switch(2, 3)
    l1 = self._get_access_link('AL1', h1, h1.interfaces[0], s1, s1.ports[1])
    l2 = self._get_access_link('AL2', h1, h1.interfaces[1], s2, s2.ports[1])
    l3 = self._get_access_link('AL3', h2, h2.interfaces[0], s2, s2.ports[2])
    l4 = self._get_network_link('NL1', s1, s1.ports[3], s2, s2.ports[3])
    graph = TopologyGraph()
    graph.add_switch(s1)
    graph.add_switch(s2)
    graph.add_host(h1)
    graph.add_host(h2)
    graph.add_link(l1, bidir=True)
    graph.add_link(l2, bidir=True)
    graph.add_link(l3, bidir=True)
    graph.add_link(l4)
    # Act
    h1_links = graph.get_host_links(h1)
    h2_links = graph.get_host_links(h2)
    # Assert
    self.assertItemsEqual([l1, l2], set(h1_links))
    self.assertItemsEqual([l3], set(h2_links))

  def test_get_switches_links(self):
    # Arrange
    h1 = self._get_host(1, 2)
    h2 = self._get_host(2, 2)
    s1 = self._get_switch(1, 3)
    s2 = self._get_switch(2, 3)
    l1 = self._get_access_link('AL1', h1, h1.interfaces[0], s1, s1.ports[1])
    l2 = self._get_access_link('AL2', h1, h1.interfaces[1], s2, s2.ports[1])
    l3 = self._get_access_link('AL3', h2, h2.interfaces[0], s2, s2.ports[2])
    l4 = self._get_network_link('NL1', s1, s1.ports[3], s2, s2.ports[3])
    graph = TopologyGraph()
    graph.add_switch(s1)
    graph.add_switch(s2)
    graph.add_host(h1)
    graph.add_host(h2)
    graph.add_link(l1, bidir=True)
    graph.add_link(l2, bidir=True)
    graph.add_link(l3, bidir=True)
    graph.add_link(l4)
    # Act
    s1_links = graph.get_switch_links(s1)
    s2_links = graph.get_switch_links(s2)
    # Assert
    self.assertItemsEqual([l1, l4], set(s1_links))
    self.assertItemsEqual([l2, l3, l4], set(s2_links))

  def test_links_iter(self):
    # Arrange
    h1 = self._get_host(1, 2)
    h2 = self._get_host(2, 2)
    s1 = self._get_switch(1, 3)
    s2 = self._get_switch(2, 3)
    l1 = self._get_access_link('AL1', h1, h1.interfaces[0], s1, s1.ports[1])
    l2 = self._get_access_link('AL2', h1, h1.interfaces[1], s2, s2.ports[1])
    l3 = self._get_access_link('AL3', h2, h2.interfaces[0], s2, s2.ports[2])
    l4 = self._get_network_link('NL1', s1, s1.ports[3], s2, s2.ports[3])
    graph = TopologyGraph()
    graph.add_switch(s1)
    graph.add_switch(s2)
    graph.add_host(h1)
    graph.add_host(h2)
    graph.add_link(l1, bidir=True)
    graph.add_link(l2, bidir=True)
    graph.add_link(l3, bidir=True)
    graph.add_link(l4)
    # Act
    links_iter = list(graph.links_iter(data=False))
    links = graph.links
    # Assert
    # Count the bidir links as two
    self.assertEquals(len(links_iter), 3 * 2 + 1)
    self.assertEquals(len(links), 3 * 2 + 1)

  def test_interfaces_iter(self):
    # Arrange
    h1 = self._get_host(1, 2)
    h2 = self._get_host(2, 1)
    graph = TopologyGraph()
    graph.add_host(h1)
    graph.add_host(h2)
    # Act
    ifaces_iter = list(graph.interfaces_iter(data=False))
    ifaces = graph.interfaces
    # Assert
    self.assertEquals(len(ifaces_iter), 3)
    self.assertEquals(len(ifaces), 3)

  def test_ports_iter(self):
    s1 = self._get_switch(1, 3)
    s2 = self._get_switch(2, 2)
    graph = TopologyGraph()
    graph.add_switch(s1)
    graph.add_switch(s2)
    # Act
    ports_iter = list(graph.ports_iter(data=False))
    ports = graph.ports
    # Assert
    self.assertEquals(len(ports_iter), 5)
    self.assertEquals(len(ports), 5)
