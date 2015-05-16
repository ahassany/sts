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

"""
A Simple Library for topology graphs.
"""

import logging

try:
  import networkx
except ImportError:
  networkx = None


LOG = logging.getLogger("sts.topology.graph")


def edges_iter_with_check(graph, check, data=False):
  """
  Iterate over edges in the topology and return edges which
  check(v1, v2, attrs) is True
  """
  for node1, node2, attrs in graph.edges_iter(data=True):
    if check(node1, node2, attrs):
      if data:
        yield node1, node2, attrs
      else:
        yield node1, node2


def nodes_iter_with_check(graph, check, data=False):
  """
  Iterate over nodes in the topology and return nodes which
  check(vertex_id, attrs) is True
  """
  for node, attrs in graph.nodes_iter(data=True):
    if check(node, attrs):
      if data:
        yield node, attrs
      else:
        yield node


class Graph(object):
  """
  A generic graph representation
  """

  def __init__(self, nodes=None, edges=None):
    """
    Args:
      nodes: dict of node_id->attrs
      edges: adjacency matrix
    """
    self._nodes = {}
    self._edges = {}
    if nodes is not None:
      assert isinstance(nodes, dict)
      for node, attrs in nodes.iteritems():
        attrs = attrs or {}
        self.add_node(node, **attrs)
    if edges is not None:
      assert isinstance(edges, dict)
      for node1, val1 in edges.iteritems():
        for node2, attrs in val1.iteritems():
          attrs = attrs or {}
          self.add_edge(node1, node2, **attrs)

  @property
  def nodes(self):
    """Returns a dict of the nodes in the graph."""
    return self._nodes

  @property
  def node(self):
    """Get a node by index"""
    return self._nodes

  def nodes_iter(self, data=False):
    """Return an iterator over nodes"""
    for node, attrs in self._nodes.iteritems():
      if data:
        yield node, attrs
      else:
        yield node

  def nodes_iter_with_check(self, check, data=False):
    """
    Iterate over nodes in the topology and return nodes which
    check(node_id, attrs) is True
    """
    return nodes_iter_with_check(self, check, data)

  @property
  def edges(self):
    """
    Returns a list of the edges in the graph.

    Each edge is represented as a tuple of start, end nodes and attrs dict.
    """
    edges = []
    for node1, val1 in self._edges.iteritems():
      for node2, attrs in val1.iteritems():
        edges.append((node1, node2, attrs))
    return edges

  def add_node(self, node, **attrs):
    """Add node to the graph."""
    self._nodes[node] = attrs
    return node

  def get_node(self, node):
    return self._nodes[node]

  def remove_node(self, node, remove_edges=True):
    """
    Remove a node from the graph.

    Args:
      node:
      remove_edges: if True will remove all the edges connected to the node.
    """
    assert self.has_node(node)
    edges = []
    for edge in self.edges_iter(data=False):
      if node in edge:
        edges.append(edge)
    if remove_edges == False:
      assert len(edges) == 0, "Node is part of some edges"
    for edge in edges:
      self.remove_edge(*edge)
    del self._nodes[node]

  def has_node(self, node):
    """Return True if the graph contains the node"""
    return node in self.nodes

  def edges_iter(self, data=False):
    """
    Returns an iterator of the edges in the graph.

    Each edge is represented as a tuple of start, end nodes and attrs dict.
    """
    for node1, val1 in self._edges.iteritems():
      for node2, attrs in val1.iteritems():
        if data:
          yield node1, node2, attrs
        else:
          yield node1, node2

  def edges_iter_with_check(self, check, data=False):
    """
    Iterate over edges in the topology and return edges which
    check(v1, v2, attrs) is True
    """
    return edges_iter_with_check(self, check, data)

  def get_edge_data(self, node1, node2):
    """Return the edge between node1 and node2."""
    return self._edges[node1][node2]

  def add_edge(self, node1, node2, **attrs):
    """Add edge between node1 and node2."""
    if node1 not in self.nodes:
      self.add_node(node1)
    if node2 not in self.nodes:
      self.add_node(node2)
    if self._edges.get(node1, None) is None:
      self._edges[node1] = {}
    self._edges[node1][node2] = attrs
    return node1, node2

  def remove_edge(self, node1, node2):
    """Removes an edge between two nodes"""
    assert self.has_edge(node1, node2), "No edge ('%s', '%s')" % (node1, node2)
    del self._edges[node1][node2]

  def has_edge(self, node1, node2):
    """Return True if an edge exists between two nodes node1 and v2"""
    try:
      self.get_edge_data(node1, node2)
      return True
    except KeyError:
      return False

  def out_edges(self, node):
    """Return list of edges in which v1 is source"""
    adj = self._edges.get(node, {})
    edges = []
    for vertex in adj:
      edges.append((node, vertex))
    return edges

  def in_edges(self, node):
    """Return list of edges in which v2 is destination"""
    edges = []
    for node1, adj in self._edges.iteritems():
      if node in adj:
        edges.append((node1, node))
    return edges


class NodeType(object):
  """Vertices Type for Network graph"""
  HOST = 'host'
  SWITCH = 'switch'
  PORT = 'PORT'
  INTERFACE = 'INTERFACE'


class EdgeType(object):
  """Edges Type for Network graph"""
  LINK = 'link'
  INTERNAL_LINK = 'internal_link'  # for switch-port and host-interface links


class TopologyGraph(object):
  """
  A high level graph of the network topology.

  This graph considers ports and host interfaces as nodes with bidirectional
  edges to the switch/host. To tell if the edge is a link or a switch-port
  or host-interface association see `is_link`.

  TODO: give the ability to pass a custom:
    interfaces iterator: see `_interfaces_iter`
    ports iterator: see `_ports_iter`
    host vertex id: see `_host_node_id`
    interface vertex id: see `_interface_node_id`
    switch vertex id: see `_switch_node_id`
    port vertex id: see `_port_node_id`
    link nodes: see `_get_link_nodes`
  """
  def __init__(self, hosts=None, switches=None, links=None):
    super(TopologyGraph, self).__init__()
    if networkx is None:
      self._g = Graph()
    else:
      self._g = networkx.DiGraph()
    self.log = LOG
    # Load initial configurations
    hosts = hosts or []
    switches = switches or []
    links = links or []
    for host in hosts:
      self.add_host(host)
    for switch in switches:
      self.add_switch(switch)
    for link in links:
      self.add_link(link)

  def is_host(self, vertex, attrs):
    """Returns True if the vertex is a host."""
    assert vertex is not None
    return attrs.get('ntype', None) == NodeType.HOST

  def is_interface(self, vertex, attrs):
    """Returns True if the vertex is an interface."""
    assert vertex is not None
    return attrs.get('ntype', None) == NodeType.INTERFACE

  def is_switch(self, vertex, attrs):
    """Returns True if the vertex is a switch."""
    assert vertex is not None
    return attrs.get('ntype', None) == NodeType.SWITCH

  def is_port(self, vertex, attrs):
    """Returns True if the vertex is a port for a switch."""
    assert vertex is not None
    return attrs.get('ntype', None) == NodeType.PORT

  def is_link(self, vertex1, vertex2, attrs):
    """
    Check if it's an actual network (or access) link.

    This check is distinguish the network links from the virtual port-switch
    or host-interface edges.
    """
    assert vertex1 is not None
    assert vertex2 is not None
    return attrs.get('etype', None) == EdgeType.LINK

  def _host_node_id(self, host):
    """Utility method to get the vertex ID for a host"""
    vertex = getattr(host, 'name', getattr(host, 'hid', host))
    self.log.debug("_host_node_id (%s): %s", host, vertex)
    return vertex

  def _interface_node_id(self, interface):
    """Utility method to get the vertex ID for an Interface"""
    node = getattr(interface, 'name', None)
    if node == '' or node is None:
      node = getattr(interface, 'port_no', None)
    self.log.debug("_interface_node_id (%s): %s", interface, node)
    return node

  def _port_node_id(self, switch, port):
    """Utility method to get the node ID for an Interface"""
    node = getattr(port, 'name', None)
    sid = self._switch_node_id(switch)
    if node == '' or node is None or str(sid) not in node:
      node = getattr(port, 'port_no', None)
      node = "%s-%s" % (sid, node)
    self.log.debug("_port_node_id (%s, %s): %s", switch, port.name, node)
    return node

  def _switch_node_id(self, switch):
    """Utility method to get the node ID for a switch"""
    node = getattr(switch, 'name', getattr(switch, 'dpid', switch))
    self.log.debug("_switch_node_id (%s): %s", switch, node)
    return node

  def _get_link_nodes(self, link):
    """
    Returns the two nodes connect by the link.
    A hack to get around bidirectional and unidirectional links.
    """
    if hasattr(link, 'start_node'):
      node1 = link.start_node
      node2 = link.end_node
      vertex1 = link.start_port
      vertex2 = link.end_port
    else:
      node1 = link.node1
      node2 = link.node2
      vertex1 = link.port1
      vertex2 = link.port2

    def guess_vertex_id(node, vertex):
      # This is ugly why to find out if the edge is an interface or a port.
      # But necessary in order to keep out any information about nodes type
      # from this class
      v_port = self._port_node_id(node, vertex)
      v_iface = self._interface_node_id(vertex)
      if (self._g.has_node(v_port) and
          self.is_port(v_port, self._g.node[v_port])):
        v = v_port
      elif (self._g.has_node(v_iface) and
            self.is_interface(v_iface, self._g.node[v_iface])):
        v = v_iface
      else:
        v = None
      return v

    v1, v2 = guess_vertex_id(node1, vertex1), guess_vertex_id(node2, vertex2)
    self.log.debug("_get_link_nodes (%s): %s<->%s", link, v1, v2)
    return v1, v2

  def hosts_iter(self, data=False):
    """
    Iterates over hosts in the topology.

    Args:
      data: If true not only host is returned but the attributes as well
    """
    return nodes_iter_with_check(self._g, check=self.is_host, data=data)

  @property
  def hosts(self):
    """List of hosts (objects not just IDs) in the topology"""
    hosts = [self.get_host(hid) for hid in self.hosts_iter(False)]
    return hosts

  def interfaces_iter(self, data=False):
    """
    Iterates over host interfaces in the topology.

    Args:
      data: If true not only interfaces are returned but the attributes as well.
    """
    return nodes_iter_with_check(self._g, check=self.is_interface,
                                 data=data)

  @property
  def interfaces(self):
    """List of interfaces (objects not just IDs) in the topology"""
    interfaces = [self.get_interface(iface) for iface in
                  self.interfaces_iter(False)]
    return interfaces

  def ports_iter(self, data=False):
    """
    Iterates over switch ports in the topology.

    Args:
      data: If true not only ports are returned but the attributes as well.
    """
    return nodes_iter_with_check(self._g, check=self.is_port, data=data)

  @property
  def ports(self):
    """List of ports (objects not just IDs) in the topology"""
    ports = [self.get_port(port) for port in self.ports_iter(False)]
    return ports

  def switches_iter(self, data=False):
    """
    Iterates over switches in the topology.

    Args:
      data: If true not only switches are returned but the attributes as well
    """
    return nodes_iter_with_check(self._g, check=self.is_switch, data=data)

  @property
  def switches(self):
    """List of Switches (objects not just IDs) in the topology"""
    switches = [self.get_switch(switch) for switch in self.switches_iter(False)]
    return switches

  def links_iter(self, data=False):
    """
    Iterates over links in the topology.

    Args:
      data: If true not only links are returned but the attributes as well
    """
    for src_port, dst_port, value in self.edges_iter(data=True):
      if not self.is_link(src_port, dst_port, value):
        continue
      src_node = value.get('src_node', None)
      dst_node = value.get('dst_node', None)
      if data:
        yield src_node, src_port, dst_node, dst_port, value
      else:
        yield src_node, src_port, dst_node, dst_port

  @property
  def links(self):
    """List of Links (objects not just IDs) in the topology"""
    all_links = [link['obj'] for _, _, _, _, link in self.links_iter(True)]
    return all_links

  def edges_iter(self, data=False):
    """
    Iterates over all edges in the topology (including the artificial
    host->interface and switch-port).

    Args:
      data: If true not only edges are returned but the attributes as well
    """
    return self._g.edges_iter(data=data)

  def has_host(self, host):
    """Returns True if the host exists in the topology"""
    hid = self._host_node_id(host)
    return (self._g.has_node(hid) and
            self.is_host(hid, self.get_host_attrs(hid)))

  def has_switch(self, switch):
    """Returns True if the topology has a switch with sid"""
    sid = self._switch_node_id(switch)
    return (self._g.has_node(sid) and
            self.is_switch(sid, self.get_switch_attrs(sid)))

  def _get_attrs(self, node, ntype):
    """Returns all attributes for the node and checks it's type."""
    info = self._g.node[node]
    assert info.get('ntype', None) == ntype, \
      "There is a node with the same ID but it's not a '%s'" % ntype
    return info

  def get_host_attrs(self, host):
    """Returns all attributes for the host vertex"""
    hid = self._host_node_id(host)
    return self._get_attrs(hid, ntype=NodeType.HOST)

  def get_switch_attrs(self, switch):
    """Returns all attributes for the switch vertex"""
    sid = self._switch_node_id(switch)
    return self._get_attrs(sid, ntype=NodeType.SWITCH)

  def get_interface_attrs(self, interface):
    """Returns all attributes for the interface vertex"""
    interface_id = self._host_node_id(interface)
    return self._get_attrs(interface_id, ntype=NodeType.INTERFACE)

  def get_port_attrs(self, port):
    """Returns all attributes for the port vertex"""
    port_id = self._host_node_id(port)
    return self._get_attrs(port_id, ntype=NodeType.PORT)

  def get_host(self, host):
    """Returns the host object"""
    return self.get_host_attrs(host)['obj']

  def get_switch(self, switch):
    """Returns the switch object"""
    return self.get_switch_attrs(switch)['obj']

  def get_interface(self, interface):
    """Returns the interface object"""
    return self.get_interface_attrs(interface)['obj']

  def get_port(self, port):
    """Returns the port object"""
    return self.get_port_attrs(port)['obj']

  def _interfaces_iterator(self, host):
    """
    Takes a Host object and return list of interfaces such that
    each item is a tuple of the interface unique ID and the interface object.

    The reason for this method is to decouple reading the list of interfaces
    connected to a host from the host type. It can be written to do it
    differently for other host types
    """
    interfaces = []
    for interface in getattr(host, 'interfaces', []):
      interfaces.append((self._interface_node_id(interface), interface))
    return interfaces

  def _ports_iterator(self, switch):
    """
    Takes a switch ID and Switch object and return list of ports such that
    each item is a tuple of the port unique ID and the port object.

    The reason for this method is to decouple reading the list of ports
    connected to a switch from the switch type. It can be written to do it
    differently for other switch types
    """
    ports = []
    for port_no, port in getattr(switch, 'ports', {}).iteritems():
      vertex = port.name
      sid = self._switch_node_id(switch)
      if vertex == '' or vertex is None or str(sid) not in vertex:
        vertex = "%s-%s" % (sid, port_no)
      ports.append((vertex, port))
    return ports

  def _get_connected_edges(self, node):
    """
    Get all links that this node is connected to.
    """
    assert self._g.has_node(node), "Node  doesn't exist: '%s'" % node
    edges = []
    for src, dst in self._g.out_edges(node):
      edges.append(self._g.get_edge_data(src, dst))
    for src, dst in self._g.in_edges(node):
      edges.append(self._g.get_edge_data(src, dst))
    return edges

  def _remove_node(self, node, ntype):
    """
    Removes a node with all associated edges from the topology.

    Also removes all associated links.
    """

    assert self._g.has_node(node), \
      "Removing a node that doesn't exist: '%s'" % node
    vertex_info = self._g.node[node]
    assert vertex_info['ntype'] == ntype
    for src, dst in self._g.out_edges(node):
      self._g.remove_edge(src, dst)
    for src, dst in self._g.in_edges(node):
      self._g.remove_edge(src, dst)
    self._g.remove_node(node)

  def remove_interface(self, port_no):
    """
    Removes interface from the topology.

    Also removes all associated links.
    """
    self._remove_node(port_no, NodeType.INTERFACE)

  def remove_port(self, port_no):
    """
    Removes switch port from the topology.

    Also removes all associated links.
    """
    self._remove_node(port_no, NodeType.PORT)

  def add_host(self, host):
    """
    Add Host to the topology graph.

    Args:
      hid: Host unique ID
      host: Host object. Little assumptions are made about the host type.
            The only thing good to have is `_interfaces_iterator` works over it.
    """
    hid = self._host_node_id(host)
    self.log.debug("Adding host: %s with vertex id: %s", host, hid)
    assert not self._g.has_node(hid)
    self._g.add_node(hid, ntype=NodeType.HOST, obj=host)
    for port_no, interface in self._interfaces_iterator(host):
      self._g.add_node(port_no, ntype=NodeType.INTERFACE, obj=interface)
      self.log.debug("Adding interface: %s with node id: %s", interface,
                     port_no)
      self._g.add_edge(hid, port_no, etype=EdgeType.INTERNAL_LINK)
      self._g.add_edge(port_no, hid, etype=EdgeType.INTERNAL_LINK)
    return hid

  def remove_host(self, host):
    """
    Remove host from the topology

    Also remove all associated links
    """
    assert self.has_host(host), \
      "Removing a host that doesn't exist: '%s'" % host
    interfaces = self._interfaces_iterator(self.get_host(host))
    for port_no, _ in interfaces:
      self.remove_interface(port_no)
    hid = self._host_node_id(host)
    self._remove_node(hid, NodeType.HOST)

  def add_switch(self, switch):
    """
    Add Switch to the topology graph.

    Args:
      sid: Switch unique ID
      switch: Switch object. Little assumptions are made about the Switch type.
    """
    sid = self._switch_node_id(switch)
    assert not self._g.has_node(sid)
    self._g.add_node(sid, ntype=NodeType.SWITCH, obj=switch)
    for port_no, port in self._ports_iterator(switch):
      self._g.add_node(port_no, ntype=NodeType.PORT, obj=port)
      self._g.add_edge(sid, port_no, etype=EdgeType.INTERNAL_LINK)
      self._g.add_edge(port_no, sid, etype=EdgeType.INTERNAL_LINK)
    return sid

  def remove_switch(self, switch):
    """
    Removes a switch from the topology

    Also remove all associated links
    """
    sid = self._switch_node_id(switch)
    assert self.has_switch(switch), \
      "Removing a switch that doesn't exist: '%s'" % sid
    ports = self._ports_iterator(self._g.node[sid]['obj'])
    for port_no, _ in ports:
      self.remove_port(port_no)
    self._remove_node(sid, NodeType.SWITCH)

  def add_link(self, link, bidir=False):
    """
    Adds a Link object connecting two nodes in the network graph.

    If bidir is set to True, two edges will be added, one for each direction
    """
    src_node, dst_node = self._get_link_nodes(link)
    assert src_node is not None
    assert dst_node is not None
    assert self._g.has_node(src_node)
    assert self._g.has_node(dst_node)
    if bidir:
      self._g.add_edge(src_node, dst_node, obj=link, etype=EdgeType.LINK,
                       bidir=bidir)
      self._g.add_edge(dst_node, src_node, obj=link, etype=EdgeType.LINK,
                       bidir=bidir)
    else:
      self._g.add_edge(src_node, dst_node, obj=link, etype=EdgeType.LINK,
                       bidir=bidir)
    return link

  def get_link(self, src_node, dst_node):
    """
    Returns the Link object (if any) that is connecting two nodes in the
    network.
    """
    if not self._g.has_edge(src_node, dst_node):
      return None
    edge_attrs = self._g.get_edge_data(src_node, dst_node)
    assert self.is_link(src_node, dst_node, edge_attrs), (
        "There is an edge between '%s' and '%s' but it's"
        "not a Link" % (src_node, dst_node))
    return edge_attrs['obj']

  def has_link(self, link):
    """Returns True if there exists a link between src and dst."""
    src_vertex, dst_vertex = self._get_link_nodes(link)
    return self.get_link(src_vertex, dst_vertex) is not None

  def remove_link(self, link):
    """Removes the link between src and dst."""
    src_vertex, dst_vertex = self._get_link_nodes(link)
    assert self.has_link(link), ("Link is not part of the graph: '%s'" % link)
    bidir = self._g.get_edge_data(src_vertex, dst_vertex)['bidir']
    self._g.remove_edge(src_vertex, dst_vertex)
    # Remove the other link in case of bidir links
    if bidir and self._g.has_edge(dst_vertex, src_vertex):
      self._g.remove_edge(dst_vertex, src_vertex)

  def get_host_links(self, host):
    """
    Return set of all links connected to the host.
    """
    interfaces = self._interfaces_iterator(self.get_host(host))
    links = []
    for port_no, _ in interfaces:
      edge_attrs = self._get_connected_edges(port_no)
      for edge in edge_attrs:
        if edge['etype'] == EdgeType.LINK:
          links.append(edge['obj'])
    return links

  def get_switch_links(self, switch):
    """
    Return set of all links connected to the switch.
    """
    ports = self._ports_iterator(switch)
    links = []
    for port_no, _ in ports:
      edge_attrs = self._get_connected_edges(port_no)
      for edge in edge_attrs:
        if edge['etype'] == EdgeType.LINK:
          links.append(edge['obj'])
    return links
