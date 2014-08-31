# Copyright 2011-2013 Colin Scott
# Copyright 2011-2013 Andreas Wundsam
# Copyright 2012-2013 Sam Whitlock
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
Classes representing events to be replayed. These events can be serialized to
events.trace JSON files.

Note about the JSON events.trace format:

All events are serialized to JSON with the Event.to_json() method.

All events have a fingerprint field, which is used to compute functional
equivalence between events across different replays of the trace.

The default format of the fingerprint field is a tuple (event class name,).

The specific format of the fingerprint field is documented in each class'
fingerprint() method.

The format of other additional fields is documented in
each event's __init__() method.
"""

import itertools
import abc
import logging
import time
import marshal
import types
import json
from collections import namedtuple

from pox.lib.packet.ethernet import EthAddr
from pox.lib.util import TimeoutError
from pox.openflow.libopenflow_01 import ofp_flow_mod

from config.invariant_checks import name_to_invariant_check
from sts.dataplane_traces.trace import DataplaneEvent
from sts.fingerprints.messages import Fingerprint
from sts.fingerprints.messages import DPFingerprint
from sts.fingerprints.messages import OFFingerprint
from sts.syncproto.base import SyncTime
from sts.openflow_buffer import PendingReceive, PendingSend, OpenFlowBuffer
from sts.util.convenience import base64_decode_openflow, show_flow_tables
from sts.util.console import msg


log = logging.getLogger("events")


def dictify_fingerprint(fingerprint):
  """
  Hack: convert Fingerprint objects into Fingerprint.to_dict()
  """
  mutable = list(fingerprint)
  for i, _ in enumerate(mutable):
    if isinstance(mutable[i], Fingerprint):
      mutable[i] = mutable[i].to_dict()
  return tuple(mutable)


class Event(object):
  """Superclass for all event types."""
  __metaclass__ = abc.ABCMeta

  # Create unique labels for events
  _label_gen = itertools.count(1)
  # Ensure globally unique labels
  _all_label_ids = set()

  def __init__(self, prefix="e", label=None, logical_round=-1, event_time=None,
               dependent_labels=None, prunable=True):
    if label is None:
      label_id = Event._label_gen.next()
      label = prefix + str(label_id)
      while label_id in Event._all_label_ids:
        label_id = Event._label_gen.next()
        label = prefix + str(label_id)
    if event_time is None:
      # TODO(cs): compress time for interactive mode?
      event_time = SyncTime.now()
    self.label = label
    Event._all_label_ids.add(int(label[1:]))
    self.logical_round = logical_round
    self.event_time = event_time
    # Add on dependent labels to appease log_processing.superlog_parser.
    # TODO(cs): Replayer shouldn't depend on superlog_parser
    self.dependent_labels = dependent_labels if dependent_labels else []
    # Whether this event should be prunable by MCSFinder. Initialization
    # inputs are not pruned.
    self.prunable = prunable
    # Whether the (internal) event timed out in the most recent logical_round
    self.timed_out = False

  @property
  def label_id(self):
    """Returns the numerical id of the label"""
    return int(self.label[1:])

  @property
  def fingerprint(self):
    """
    All events must have a fingerprint. Fingerprints are used to compute
    functional equivalence.
    """
    return (self.__class__.__name__,)

  @abc.abstractmethod
  def proceed(self, simulation):
    """
    Executes a single `logical_round'. Returns a boolean that is true if the
    Replayer may continue to the next Event, otherwise proceed() again later.
    """
    pass

  def to_json(self):
    """Convert the event to json format"""
    fields = dict(self.__dict__)
    fields['class'] = self.__class__.__name__
    # fingerprints are accessed through @property, not in __dict__:
    fields['fingerprint'] = dictify_fingerprint(self.fingerprint)
    if '_fingerprint' in fields:
      del fields['_fingerprint']
    return json.dumps(fields)

  @staticmethod
  def from_json(json_hash):
    """Create the event from a json dict"""
    raise NotImplementedError()

  def __hash__(self):
    # Assumption: labels are unique
    return self.label.__hash__()

  def __eq__(self, other):
    # Assumption: labels are unique
    if type(self) != type(other):
      return False
    return self.label == other.label

  def __ne__(self, other):
    return not self.__eq__(other)

  def __str__(self):
    return self.__class__.__name__ + ":" + self.label

  def __repr__(self):
    ret_val = self.__class__.__name__ + ":" + self.label +\
              ":" + str(self.fingerprint)
    return ret_val


# -------------------------------------------------------- #
# Semi-abstract classes for internal and external events   #
# -------------------------------------------------------- #


class InternalEvent(Event):
  """
  An InternalEvent is one that happens within the controller(s) under
  simulation. Derivatives of this class verify that the internal event has
  occurred during replay in its proceed method before it returns.
  """
  __metaclass__ = abc.ABCMeta

  def __init__(self, label=None, logical_round=-1, event_time=None,
               timeout_disallowed=False, prunable=False):
    super(InternalEvent, self).__init__(prefix='i', label=label,
                                        logical_round=logical_round,
                                        event_time=event_time,
                                        prunable=prunable)
    self.timeout_disallowed = timeout_disallowed

  def whitelisted(self):
    """Should this event be always allowed."""
    return False

  def proceed(self, simulation):
    # There might be nothing happening for certain internal events, so default
    # to just doing nothing for proceed (i.e. proceeding automatically).
    pass

  def disallow_timeouts(self):
    """Is ok for this event to time out?"""
    self.timeout_disallowed = True

  @staticmethod
  def from_json(json_hash):
    """Create the event from a json dict"""
    raise NotImplementedError()


class InputEvent(Event):
  """
  An InputEvents is an event that the simulator injects into the simulation.

  Each InputEvent has a list of dependent InternalEvents that it takes in its
  constructor. This enables us to properly prune events.

  `InputEvents' may also be referred to as 'external
  events', elsewhere in documentation or code.
  """
  __metaclass__ = abc.ABCMeta

  def __init__(self, label=None, logical_round=-1, event_time=None,
               dependent_labels=None, prunable=True):
    super(InputEvent, self).__init__(prefix='e', label=label,
                                     logical_round=logical_round,
                                     event_time=event_time,
                                     dependent_labels=dependent_labels,
                                     prunable=prunable)

  @abc.abstractmethod
  def proceed(self, simulation):
    raise NotImplementedError()


# --------------------------------- #
#  Concrete classes of InputEvents  #
# --------------------------------- #

def assert_fields_exist(json_hash, *args):
  """assert that the list of fields (in args) exist in json_hash"""
  fields = args
  for field in fields:
    if field not in json_hash:
      raise ValueError("Field %s not in json_hash %s" % (field, str(json_hash)))


def extract_label_time(json_hash):
  """Extracts label, time, logical_round from json hash"""
  assert_fields_exist(json_hash, 'label', 'event_time', 'logical_round')
  label = json_hash['label']
  event_time = SyncTime(json_hash['event_time'][0], json_hash['event_time'][1])
  logical_round = json_hash['logical_round']
  return label, event_time, logical_round


class ConnectToControllers(InputEvent):
  """
  Logged at the beginning of the execution. Causes all switches to open
  TCP connections their their parent controller(s).
  """
  def __init__(self, label=None, logical_round=-1, event_time=None,
               timeout_disallowed=True):
    super(ConnectToControllers, self).__init__(label=label,
                                               logical_round=logical_round,
                                               event_time=event_time)
    self.prunable = False
    # timeout_disallowed is only for backwards compatibility
    self.timeout_disallowed = timeout_disallowed

  def proceed(self, simulation):
    simulation.connect_to_controllers()
    return True

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round, timeout_disallowed) = \
      extract_base_fields(json_hash)
    return ConnectToControllers(label=label, event_time=event_time,
                                logical_round=logical_round,
                                timeout_disallowed=timeout_disallowed)


class SwitchFailure(InputEvent):
  """
  Crashes a switch, by disconnecting its TCP connection with the controller(s).
  """
  def __init__(self, dpid, label=None, logical_round=-1, event_time=None):
    """
    Parameters:
     - dpid: unique integer identifier of the switch.
     - label: a unique label for this event. Internal event labels begin with
              'i' and input event labels begin with 'e'.
     - time: the timestamp of when this event occurred. Stored as a tuple:
       [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation
                      logical_round this event occurred in.
    """
    super(SwitchFailure, self).__init__(label=label,
                                        logical_round=logical_round,
                                        event_time=event_time)
    self.dpid = dpid

  def proceed(self, simulation):
    switch = simulation.topology.switches_manager.get_switch_dpid(self.dpid)
    log.info("SwitchFailure Event: %s", switch)
    simulation.topology.switches_manager.crash_switch(switch)
    return True

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'dpid')
    dpid = int(json_hash['dpid'])
    return SwitchFailure(dpid, label=label, logical_round=logical_round,
                         event_time=event_time)

  @property
  def fingerprint(self):
    """Fingerprint tuple format: (class name, dpid)"""
    return self.__class__.__name__, self.dpid


class SwitchRecovery(InputEvent):
  """
  Recovers a crashed switch, by reconnecting its TCP connection with the
  controller(s).
  """
  def __init__(self, dpid, label=None, logical_round=-1, event_time=None):
    """
    Parameters:
     - dpid: unique integer identifier of the switch.
     - label: a unique label for this event. Internal event labels begin with
             'i' and input event labels begin with 'e'.
     - event_time: the timestamp of when this event occurred. Stored as a tuple:
                  [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation
                      logical_round this event occurred in.
    """
    super(SwitchRecovery, self).__init__(label=label,
                                         logical_round=logical_round,
                                         event_time=event_time)
    self.dpid = dpid

  def proceed(self, simulation):
    switch = simulation.topology.switches_manager.get_switch_dpid(self.dpid)
    log.info("SwitchRecovery Event: %s", switch)
    try:
      #down_controller_ids = map(lambda c: c.cid,
      #                        simulation.controller_manager.down_controllers)

      #simulation.topology.recover_switch(software_switch,
      #                                down_controller_ids=down_controller_ids)
      simulation.topology.switches_manager.recover_switch(switch)
    except TimeoutError:
      # Controller is down... Hopefully control flow will notice soon enough
      log.warn("Timed out on %s", str(self.fingerprint))
      return False
    return True

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'dpid')
    dpid = int(json_hash['dpid'])
    return SwitchRecovery(dpid, logical_round=logical_round, label=label,
                          event_time=event_time)

  @property
  def fingerprint(self):
    """Fingerprint tuple format: (class name, dpid)"""
    return self.__class__.__name__, self.dpid


def get_link(link_event, simulation):
  """
  Get the link object that the link event is related to.
  """
  sw_mgm = simulation.topology.switches_manager
  patch_panel = simulation.topology.patch_panel
  start_software_switch = sw_mgm.get_switch_dpid(link_event.start_dpid)
  end_software_switch = sw_mgm.get_switch_dpid(link_event.end_dpid)
  start_port = start_software_switch.ports[link_event.start_port_no]
  end_port = end_software_switch.ports[link_event.end_port_no]
  links = patch_panel.query_network_links(start_software_switch, start_port,
                                          end_software_switch, end_port)
  return links.pop()


class LinkFailure(InputEvent):
  """
  Cuts a link between switches. This causes the switch to send an
  ofp_port_status message to its parent(s). All packets forwarded over
  this link will be dropped until a LinkRecovery occurs.
  """
  def __init__(self, start_dpid, start_port_no, end_dpid, end_port_no,
               label=None, logical_round=-1, event_time=None):
    """
    Parameters:
     - start_dpid: unique integer identifier of the first switch connected to
                   the link.
     - start_port_no: integer port number of the start switch's port.
     - end_dpid: unique integer identifier of the second switch connected to
                 the link.
     - end_port_no: integer port number of the end switch's port to be created.
     - label: a unique label for this event. Internal event labels begin
              with 'i' and input event labels begin with 'e'.
     - event_time: the timestamp of when this event occurred. Stored as a tuple:
                  [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
    """
    super(LinkFailure, self).__init__(label=label, logical_round=logical_round,
                                      event_time=event_time)
    self.start_dpid = start_dpid
    self.start_port_no = start_port_no
    self.end_dpid = end_dpid
    self.end_port_no = end_port_no

  def proceed(self, simulation):
    link = get_link(self, simulation)
    log.info("LinkFailure Event: %s", link)
    return simulation.topology.patch_panel.sever_network_link(link)

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'start_dpid', 'start_port_no', 'end_dpid',
                        'end_port_no')
    start_dpid = int(json_hash['start_dpid'])
    start_port_no = int(json_hash['start_port_no'])
    end_dpid = int(json_hash['end_dpid'])
    end_port_no = int(json_hash['end_port_no'])
    return LinkFailure(start_dpid, start_port_no, end_dpid, end_port_no,
                       logical_round=logical_round, label=label,
                       event_time=event_time)

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format:
      (class name, start dpid, start port_no, end dpid, end port_no)
    """
    return (self.__class__.__name__,
            self.start_dpid, self.start_port_no,
            self.end_dpid, self.end_port_no)


class LinkRecovery(InputEvent):
  """
  Recovers a failed link between switches. This causes the switch to send an
  ofp_port_status message to its parent(s).
  """
  def __init__(self, start_dpid, start_port_no, end_dpid, end_port_no,
               label=None, logical_round=-1, event_time=None):
    """
    Parameters:
     - start_dpid: unique integer identifier of the first switch connected to
                   the link.
     - start_port_no: integer port number of the start switch's port.
     - end_dpid: unique integer identifier of the second switch connected to the
                 link.
     - end_port_no: integer port number of the end switch's port to be created.
     - label: a unique label for this event. Internal event labels begin
              with 'i' and input event labels begin with 'e'.
     - event_time: the timestamp of when this event occurred. Stored as a tuple:
                  [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation
                      logical_round this event occurred in.
    """
    super(LinkRecovery, self).__init__(label=label, logical_round=logical_round,
                                       event_time=event_time)
    self.start_dpid = start_dpid
    self.start_port_no = start_port_no
    self.end_dpid = end_dpid
    self.end_port_no = end_port_no

  def proceed(self, simulation):
    link = get_link(self, simulation)
    log.info("LinkRecovery Event: %s", link)
    simulation.topology.patch_panel.repair_network_link(link)
    return True

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'start_dpid', 'start_port_no', 'end_dpid',
                        'end_port_no')
    start_dpid = int(json_hash['start_dpid'])
    start_port_no = int(json_hash['start_port_no'])
    end_dpid = int(json_hash['end_dpid'])
    end_port_no = int(json_hash['end_port_no'])
    return LinkRecovery(start_dpid, start_port_no, end_dpid, end_port_no,
                        logical_round=logical_round, label=label,
                        event_time=event_time)

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format:
      (class name, start dpid, start port, end dpid, end port)
    """
    return (self.__class__.__name__,
            self.start_dpid, self.start_port_no,
            self.end_dpid, self.end_port_no)


class ControllerFailure(InputEvent):
  """Kills a controller process with `kill -9`"""
  def __init__(self, controller_id, label=None, logical_round=-1,
               event_time=None):
    """
    Parameters:
     - controller_id: unique string label for the controller to be killed.
     - label: a unique label for this event. Internal event labels begin
              with 'i' and input event labels begin with 'e'.
     - time: the timestamp of when this event occurred. Stored as a tuple:
       [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
    """
    super(ControllerFailure, self).__init__(label=label,
                                            logical_round=logical_round,
                                            event_time=event_time)
    self.controller_id = controller_id

  def proceed(self, simulation):
    c_mgm = simulation.topology.controllers_manager
    controller = c_mgm.get_controller(self.controller_id)
    log.info("ControllerFailure Event: %s", controller)
    simulation.topology.controllers_manager.crash_controller(controller)
    return True

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist('controller_id')
    controller_id = json_hash['controller_id']
    return ControllerFailure(controller_id, logical_round=logical_round,
                             label=label, event_time=event_time)

  @property
  def fingerprint(self):
    """Fingerprint tuple format: (class name, controller id)"""
    return self.__class__.__name__, self.controller_id


class ControllerRecovery(InputEvent):
  """
  Reboots a crashed controller by re-invoking its original command line
  parameters.
  """
  def __init__(self, controller_id, label=None, logical_round=-1,
               event_time=None):
    """
    Parameters:
     - controller_id: unique string label for the controller.
     - label: a unique label for this event. Internal event labels begin
              with 'i' and input event labels begin with 'e'.
     - time: the timestamp of when this event occurred. Stored as a tuple:
       [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation
                      logical_round this event occurred in.
    """
    super(ControllerRecovery, self).__init__(label=label,
                                             logical_round=logical_round,
                                             event_time=event_time)
    self.controller_id = controller_id

  def proceed(self, simulation):
    c_mgm = simulation.topology.controllers_manager
    controller = c_mgm.get_controller(self.controller_id)
    log.info("ControllerRecovery Event: %s", controller)
    simulation.topology.controllers_manager.recover_controller(controller)
    return True

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist('controller_id')
    controller_id = json_hash['controller_id']
    return ControllerRecovery(controller_id, logical_round=logical_round,
                              label=label, event_time=event_time)

  @property
  def fingerprint(self):
    """Fingerprint tuple format: (class name, controller id)."""
    return self.__class__.__name__, self.controller_id


class HostMigration(InputEvent):
  """
  Migrates a host from one location in network to another. Creates a new
  virtual port on the new switch, and takes down the old port on the old switch.
  """
  def __init__(self, old_ingress_dpid, old_ingress_port_no,
               new_ingress_dpid, new_ingress_port_no, host_id, label=None,
               logical_round=-1, event_time=None):
    """
    Parameters:
     - old_ingress_dpid: unique integer identifier of the ingress switch the
                          host is moving away from.
     - old_ingress_port_no: integer identifier of the port the host is moving
       away from.
     - new_ingress_dpid: unique integer identifier of the ingress switch the
                         host is moving to.
     - new_ingress_port_no: integer identifier of the port the host is moving
       to.
     - host_id: unique integer identifier of the host.
     - label: a unique label for this event. Internal event labels begin with
            'i' and input event labels begin with 'e'.
     - time: the timestamp of when this event occurred. Stored as a tuple:
            [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
    """
    super(HostMigration, self).__init__(label=label,
                                        logical_round=logical_round,
                                        event_time=event_time)
    self.old_ingress_dpid = old_ingress_dpid
    self.old_ingress_port_no = old_ingress_port_no
    self.new_ingress_dpid = new_ingress_dpid
    self.new_ingress_port_no = new_ingress_port_no
    self.host_id = host_id

  def proceed(self, simulation):
    simulation.topology.migrate_host(self.old_ingress_dpid,
                                     self.old_ingress_port_no,
                                     self.new_ingress_dpid,
                                     self.new_ingress_port_no)
    return True

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'old_ingress_dpid', 'old_ingress_port_no',
                        'new_ingress_dpid', 'new_ingress_port_no', 'host_id')
    old_ingress_dpid = int(json_hash['old_ingress_dpid'])
    old_ingress_port_no = int(json_hash['old_ingress_port_no'])
    new_ingress_dpid = int(json_hash['new_ingress_dpid'])
    new_ingress_port_no = int(json_hash['new_ingress_port_no'])
    host_id = json_hash['host_id']
    return HostMigration(old_ingress_dpid, old_ingress_port_no,
                         new_ingress_dpid, new_ingress_port_no,
                         host_id, logical_round=logical_round, label=label,
                         event_time=event_time)

  @property
  def old_location(self):
    """Switch, port pair of the old host location"""
    return self.old_ingress_dpid, self.old_ingress_port_no

  @property
  def new_location(self):
    """Switch, port pair of the new host location"""
    return self.new_ingress_dpid, self.new_ingress_port_no

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format:
    (class name, old dpid, old port, new dpid, new port, host id)
    """
    return (self.__class__.__name__, self.old_ingress_dpid,
            self.old_ingress_port_no, self.new_ingress_dpid,
            self.new_ingress_port_no, self.host_id)

  def pretty_print_fingerprint(self):
    """Just prettier version of finger print."""
    return ("[h%d]: (s%d,p%d) -> (s%d,p%d)" %
            (self.host_id, self.old_ingress_dpid, self.old_ingress_port_no,
             self.new_ingress_dpid, self.new_ingress_port_no))


class PolicyChange(InputEvent):
  """
  Policy changes induced by the user.
  """
  def __init__(self, request_type, label=None, logical_round=-1,
               event_time=None):
    super(PolicyChange, self).__init__(label=label, logical_round=logical_round,
                                       event_time=event_time)
    self.request_type = request_type

  def proceed(self, simulation):
    # TODO(cs): implement me, and add PolicyChanges to Fuzzer
    pass

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'request_type')
    request_type = json_hash['request_type']
    return PolicyChange(request_type, logical_round=logical_round, label=label,
                        event_time=event_time)


class TrafficInjection(InputEvent):
  """
  Injects a dataplane packet into the network at the given host's access link
  """
  def __init__(self, label=None, dp_event=None, host_id=None, logical_round=-1,
               event_time=None, prunable=True):
    """
    Parameters:
     - dp_event: DataplaneEvent object encapsulating the packet contents and the
       access link.
     - host_id: unique integer label identifying the host that generated the
       packet.
     - label: a unique label for this event. Internal event labels begin with
             'i' and input event labels begin with 'e'.
     - event_time: the timestamp of when this event occurred. Stored as a tuple:
                  [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
     - prunable: whether this input event can be pruned during delta
       debugging.
    """
    super(TrafficInjection, self).__init__(label=label,
                                           logical_round=logical_round,
                                           event_time=event_time,
                                           prunable=prunable)
    self.dp_event = dp_event
    self.host_id = host_id

  def proceed(self, simulation):
    # If dp_event is None, backwards compatibility
    if self.dp_event is None:
      if simulation.dataplane_trace is None:
        raise RuntimeError("No dataplane trace specified!")
      simulation.dataplane_trace.inject_trace_event()
    else:
      patch_panel = simulation.topology.patch_panel
      host = patch_panel.interface2access_link[self.dp_event.interface].host
      host.send(self.dp_event.interface, self.dp_event.packet)
    return True

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format: (class name, dp event, host_id)
    The format of dp event is:
    {"interface": HostInterface.to_json(),
     "packet": base 64 encoded packet contents}
    See entities.py for the HostInterface json format.
    """
    return self.__class__.__name__, self.dp_event, self.host_id

  def to_json(self):
    fields = {}
    fields = dict(self.__dict__)
    fields['class'] = self.__class__.__name__
    fields['dp_event'] = self.dp_event.to_json()
    fields['fingerprint'] = (self.__class__.__name__, self.dp_event.to_json(),
                             self.host_id)
    fields['host_id'] = self.host_id
    return json.dumps(fields)

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    prunable = True
    if 'prunable' in json_hash:
      prunable = json_hash['prunable']
    dp_event = None
    if 'dp_event' in json_hash:
      dp_event = DataplaneEvent.from_json(json_hash['dp_event'])
    host_id = None
    if 'host_id' in json_hash:
      host_id = json_hash['host_id']
    return TrafficInjection(label=label, dp_event=dp_event, host_id=host_id,
                            event_time=event_time, logical_round=logical_round,
                            prunable=prunable)


class WaitTime(InputEvent):
  """
  Causes the simulation to sleep for the specified number of seconds.

  Controller processes continue running during this time.
  """
  def __init__(self, wait_time, label=None, logical_round=-1, event_time=None):
    """
    Parameters:
     - wait_time: float representing how long to sleep in seconds.
     - label: a unique label for this event. Internal event labels begin with
            'i' and input event labels begin with 'e'.
     - time: the timestamp of when this event occurred. Stored as a tuple:
             [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
    """
    super(WaitTime, self).__init__(label=label, logical_round=logical_round,
                                   event_time=event_time)
    self.wait_time = wait_time

  def proceed(self, simulation):
    log.info("WaitTime: pausing simulation for %f seconds", (self.wait_time))
    time.sleep(self.wait_time)
    return True

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'wait_time')
    wait_time = json_hash['wait_time']
    return WaitTime(wait_time, logical_round=logical_round, label=label,
                    event_time=event_time)


class CheckInvariants(InputEvent):
  """
  Causes the simulation to pause itself and check the given invariant before
  proceeding.
  """
  def __init__(self, label=None, logical_round=-1, event_time=None,
               invariant_check_name="InvariantChecker.check_correspondence"):
    """
    Parameters:
     - label: a unique label for this event. Internal event labels begin with
             'i' and input event labels begin with 'e'.
     - time: the timestamp of when this event occurred. Stored as a tuple:
       [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
     - invariant_check_name: unique name of the invariant to be checked. See
       config.invariant_checks for an exhaustive list of possible invariant
       checks.
    """
    super(CheckInvariants, self).__init__(label=label,
                                          logical_round=logical_round,
                                          event_time=event_time)
    # For backwards compatibility.. (invariants used to be specified as
    # marshalled functions, not invariant check names)
    self.legacy_invariant_check = not isinstance(invariant_check_name,
                                                 basestring)
    if self.legacy_invariant_check:
      self.invariant_check = invariant_check_name
    else:
      # Otherwise, invariant check is specified as a name
      self.invariant_check_name = invariant_check_name
      if invariant_check_name not in name_to_invariant_check:
        raise ValueError("Unknown invariant check %s.\n"
                         "Invariant check name must be defined in "
                         "config.invariant_checks" % invariant_check_name)
      self.invariant_check = name_to_invariant_check[invariant_check_name]

  def proceed(self, simulation):
    # TODO (AH): Move the tracking part to event exec
    try:
      violations = self.invariant_check(simulation)
      simulation.violation_tracker.track(violations, self.logical_round)
      persistent_violations = simulation.violation_tracker.persistent_violations
    except NameError as exp:
      raise ValueError("Closures are unsupported for invariant check "
                       "functions.\n Use dynamic imports inside of your "
                       "invariant check code and define all globals "
                       "locally.\n NameError: %s" % str(exp))
    ret_value = violations == []
    if violations != []:
      msg.fail("The following correctness violations"
               "have occurred: %s, Check name: %s" %
               (str(violations), self.invariant_check_name))
      fail_interactive = hasattr(simulation, "fail_to_interactive")
      if fail_interactive and simulation.fail_to_interactive:
        raise KeyboardInterrupt("fail to interactive")
    else:
      msg.success("No correctness violations!")
    if persistent_violations != []:
      msg.fail("Persistent violations detected!: %s" %
               str(persistent_violations))
      if (hasattr(simulation, "fail_to_interactive_on_persistent_violations")
          and simulation.fail_to_interactive_on_persistent_violations):
        raise KeyboardInterrupt("fail to interactive on persistent violation")
      return ret_value
    return ret_value

  def to_json(self):
    fields = dict(self.__dict__)
    fields['class'] = self.__class__.__name__
    if self.legacy_invariant_check:
      dump = marshal.dumps(self.invariant_check.func_code)
      fields['invariant_check'] = dump.encode('base64')
      fields['invariant_name'] = self.invariant_check.__name__
    else:
      fields['invariant_name'] = self.invariant_check_name
      fields['invariant_check'] = None
    fields['fingerprint'] = "N/A"
    return json.dumps(fields)

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    invariant_check_name = "InvariantChecker.check_connectivity"
    if 'invariant_name' in json_hash:
      invariant_check_name = json_hash['invariant_name']
    elif 'invariant_check' in json_hash:
      # Legacy code (marshalled function)
      # Assumes that the closure is empty
      code = marshal.loads(json_hash['invariant_check'].decode('base64'))
      invariant_check_name = types.FunctionType(code, globals())

    return CheckInvariants(label=label, event_time=event_time,
                           logical_round=logical_round,
                           invariant_check_name=invariant_check_name)


class ControlChannelBlock(InputEvent):
  """
  Simulates delay between switches and controllers by temporarily
  queuing all messages sent on the switch<->controller TCP connection. No
  messages will be sent over the connection until a ControlChannelUnblock
  occurs.
  """
  def __init__(self, dpid, controller_id, label=None, logical_round=-1,
               event_time=None):
    """
    Parameters:
     - dpid: unique integer identifier of the switch.
     - controller_id: unique string label for the controller.
     - label: a unique label for this event. Internal event labels begin with
              'i' and input event labels begin with 'e'.
     - time: the timestamp of when this event occurred. Stored as a tuple:
            [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
    """
    super(ControlChannelBlock, self).__init__(label=label,
                                              logical_round=logical_round,
                                              event_time=event_time)
    self.dpid = dpid
    self.controller_id = controller_id

  def proceed(self, simulation):
    switch = simulation.topology.get_switch(self.dpid)
    connection = switch.get_connection(self.controller_id)
    if connection.io_worker.currently_blocked:
    #raise RuntimeError("Expected channel %s to not be blocked" %
    # str(connection))
      return True
    connection.io_worker.block()
    return True

  @property
  def fingerprint(self):
    """Fingerprint tuple format: (class name, dpid, controller id)"""
    return self.__class__.__name__, self.dpid, self.controller_id

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'dpid', 'controller_id')
    dpid = json_hash['dpid']
    controller_id = json_hash['controller_id']
    return ControlChannelBlock(dpid, controller_id, logical_round=logical_round,
                               label=label, event_time=event_time)


class ControlChannelUnblock(InputEvent):
  """
  Unblocks the control channel delay triggered by a ControlChannelUnblock.
  All queued messages will be sent.
  """
  def __init__(self, dpid, controller_id, label=None, logical_round=-1,
               event_time=None):
    """
    Parameters:
     - dpid: unique integer identifier of the switch.
     - controller_id: unique string label for the controller.
     - label: a unique label for this event. Internal event labels begin with
             'i' and input event labels begin with 'e'.
     - event_time: the timestamp of when this event occurred. Stored as a tuple:
                  [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation
                      logical_round this event occurred in.
    """
    super(ControlChannelUnblock, self).__init__(label=label,
                                                logical_round=logical_round,
                                                event_time=event_time)
    self.dpid = dpid
    self.controller_id = controller_id

  def proceed(self, simulation):
    switch = simulation.topology.get_switch(self.dpid)
    connection = switch.get_connection(self.controller_id)
    if not connection.io_worker.currently_blocked:
    #  raise RuntimeError("Expected channel %s to be blocked" % str(connection))
      return True
    connection.io_worker.unblock()
    return True

  @property
  def fingerprint(self):
    """Fingerprint tuple format: (class name, dpid, controller id)"""
    return self.__class__.__name__, self.dpid, self.controller_id

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'dpid', 'controller_id')
    dpid = json_hash['dpid']
    controller_id = json_hash['controller_id']
    return ControlChannelUnblock(dpid, controller_id,
                                 logical_round=logical_round, label=label,
                                 event_time=event_time)


class DataplaneDrop(InputEvent):
  """
  Removes an in-flight dataplane packet with the given fingerprint from
  the network.
  """
  def __init__(self, fingerprint, label=None, host_id=None, dpid=None,
               logical_round=-1, event_time=None, passive=True):
    """
    Parameters:
     - label: a unique label for this event. Internal event labels begin with
             'i' and input event labels begin with 'e'.
     - host_id: unique integer label identifying the host that generated the
       packet. May be None.
     - dpid: unique integer identifier of the switch. May be None.
     - event_time: the timestamp of when this event occurred. Stored as a tuple:
                  [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation
                      logical_round this event occurred in.
     - passive: whether we're using Replayer.DataplaneChecker
    """
    super(DataplaneDrop, self).__init__(label=label,
                                        logical_round=logical_round,
                                        event_time=event_time)
    # N.B. fingerprint is monkeypatched on to DpPacketOut events by
    # BufferedPatchPanel
    if fingerprint[0] != self.__class__.__name__:
      fingerprint = list(fingerprint)
      fingerprint.insert(0, self.__class__.__name__)
    if type(fingerprint) == list:
      fingerprint = (fingerprint[0], DPFingerprint(fingerprint[1]),
                     fingerprint[2], fingerprint[3])
    self._fingerprint = fingerprint
    # TODO(cs): passive is a bit of a hack, but this was easier.
    self.passive = passive
    self.host_id = host_id
    self.dpid = dpid

  def proceed(self, simulation):
    # Handled by control_flow.replayer.DataplaneChecker
    if self.passive:
      return True
    else:
      fprint = self.fingerprint[1:]
      dp_event = simulation.patch_panel.get_buffered_dp_event(fprint)
      if dp_event is not None:
        simulation.patch_panel.drop_dp_event(dp_event)
        return True
    return False

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format:
    (class name, DPFingerprint, switch dpid, port no)
    See fingerprints/messages.py for format of DPFingerprint.
    """
    return self._fingerprint

  @property
  def dp_fingerprint(self):
    """Data path packet fingerprint"""
    return self.fingerprint[1]

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'fingerprint')
    fingerprint = json_hash['fingerprint']
    return DataplaneDrop(fingerprint, logical_round=logical_round, label=label,
                         event_time=event_time)

  def to_json(self):
    fields = dict(self.__dict__)
    fields['class'] = self.__class__.__name__
    fields['fingerprint'] = (self.fingerprint[0], self.fingerprint[1].to_dict(),
                             self.fingerprint[2], self.fingerprint[3])
    del fields['_fingerprint']
    return json.dumps(fields)


class BlockControllerPair(InputEvent):
  """Blocks connectivity between two controllers."""
  def __init__(self, cid1, cid2, label=None, logical_round=-1, event_time=None):
    super(BlockControllerPair, self).__init__(label=label,
                                              logical_round=logical_round,
                                              event_time=event_time)
    self.cid1 = cid1
    self.cid2 = cid2

  def proceed(self, simulation):
    # if there is a controller patch panel configured, us it, otherwise use
    # iptables.
    #if simulation.controller_patch_panel is not None:
    #  c_patch_panel = simulation.controller_patch_panel
    #  c_patch_panel.block_controller_pair(self.cid1, self.cid2)
    #else:
    c_mgm = simulation.topology.controllers_manager
    ctrl1, ctrl2 = [c_mgm.get_controller(cid) for cid in [self.cid1, self.cid2]]
    c_mgm.block_peers(ctrl1, ctrl2)
    return True

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format:
    (class name, cid1, cid2)
    """
    return self.__class__.__name__, self.cid1, self.cid2

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'cid1', 'cid2')
    cid1 = json_hash['cid1']
    cid2 = json_hash['cid2']
    return BlockControllerPair(cid1, cid2, logical_round=logical_round,
                               label=label, event_time=event_time)


class UnblockControllerPair(InputEvent):
  """Unblocks connectivity between two controllers."""
  def __init__(self, cid1, cid2, label=None, logical_round=-1, event_time=None):
    super(UnblockControllerPair, self).__init__(label=label,
                                                logical_round=logical_round,
                                                event_time=event_time)
    self.cid1 = cid1
    self.cid2 = cid2

  def proceed(self, simulation):
    # if there is a controller patch panel configured, us it, otherwise use
    # iptables.
    #if simulation.controller_patch_panel is not None:
    #  c_patch_panel = simulation.controller_patch_panel
    #  c_patch_panel.unblock_controller_pair(self.cid1, self.cid2)
    #else:
    c_mgm = simulation.topology.controllers_manager
    ctrl1, ctrl2 = [c_mgm.get_controller(cid) for cid in [self.cid1, self.cid2]]
    c_mgm.unblock_peers(ctrl1, ctrl2)
    return True

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format:
      (class name, cid1, cid2)
    """
    return self.__class__.__name__, self.cid1, self.cid2

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'cid1', 'cid2')
    cid1 = json_hash['cid1']
    cid2 = json_hash['cid2']
    return UnblockControllerPair(cid1, cid2, logical_round=logical_round,
                                 label=label, event_time=event_time)


# TODO(cs): Temporary hack until we figure out determinism
class LinkDiscovery(InputEvent):
  """Deprecated"""
  def __init__(self, controller_id, link_attrs, label=None, logical_round=-1,
               event_time=None):
    super(LinkDiscovery, self).__init__(label=label,
                                        logical_round=logical_round,
                                        event_time=event_time)
    self._fingerprint = (self.__class__.__name__,
                         controller_id, tuple(link_attrs))
    self.controller_id = controller_id
    self.link_attrs = link_attrs

  def proceed(self, simulation):
    c_mgm = simulation.controllers_manager
    controller = c_mgm.get_controller(self.controller_id)
    controller.sync_connection.send_link_notification(self.link_attrs)
    return True

  @property
  def fingerprint(self):
    return self._fingerprint

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'controller_id', 'link_attrs')
    controller_id = json_hash['controller_id']
    link_attrs = json_hash['link_attrs']
    return LinkDiscovery(controller_id, link_attrs, logical_round=logical_round,
                         label=label, event_time=event_time)


class NOPInput(InputEvent):
  """Does nothing. Useful for fenceposting."""
  def proceed(self, simulation):
    return True

  @property
  def fingerprint(self):
    return self.__class__.__name__,

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    return NOPInput(logical_round=logical_round, label=label,
                    event_time=event_time)


class AddIntent(PolicyChange):
  """Add Intent, currently ONOS specific"""
  def __init__(self, cid, intent_id, src_dpid, dst_dpid, src_port, dst_port,
               src_mac, dst_mac, static_path, intent_type, intent_ip,
               intent_port, intent_url, label=None, logical_round=-1,
               event_time=None):
    super(AddIntent, self).__init__(request_type='AddIntent', label=label,
                                    logical_round=logical_round,
                                    event_time=event_time)
    self.cid = cid
    self.intent_id = intent_id
    self.src_dpid = src_dpid
    self.dst_dpid = dst_dpid
    self.src_port = src_port
    self.dst_port = dst_port
    self.src_mac = src_mac if isinstance(src_mac, basestring) else str(src_mac)
    self.dst_mac = dst_mac if isinstance(dst_mac, basestring) else str(dst_mac)
    self.static_path = static_path
    self.intent_type = intent_type
    self.intent_ip = intent_ip
    self.intent_port = intent_port
    self.intent_url = intent_url

  def proceed(self, simulation):
    c_mgm = simulation.topology.controllers_manager
    controller = c_mgm.get_controller(self.cid)
    intent = dict()
    intent['intent_id'] = self.intent_id
    intent['src_dpid'] = self.src_dpid
    intent['dst_dpid'] = self.dst_dpid
    intent['src_port'] = self.src_port
    intent['dst_port'] = self.dst_port
    intent['src_mac'] = self.src_mac
    intent['dst_mac'] = self.dst_mac
    intent['static_path'] = self.static_path
    intent['intent_type'] = self.intent_type
    intent['intentIP'] = self.intent_ip
    intent['intentPort'] = self.intent_port
    intent['intentURL'] = self.intent_url
    log.info("Adding intent: %s", intent)
    ret = controller.add_intent(intent)
    # Skip adding the hosts as connected if the add intent failed
    if not ret:
      return ret
    # Add policy to connectivity tracker
    src_host = None
    dst_host = None
    src_iface = None
    dst_iface = None
    for host in simulation.topology.hosts_manager.hosts:
      for iface in host.interfaces:
        if iface.hw_addr == EthAddr(self.src_mac):
          src_host = host
          src_iface = iface
        if iface.hw_addr == EthAddr(self.dst_mac):
          dst_host = host
          dst_iface = iface
        # Just an early termination
        if src_host is not None and dst_host is not None:
          break
    track = simulation.topology.connectivity_tracker
    log.debug("Adding hosts to connected hosts: src=%s, src_iface=%s dst=%s, "
              "dst_iface=%s", src_host, src_iface, dst_host, dst_iface)
    track.add_connected_hosts(src_host, src_iface, dst_host, dst_iface,
                              self.intent_id)
    return ret

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'request_type', 'cid', 'intent_id',
                        'src_dpid', 'dst_dpid', 'src_port', 'dst_port',
                        'src_mac', 'dst_mac', 'static_path', 'intent_type',
                        'intent_ip', 'intent_port', 'intent_url')
    cid = json_hash['cid']
    intent_id = json_hash['intent_id']
    src_dpid = json_hash['src_dpid']
    dst_dpid = json_hash['dst_dpid']
    src_port = json_hash['src_port']
    dst_port = json_hash['dst_port']
    src_mac = json_hash['src_mac']
    dst_mac = json_hash['dst_mac']
    static_path = json_hash['static_path']
    intent_type = json_hash['intent_type']
    intent_ip = json_hash['intent_ip']
    intent_port = json_hash['intent_port']
    intent_url = json_hash['intent_url']
    return AddIntent(cid=cid, intent_id=intent_id, src_dpid=src_dpid,
                     dst_dpid=dst_dpid, src_port=src_port, dst_port=dst_port,
                     src_mac=src_mac, dst_mac=dst_mac, static_path=static_path,
                     intent_type=intent_type, intent_ip=intent_ip,
                     intent_port=intent_port, intent_url=intent_url,
                     logical_round=logical_round, label=label,
                     event_time=event_time)

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format: (class name, cid, intent_id, src_dpid,
            dst_dpid, src_port, dst_port, src_mac, dst_mac, static_path,
            intent_type, intent_ip, intent_port, intent_url)
    """
    return (self.__class__.__name__, self.cid, self.intent_id, self.src_dpid,
            self.dst_dpid, self.src_port, self.dst_port, self.src_mac,
            self.dst_mac, self.static_path, self.intent_type, self.intent_ip,
            self.intent_port, self.intent_url)


class RemoveIntent(PolicyChange):
  """Remove Intent, currently ONOS specific"""
  def __init__(self, cid, intent_id, intent_ip, intent_port, intent_url,
               label=None, logical_round=-1, event_time=None):
    super(RemoveIntent, self).__init__(request_type='RemoveIntent', label=label,
                                       logical_round=logical_round,
                                       event_time=event_time)
    self.cid = cid
    self.intent_id = intent_id
    self.intent_ip = intent_ip
    self.intent_port = intent_port
    self.intent_url = intent_url

  def proceed(self, simulation):
    cid = self.cid
    controller = simulation.topology.controllers_manager.get_controller(cid)
    log.info("Removing intent: %s", self.intent_id)
    ret = controller.remove_intent(intent_id=self.intent_id,
                                   intent_ip=self.intent_ip,
                                   intent_port=self.intent_port,
                                   intent_url=self.intent_url)
    if ret:
      simulation.topology.connectivity_tracker.remove_policy(self.intent_id)
    return ret

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'request_type', 'cid', 'intent_id',
                        'intent_ip', 'intent_port', 'intent_url')
    cid = json_hash['cid']
    intent_id = json_hash['intent_id']
    intent_ip = json_hash['intent_ip']
    intent_port = json_hash['intent_port']
    intent_url = json_hash['intent_url']
    return RemoveIntent(cid=cid, intent_id=intent_id, intent_ip=intent_ip,
                        intent_port=intent_port, intent_url=intent_url,
                        logical_round=logical_round, label=label,
                        event_time=event_time)

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format: (class name, cid, intent_id, intent_ip,
                              intent_port, intent_url)
    """
    return (self.__class__.__name__, self.cid, self.intent_id, self.intent_ip,
            self.intent_port, self.intent_url)


class PingEvent(InputEvent):
  """TestON specific event to ping between two hosts"""
  def __init__(self, src_host_id, dst_host_id,
               label=None, logical_round=-1, event_time=None, prunable=False):
    super(PingEvent, self).__init__(label=label, logical_round=logical_round,
                                    event_time=event_time,
                                    prunable=prunable)
    self.src_host_id = src_host_id
    self.dst_host_id = dst_host_id

  def proceed(self, simulation):
    src_host = simulation.topology.hosts_manager.get_host(self.src_host_id)
    dst_host = simulation.topology.hosts_manager.get_host(self.dst_host_id)
    return src_host.ping(dst_host)

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format: (class name, src_host_id, dst_hosts_id)
    """
    return self.__class__.__name__, self.src_host_id, self.dst_host_id

  def to_json(self):
    fields = {}
    fields = dict(self.__dict__)
    fields['class'] = self.__class__.__name__
    fields['src_host_id'] = self.src_host_id
    fields['dst_host_id'] = self.dst_host_id
    fields['fingerprint'] = self.fingerprint
    return json.dumps(fields)

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    prunable = True
    if 'prunable' in json_hash:
      prunable = json_hash['prunable']
    src_host_id = json_hash['src_host_id']
    dst_host_id = json_hash['dst_host_id']
    return PingEvent(src_host_id=src_host_id, dst_host_id=dst_host_id,
                     label=label, event_time=event_time,
                     logical_round=logical_round, prunable=prunable)


# N.B. When adding inputs to this list, make sure to update input susequence
# validity checking in event_dag.py.
all_input_events = [SwitchFailure, SwitchRecovery, LinkFailure, LinkRecovery,
                    ControllerFailure, ControllerRecovery, HostMigration,
                    PolicyChange, TrafficInjection, WaitTime, CheckInvariants,
                    ControlChannelBlock, ControlChannelUnblock,
                    DataplaneDrop, BlockControllerPair, UnblockControllerPair,
                    LinkDiscovery, ConnectToControllers, NOPInput, AddIntent,
                    RemoveIntent, PingEvent]


# ----------------------------------- #
#  Concrete classes of InternalEvents #
# ----------------------------------- #

def extract_base_fields(json_hash):
  """Extract label, event_time and logical_round"""
  (label, event_time, logical_round) = extract_label_time(json_hash)
  timeout_disallowed = False
  if 'timeout_disallowed' in json_hash:
    timeout_disallowed = json_hash['timeout_disallowed']
  return label, event_time, logical_round, timeout_disallowed


class ControlMessageBase(InternalEvent):
  """
  Logged whenever an OpenFlowBuffer decides to explicitly fail an OpenFlow
  packet, or allow a switch to receive or send an openflow packet.
  """

  def __init__(self, dpid, controller_id, fingerprint, b64_packet="",
               label=None, logical_round=-1, event_time=None,
               timeout_disallowed=False):
    """
    Parameters:
     - dpid: unique integer identifier of the switch.
     - controller_id: unique string label for the controller.
     - b64_packet: base64 encoded packed openflow message.
     - label: a unique label for this event. Internal event labels begin with
              'i' and input event labels begin with 'e'.
     - event_time: the timestamp of when this event occurred. Stored as a tuple:
                  [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
     - timeout_disallowed: whether the replayer should wait indefinitely for
       this event to occur. Defaults to False.
    """
    # If constructed directly (not from json), fingerprint is the
    # OFFingerprint, not including dpid and controller_id
    super(ControlMessageBase, self).__init__(label=label,
                                             logical_round=logical_round,
                                             event_time=event_time,
                                             timeout_disallowed=timeout_disallowed)
    self.dpid = dpid
    self.controller_id = controller_id
    self.b64_packet = b64_packet
    if type(fingerprint) == list:
      fingerprint = (fingerprint[0], OFFingerprint(fingerprint[1]),
                     fingerprint[2], tuple(fingerprint[3]))
    if type(fingerprint) == dict or type(fingerprint) != tuple:
      fingerprint = (self.__class__.__name__, OFFingerprint(fingerprint),
                     dpid, controller_id)

    self._fingerprint = fingerprint
    self.ignore_whitelisted_packets = False
    self.pass_through_sends = False

  def get_packet(self):
    # Avoid serialization exceptions, but we still want to memoize.
    if not hasattr(self, "_packet"):
      self._packet = base64_decode_openflow(self.b64_packet)
    return self._packet

  def to_json(self):
    if hasattr(self, "_packet"):
      delattr(self, "_packet")
    return super(ControlMessageBase, self).to_json()

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format:
      (class name, OFFingerprint, dpid, controller id)
    See fingerprints/messages.py for OFFingerprint format.
    """
    return self._fingerprint

  @staticmethod
  def from_json(json_hash):
    raise NotImplementedError()


class ControlMessageReceive(ControlMessageBase):
  """
  Logged whenever the GodScheduler decides to allow a switch to receive an
  openflow message.
  """
  def proceed(self, simulation):
    pending_receive = self.pending_receive
    of_buff = simulation.openflow_buffer
    message_waiting = of_buff.message_receipt_waiting(pending_receive)
    if message_waiting:
      if (log.getEffectiveLevel() == logging.DEBUG and
              type(base64_decode_openflow(self.b64_packet)) == ofp_flow_mod):
        show_flow_tables(simulation)
      simulation.openflow_buffer.schedule(pending_receive)
      return True
    return False

  def whitelisted(self):
    pending_receive = self.pending_receive
    ignore = self.ignore_whitelisted_packets
    in_whitelist = OpenFlowBuffer.in_whitelist(pending_receive.fingerprint)
    return ignore and in_whitelist

  @property
  def pending_receive(self):
    """Return a pending receive buffer event"""
    # TODO(cs): inefficient to keep reconstructing this tuple.
    return PendingReceive(self.dpid, self.controller_id, self.fingerprint[1])

  def manually_inject(self, simulation):
    switch = simulation.topology.get_switch(self.dpid)
    conn = switch.get_connection(self.controller_id)
    conn.read(self.get_packet())

  def __str__(self):
    return ("ControlMessageReceive:%s c %s -> s %s [%s]" %
            (self.label, self.controller_id, self.dpid,
             self.fingerprint[1].human_str()))

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round, timeout_disallowed) = \
      extract_base_fields(json_hash)
    assert_fields_exist(json_hash, 'dpid', 'controller_id', 'fingerprint')
    dpid = json_hash['dpid']
    controller_id = json_hash['controller_id']
    fingerprint = json_hash['fingerprint']
    b64_packet = ""
    if 'b64_packet' in json_hash:
      b64_packet = json_hash['b64_packet']
    return ControlMessageReceive(dpid, controller_id, fingerprint,
                                 b64_packet=b64_packet,
                                 logical_round=logical_round, label=label,
                                 event_time=event_time,
                                 timeout_disallowed=timeout_disallowed)


class ControlMessageSend(ControlMessageBase):
  """
  Logged whenever the GodScheduler decides to allow a switch to send an
  openflow message.
  """
  def proceed(self, simulation):
    pending_send = self.pending_send
    of_buff = simulation.openflow_buffer
    message_waiting = of_buff.message_send_waiting(pending_send)
    if message_waiting:
      simulation.openflow_buffer.schedule(pending_send)
      return True
    return False

  def whitelisted(self):
    pending_send = self.pending_send
    return (self.pass_through_sends or
            (self.ignore_whitelisted_packets and
             OpenFlowBuffer.in_whitelist(pending_send.fingerprint)))

  @property
  def pending_send(self):
    """Return a pending receive buffer event"""
    # TODO(cs): inefficient to keep reconstructing this tuple.
    return PendingSend(self.dpid, self.controller_id, self.fingerprint[1])

  def __str__(self):
    return ("ControlMessageSend:%s c %s -> s %s [%s]" %
            (self.label, self.dpid, self.controller_id,
             self.fingerprint[1].human_str()))

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round, timeout_disallowed) = \
      extract_base_fields(json_hash)
    assert_fields_exist(json_hash, 'dpid', 'controller_id', 'fingerprint')
    dpid = json_hash['dpid']
    controller_id = json_hash['controller_id']
    fingerprint = json_hash['fingerprint']
    b64_packet = ""
    if 'b64_packet' in json_hash:
      b64_packet = json_hash['b64_packet']
    return ControlMessageSend(dpid, controller_id, fingerprint,
                              logical_round=logical_round,
                              b64_packet=b64_packet, label=label,
                              event_time=event_time,
                              timeout_disallowed=timeout_disallowed)


# TODO(cs): move me?
class PendingStateChange(namedtuple('PendingStateChange',
                                    ['controller_id', 'event_time', 'fingerprint',
                                     'name', 'value'])):
  def __new__(cls, controller_id, event_time, fingerprint, name, value):
    controller_id = controller_id
    if type(event_time) == list:
      event_time = tuple(event_time)
    if type(fingerprint) == list:
      fingerprint = tuple(fingerprint)
    if type(value) == list:
      value = tuple(value)
    return super(cls, PendingStateChange).__new__(cls, controller_id,
                                                  event_time, fingerprint, name,
                                                  value)

  def _get_regex(self):
    # TODO(cs): if we add varargs to the signature, this needs to be changed
    if type(self.fingerprint) == tuple:
      # Skip over the class name
      return self.fingerprint[1]
    return self.fingerprint

  def __hash__(self):
    # TODO(cs): may need to add more context into the fingerprint to avoid
    # ambiguity
    return self._get_regex().__hash__() + self.controller_id.__hash__()

  def __eq__(self, other):
    if type(other) != type(self):
      return False
    return (self._get_regex() == other._get_regex() and
            self.controller_id == other.controller_id)

  def __ne__(self, other):
    # NOTE: __ne__ in python does *NOT* by default delegate to eq
    return not self.__eq__(other)


class ControllerStateChange(InternalEvent):
  """
  Logged for any (visible) state change in the controller (e.g.
  mastership change). Visibility into controller state changes is obtained
  via syncproto.
  """
  def __init__(self, controller_id, fingerprint, name, value, label=None,
               logical_round=-1, event_time=None, timeout_disallowed=False):
    """
    Parameters:
     - controller_id: unique string label for the controller.
     - name: The format string passed to the controller's logging library.
     - value: An array of values for the format string.
     - label: a unique label for this event. Internal event labels begin with
              'i' and input event labels begin with 'e'.
     - time: the timestamp of when this event occurred. Stored as a tuple:
       [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
     - timeout_disallowed: whether the replayer should wait indefinitely for
       this event to occur. Defaults to False.
    """
    super(ControllerStateChange, self).__init__(label=label,
                                                logical_round=logical_round,
                                                event_time=event_time,
                                                timeout_disallowed=timeout_disallowed)
    self.controller_id = controller_id
    if type(fingerprint) == str or type(fingerprint) == unicode:
      fingerprint = (self.__class__.__name__, fingerprint)
    if type(fingerprint) == list:
      fingerprint = tuple(fingerprint)
    self._fingerprint = fingerprint
    self.name = name
    if type(value) == list:
      value = tuple(value)
    self.value = value

  def proceed(self, simulation):
    observed_yet = simulation.controller_sync_callback\
                             .state_change_pending(self.pending_state_change)
    if observed_yet:
      simulation.controller_sync_callback\
                .ack_pending_state_change(self.pending_state_change)
      return True
    return False

  @property
  def pending_state_change(self):
    """Return a pending state change buffer event"""
    return PendingStateChange(self.controller_id, self.event_time,
                              self._get_message_fingerprint(),
                              self.name, self.value)

  def _get_message_fingerprint(self):
    return self._fingerprint[1]

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format:
    (class name, PendingStateChange.fingerprint, controller id)
    PendingStateChange.fingerprint is the format string passed to the
    controller's logging library (without interpolated values)
    """
    # Somewhat confusing: the StateChange's fingerprint is self._fingerprint,
    # but the overall fingerprint of this event needs to include the controller
    # id
    return tuple(list(self._fingerprint) + [self.controller_id])

  @staticmethod
  def from_pending_state_change(state_change):
    return ControllerStateChange(state_change.controller_id,
                                 state_change.fingerprint, state_change.name,
                                 state_change.value,
                                 event_time=state_change.event_time)

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round, timeout_disallowed) =\
      extract_base_fields(json_hash)
    assert_fields_exist(json_hash, 'controller_id', 'fingerprint',
                        'name', 'value')
    controller_id = json_hash['controller_id']
    fingerprint = json_hash['fingerprint']
    name = json_hash['name']
    value = json_hash['value']
    return ControllerStateChange(controller_id, fingerprint, name, value,
                                 logical_round=logical_round, label=label,
                                 event_time=event_time,
                                 timeout_disallowed=timeout_disallowed)


class DeterministicValue(InternalEvent):
  """
  Logged whenever the controller asks for a deterministic value (e.g.
  gettimeofday()
  """
  def __init__(self, controller_id, name, value, label=None, logical_round=-1,
               event_time=None, timeout_disallowed=False):
    """
    Parameters:
     - controller_id: unique string label for the controller.
     - name: name of the DeterministicValue request, e.g. "gettimeofday"
     - value: the return value of the DeterministicValue request.
     - label: a unique label for this event. Internal event labels begin with
              'i' and input event labels begin with 'e'.
     - event_time: the timestamp of when this event occurred. Stored as a tuple:
                  [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
     - timeout_disallowed: whether the replayer should wait indefinitely for
       this event to occur. Defaults to False.
    """
    super(DeterministicValue, self).__init__(label=label,
                                             logical_round=logical_round,
                                             event_time=event_time,
                                             timeout_disallowed=timeout_disallowed)
    self.controller_id = controller_id
    self.name = name
    if name == "gettimeofday":
      value = SyncTime(seconds=value[0], microSeconds=value[1])
    elif type(value) == list:
      value = tuple(value)
    self.value = value

  def proceed(self, simulation):
    if simulation.controller_sync_callback\
                 .pending_deterministic_value_request(self.controller_id):
      simulation.controller_sync_callback\
        .send_deterministic_value(self.controller_id, self.value)
      return True
    return False

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round, timeout_disallowed) =\
      extract_base_fields(json_hash)
    assert_fields_exist(json_hash, 'controller_id',
                        'name', 'value')
    controller_id = json_hash['controller_id']
    name = json_hash['name']
    value = json_hash['value']
    return DeterministicValue(controller_id, name, value,
                              logical_round=logical_round, label=label,
                              event_time=event_time,
                              timeout_disallowed=timeout_disallowed)


class DataplanePermit(InternalEvent):
  """
  DataplanePermit allows a packet to move from one port to another in the
  dataplane. We basically just keep this around for bookkeeping purposes. During
  replay, this let's us know which packets to let through, and which to drop.
  """
  def __init__(self, fingerprint, label=None, logical_round=-1, event_time=None,
               passive=True):
    """
    Parameters:
     - label: a unique label for this event. Internal event labels begin with
              'i' and input event labels begin with 'e'.
     - event_time: the timestamp of when this event occurred. Stored as a tuple:
                  [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation
                      logical_round this event occurred in.
     - passive: whether we're using Replayer.DataplaneChecker
    """
    # N.B. fingerprint is monkeypatched onto DpPacketOut events by
    # BufferedPatchPanel
    super(DataplanePermit, self).__init__(label=label,
                                          logical_round=logical_round,
                                          event_time=event_time)
    if fingerprint[0] != self.__class__.__name__:
      fingerprint = list(fingerprint)
      fingerprint.insert(0, self.__class__.__name__)
    if type(fingerprint) == list:
      fingerprint = (fingerprint[0], DPFingerprint(fingerprint[1]),
                     fingerprint[2], fingerprint[3])
    self._fingerprint = fingerprint
    # TODO(cs): passive is a bit of a hack, but this was easier.
    self.passive = passive

  def whitelisted(self):
    return self.passive

  def proceed(self, simulation):
    patch_panel = simulation.patch_panel
    dp_event = patch_panel.get_buffered_dp_event(self.fingerprint[1:])
    if dp_event is not None:
      simulation.patch_panel.permit_dp_event(dp_event)
      return True
    return False

  @property
  def fingerprint(self):
    """
    Fingerprint tuple format:
    (class name, DPFingerprint, switch dpid, outgoing port no)
    See fingerprints/messages.py for format of DPFingerprint.
    """
    return self._fingerprint

  @property
  def dp_fingerprint(self):
    """Data path event fingerprint"""
    return self.fingerprint[1]

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'fingerprint')
    fingerprint = json_hash['fingerprint']
    return DataplanePermit(fingerprint, label=label,
                           logical_round=logical_round, event_time=event_time)

  def to_json(self):
    fields = dict(self.__dict__)
    fields['class'] = self.__class__.__name__
    fields['fingerprint'] = (self.fingerprint[0], self.fingerprint[1].to_dict(),
                             self.fingerprint[2], self.fingerprint[3])
    del fields['_fingerprint']
    return json.dumps(fields)


class ProcessFlowMod(ControlMessageBase):
  """
  Logged whenever the network-wide OpenFlowBuffer decides to allow buffered
  (local to each switch) OpenFlow flow_mod message through and be processed by
  the switch.
  """
  # TODO(jl): Update visualization tool to recognize this replay event

  def proceed(self, simulation):
    switch = simulation.topology.get_switch(self.dpid)
    of_buff = switch.openflow_buffer
    message_waiting = of_buff.message_receipt_waiting(self.pending_receive)
    if message_waiting:
      switch.openflow_buffer.schedule(self.pending_receive)
      return True
    return False

  @property
  def pending_receive(self):
    """Return a pending receive buffer event"""
    # TODO(cs): inefficient to keep reconrstructing this tuple.
    return PendingReceive(self.dpid, self.controller_id, self.fingerprint[1])

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round, timeout_disallowed) =\
      extract_base_fields(json_hash)
    assert_fields_exist(json_hash, 'dpid', 'controller_id', 'fingerprint')
    dpid = json_hash['dpid']
    controller_id = json_hash['controller_id']
    fingerprint = json_hash['fingerprint']
    b64_packet = ""
    if 'b64_packet' in json_hash:
      b64_packet = json_hash['b64_packet']
    return ProcessFlowMod(dpid, controller_id, fingerprint,
                          b64_packet=b64_packet,
                          logical_round=logical_round, label=label,
                          event_time=event_time,
                          timeout_disallowed=timeout_disallowed)

  def __str__(self):
    return ("ProcessFlowMod:%s c %s -> s %s [%s]" %
            (self.label, self.controller_id, self.dpid,
             self.fingerprint[1].human_str()))

all_internal_events = [ControlMessageReceive, ControlMessageSend,
                       ControllerStateChange, DeterministicValue,
                       DataplanePermit, ProcessFlowMod]


# Special events:
class SpecialEvent(Event):
  """Special Events without proceed function"""
  def __init__(self, prefix="e", label=None, logical_round=-1, event_time=None,
               dependent_labels=None, prunable=True):
    super(SpecialEvent, self).__init__(prefix=prefix, label=label,
                                       logical_round=logical_round,
                                       event_time=event_time,
                                       dependent_labels=dependent_labels,
                                       prunable=prunable)

  def proceed(self, _):
    raise RuntimeError("Should never be called!")

  @staticmethod
  def from_json(json_hash):
    raise NotImplementedError()


class InvariantViolation(SpecialEvent):
  """Class for logging violations as json dicts"""
  def __init__(self, violations, label=None, logical_round=-1, event_time=None,
               persistent=False):
    """
    Parameters:
     - violations: an array of strings specifying the invariant violation
       fingerprints. Format of the strings depends on the invariant check.
       Empty array means there were no violations.
     - label: a unique label for this event. Internal event labels begin with
              'i' and input event labels begin with 'e'.
     - time: the timestamp of when this event occurred. Stored as a tuple:
       [seconds since unix epoch, microseconds].
     - logical_round: optional integer. Indicates what simulation logical_round
                      this event occurred in.
    """
    super(InvariantViolation, self).__init__(label=label,
                                             logical_round=logical_round,
                                             event_time=event_time)
    if len(violations) == 0:
      raise ValueError("Must have at least one violation string")
    # To avoid splitting strings into list of single chars
    if isinstance(violations, basestring):
      violations = [violations]
    self.violations = [str(v) for v in violations]
    self.persistent = persistent

  @staticmethod
  def from_json(json_hash):
    (label, event_time, logical_round) = extract_label_time(json_hash)
    assert_fields_exist(json_hash, 'violations')
    violations = json_hash['violations']
    persistent = True
    if 'persistent' in json_hash:
      persistent = json_hash['persistent']
    return InvariantViolation(violations, label=label,
                              logical_round=logical_round,
                              event_time=event_time, persistent=persistent)

all_special_events = [InvariantViolation]

all_events = all_input_events + all_internal_events + all_special_events

dp_events = set([DataplanePermit, DataplaneDrop])
