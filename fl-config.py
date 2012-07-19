from sts.topology_generator import *
from sts.experiment_config_lib import *
controllers = [ Controller(port=6633), Controller(port=6634) ]
boot_controllers = False
topology_generator = TopologyGenerator()
topology_generator.connections_per_switch = 2
floodlight_port = 8080
delay = 1.0