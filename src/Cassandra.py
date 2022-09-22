import logging
import pathlib
import shutil
import time
import yaml
import enoslib as en


def update_dict_from_spec(data, update_spec):
    """
    Update a dict (or a list) according to a given update specification.
    """

    for key in update_spec:
        if isinstance(update_spec[key], dict):
            update_dict_from_spec(data[key], update_spec[key])
        else:
            data[key] = update_spec[key]


def build_yaml(template_path, output_path, update_spec):
    """
    Load a YAML template file, update some properties, and write the result
    to a new YAML file.
    
    This function takes an update specification to inject the modified values
    at the right location in the YAML structure. This specification takes the
    form of a Python dictionary that follows the structure of the property to
    update.
    
    This is easier to understand with an example. Let us say we have the
    following YAML template file:
    
    ```yaml
    foo:
      bar:
        baz: 0
        qux: 1
    ```
    
    Then, suppose we apply the following update specification:
    
    ```python
    spec = {
        "foo": {
            "bar": {
                "qux": 1234
            }
        }
    }
    ```
    
    The resulting YAML file will be:
    
    ```yaml
    foo:
      bar:
        baz: 0
        qux: 1234
    ```
    
    Note that the other property `bar` has not been modified.
    """

    with open(template_path, mode="r") as template_file:
        data = yaml.safe_load(template_file)
    
    update_dict_from_spec(data, update_spec)
    
    with open(output_path, mode="w+") as output_file:
        yaml.dump(data, output_file)


def host_addresses(hosts, port=0):
    """
    Transform a list of EnOSlib roles to a list of string addresses.
    """

    _port = f":{port}" if port > 0 else ""

    return [f"{host.address}{_port}" for host in hosts]


class Cassandra:
    def __init__(self, name, docker_image, root_path="/root/cassandra"):
        self.name = name
        self.docker_image = docker_image
        self.root_path = root_path

        self.mounts = []
        self.hosts = None
        self.seeds = None
        self.not_seeds = None
    
    def reset(self):
        self.mounts = []
        self.hosts = None
        self.seeds = None
        self.not_seeds = None

    def set_host_data(self):
        for host in self.hosts:
            local_root_path = host.address

            host.extra.update(local_root_path=local_root_path)
            host.extra.update(local_conf_path=f"{local_root_path}/conf")

    def create_config(self, template_path):
        seed_addresses = ",".join(host_addresses(self.seeds, port=7000))

        for host in self.hosts:
            local_conf_path = host.extra["local_conf_path"]

            # Make sure the output directory exists
            pathlib.Path(local_conf_path).mkdir(parents=True, exist_ok=True)

            build_yaml(template_path=template_path,
                       output_path=f"{local_conf_path}/cassandra.yaml",
                       update_spec={
                           "seed_provider": {0: {"parameters": {0: {"seeds": seed_addresses}}}},
                           "listen_address": host.address,
                           "rpc_address": host.address
                       })

    def setup(self, hosts, seeds, not_seeds, conf_template):
        self.reset()

        self.hosts = hosts
        self.seeds = seeds
        self.not_seeds = not_seeds
        
        self.mount(source=f"{self.root_path}/conf/cassandra.yaml",
                   target="/etc/cassandra/cassandra.yaml")

        self.set_host_data()

        self.create_config(conf_template)

    def cleanup(self):
        for host in self.hosts:
            shutil.rmtree(host.extra["local_root_path"])

    def mount(self, source, target, type="bind"):
        self.mounts.append({
            "source": source,
            "target": target,
            "type": type
        })

    def deploy(self):
        """
        Deploy a cluster of Cassandra nodes. Make some system
        optimizations to run Cassandra properly.

        1. Configure nodes for running Cassandra:
            a. Disable the swap memory.
            b. Increase number of memory map areas.
        2. Create Cassandra containers.
        """

        with en.actions(roles=self.hosts) as actions:
            actions.file(path=self.root_path, state="directory")

            # Transfer configuration files
            actions.copy(src="{{local_conf_path}}", dest=self.root_path)

            # Disable the swap memory
            actions.shell(command="swapoff --all")

            # Increase number of memory map areas
            actions.sysctl(name="vm.max_map_count", value="1048575")

            # Create Cassandra container (without running)
            actions.docker_container(name=self.name,
                                     image=self.docker_image,
                                     state="present",
                                     detach="yes",
                                     network_mode="host",
                                     mounts=self.mounts)

    def start_host(self, host, spawn_time=120):
        """
        Run a Cassandra node. Make sure to wait at least 2 minutes
        in order to let Cassandra start properly.
        """
        
        with en.actions(roles=host) as actions:
            actions.docker_container(name=self.name, state="started")
        
        time.sleep(spawn_time)
        
        logging.info(f"Cassandra is up and running on host {host.address}")

    def start(self):
        """
        Run a Cassandra cluster. This is done in 2 steps:

        1. Run Cassandra on seed nodes.
        2. Run Cassandra on remaining nodes.

        Note that running Cassandra should be done one node at a time;
        this is due to the bootstrapping process which may generate
        collisions when two nodes are starting at the same time.

        For more details, see:
        - https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/initialize/initSingleDS.html
        - https://thelastpickle.com/blog/2017/05/23/auto-bootstrapping-part1.html
        """

        for host in self.seeds:
            self.start_host(host)

        for host in self.not_seeds:
            self.start_host(host)

        logging.info("Cassandra is running!")

    def deploy_and_start(self):
        """
        Util to deploy and start Cassandra in one call.
        """

        self.deploy()
        
        self.start()
