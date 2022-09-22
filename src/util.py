import time
import pathlib
import shutil
import logging
import yaml
import enoslib as en
import enoslib.config as en_config


CASSANDRA_SPAWN_TIME_IN_SECONDS = 120


en_config.set_config(g5k_cache=False)
en.init_logging()


def get_g5k_resources(conf):
    """
    Reserve resources on Grid5000.

    Example:

    ```python
    provider = get_resources({
        "site": "nancy",
        "cluster": "gros",
        "walltime": "1:00:00",
        "job_name": "cassandra",
        "node_count": 6,
        "seed_count": 1,
        "client_count": 1
    })
    
    roles, networks = provider.init()
    
    # ...
    
    provider.destroy()
    ```
    """

    network = en.G5kNetworkConf(type="prod",
                                roles=["cassandra-net"],
                                site=conf["site"])

    return en.G5k(
        en.G5kConf.from_settings(job_type="allow_classic_ssh",
                                 job_name=conf["job_name"],
                                 walltime=conf["walltime"])
        .add_machine(roles=["hosts", "cassandra", "seeds"],
                     cluster=conf["cluster"],
                     nodes=conf["seed_count"],
                     primary_network=network)
        .add_machine(roles=["hosts", "cassandra", "not_seeds"],
                     cluster=conf["cluster"],
                     nodes=conf["node_count"] - conf["seed_count"],
                     primary_network=network)
        .add_machine(roles=["hosts", "clients"],
                     cluster=conf["cluster"],
                     nodes=conf["client_count"],
                     primary_network=network)
        .add_network_conf(network)
        .finalize()
    )


def update_dict_from_spec(data, update_spec):
    for key in update_spec:
        if isinstance(update_spec[key], dict):
            update_dict_from_spec(data[key], update_spec[key])
        else:
            data[key] = update_spec[key]


def build_yaml_conf(template_path, output_path, update_spec):
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
        conf = yaml.safe_load(template_file)
    
    update_dict_from_spec(conf, update_spec)
    
    with open(output_path, mode="w+") as output_file:
        yaml.dump(conf, output_file)


def roles_to_addresses(roles, port=0):
    """
    Transform a list of EnOSlib roles to a list of string addresses.
    """

    _port = f":{port}" if port > 0 else ""

    return [f"{role.address}{_port}" for role in roles]


def set_cassandra_conf_path(roles):
    for role in roles["cassandra"]:
        conf_path = f"{role.address}/conf"
        
        role.extra.update(conf_path=conf_path)
        pathlib.Path(conf_path).mkdir(parents=True, exist_ok=True)


def set_cassandra_conf(roles, template_path):
    seed_addresses = ",".join(roles_to_addresses(roles["seeds"], port=7000))
    
    for role in roles["cassandra"]:
        conf_path = role.extra["conf_path"]
        output_path = f"{conf_path}/cassandra.yaml"

        build_yaml_conf(template_path=template_path,
                        output_path=output_path,
                        update_spec={
                            "seed_provider": {0: {"parameters": {0: {"seeds": seed_addresses}}}},
                            "listen_address": role.address,
                            "rpc_address": role.address
                        })


def transfer_cassandra_conf(roles):
    with en.actions(roles=roles["cassandra"]) as actions:
        actions.file(path="/root/cassandra", state="directory")
        actions.copy(src="{{conf_path}}", dest="/root/cassandra")
    
    for role in roles["cassandra"]:
        shutil.rmtree(role.address)


def deploy_docker(roles):
    """
    Setup a Docker agent on nodes. Additionally configure a custom
    Docker registry.
    """

    registry_opts = dict(type="external",
                         ip="docker-cache.grid5000.fr",
                         port=80)

    # Deploy Docker agent
    en.Docker(agent=roles["hosts"],
              bind_var_docker="/tmp/docker",
              registry_opts=registry_opts).deploy()


def deploy_cassandra(roles, docker_image):
    """
    Deploy a cluster of Cassandra nodes. Make some system
    optimizations to run Cassandra properly.

    1. Configure nodes for running Cassandra:
        a. Disable the swap memory.
        b. Increase number of memory map areas.
    2. Create Cassandra containers.
    """

    with en.actions(roles=roles["cassandra"]) as actions:
        # Disable the swap memory
        actions.shell(command="swapoff --all")
        
        # Increase number of memory map areas
        actions.sysctl(name="vm.max_map_count", value="1048575")
        
        # Create Cassandra container (without running)
        actions.docker_container(name="cassandra",
                                 state="present",
                                 image=docker_image,
                                 detach="yes",
                                 network_mode="host",
                                 mounts=[
                                     {
                                         "source": "/root/cassandra/conf/cassandra.yaml",
                                         "target": "/etc/cassandra/cassandra.yaml",
                                         "type": "bind"
                                     }
                                 ])


def deploy_nosqlbench(roles, docker_image):
    with en.actions(roles=roles["clients"]) as actions:
        actions.file(path="/root/nosqlbench", state="directory")
        actions.docker_image(name=docker_image, source="pull")


def run_nosqlbench(roles, docker_image, command):
    with en.actions(roles=roles["clients"]) as actions:
        actions.docker_container(name="nosqlbench",
                                 image=docker_image,
                                 command=command,
                                 network_mode="host",
                                 mounts=[
                                     {
                                         "source": "/root/nosqlbench",
                                         "target": "/etc/nosqlbench",
                                         "type": "bind"
                                     }
                                 ])


def start_cassandra_nodes(roles):
    for role in roles:
        with en.actions(roles=role) as actions:
            actions.docker_container(name="cassandra", state="started")
        
        time.sleep(CASSANDRA_SPAWN_TIME_IN_SECONDS)
        
        logging.info(f"Cassandra is up and running on host {role.address}")


def start_cassandra(roles):
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

    start_cassandra_nodes(roles["seeds"])
    start_cassandra_nodes(roles["others"])
    
    logging.info("Cassandra cluster is running!")


def deploy_and_start_cassandra(roles, conf_template_path, docker_image):
    set_cassandra_conf_path(roles)
    set_cassandra_conf(roles, conf_template_path)

    transfer_cassandra_conf(roles)

    deploy_cassandra(roles, docker_image)

    start_cassandra(roles)
