import logging
import enoslib as en


class Cluster:
    def __init__(self, site, cluster, reservation, walltime, job_name,
                 node_count, seed_count, client_count,
                 docker_registry=dict(type="external", ip="docker-cache.grid5000.fr", port=80)):
        self.site = site
        self.cluster = cluster
        self.reservation = reservation
        self.walltime = walltime
        self.job_name = job_name
        self.node_count = node_count
        self.seed_count = seed_count
        self.client_count = client_count
        self.docker_registry = docker_registry
        
        self.provider = None
        self.roles = None
    
    @property
    def not_seed_count(self):
        return self.node_count - self.seed_count
    
    def get_resources(self, with_docker=True):
        network = en.G5kNetworkConf(type="prod", roles=["cassandra-net"], site=self.site)

        self.provider = en.G5k(
            en.G5kConf.from_settings(job_type="allow_classic_ssh",
                                     job_name=self.job_name,
                                     reservation=self.reservation,
                                     walltime=self.walltime)
            .add_machine(roles=["hosts", "cassandra", "seeds"],
                         cluster=self.cluster,
                         nodes=self.seed_count,
                         primary_network=network)
            .add_machine(roles=["hosts", "cassandra", "not_seeds"],
                         cluster=self.cluster,
                         nodes=self.not_seed_count,
                         primary_network=network)
            .add_machine(roles=["hosts", "clients"],
                         cluster=self.cluster,
                         nodes=self.client_count,
                         primary_network=network)
            .add_network_conf(network)
            .finalize()
        )

        self.roles, _ = self.provider.init()
        
        logging.info("Installing Docker on hosts...")

        if with_docker:
            # Deploy Docker agent
            en.Docker(agent=self.roles["hosts"],
                      bind_var_docker="/tmp/docker",
                      registry_opts=self.docker_registry).deploy()
        
        for host in self.roles["hosts"]:
            logging.info(f"Host {host.address} is ready!")

        return self.roles
    
    def destroy(self):
        self.provider.destroy()
        
        self.provider = None
        self.roles = None
