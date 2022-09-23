from lib.Cluster import Cluster
from lib.Cassandra import Cassandra
from lib.NoSQLBench import NoSQLBench

import logging


class Driver:
    def __init__(self):
        self.cluster = None
        self.cassandra = None
        self.nb = None
        self.roles = None

    def get_resources(self, **kwargs):
        self.cluster = Cluster(**kwargs)
        self.roles = self.cluster.get_resources()
    
    def deploy_cassandra(self, name, docker_image, conf_template, log_status=True):
        self.cassandra = Cassandra(name=name, docker_image=docker_image)

        self.cassandra.set_hosts(self.roles["cassandra"])
        self.cassandra.set_seeds(self.roles["seeds"])

        if self.cluster.not_seed_count > 0:
            self.cassandra.set_not_seeds(self.roles["not_seeds"])

        self.cassandra.create_config(conf_template)

        self.cassandra.deploy_and_start()
        
        if log_status:
            logging.info(self.cassandra.nodetool("status"))

    def deploy_nb(self, name, docker_image):
        self.nb = NoSQLBench(name=name, docker_image=docker_image)

        self.nb.set_hosts(self.roles["clients"])

        self.nb.deploy()
        
    def run_nb(self, command):
        self.nb.command(command)
