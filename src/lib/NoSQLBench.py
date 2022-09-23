from datetime import datetime

import logging
import pathlib
import enoslib as en


class Command:
    def __init__(self):
        self.tokens = []
    
    def option(self, key, value):
        self.tokens.append(f"{key}={value}")
        
        return self
    
    def options(self, **kwargs):
        for key in kwargs:
            self.option(key, kwargs[key])
        
        return self
    
    def arg(self, name, value):
        self.tokens.append(name)
        self.tokens.append(str(value))
        
        return self
    
    def logs_dir(self, value):
        self.arg("--logs-dir", value)
        
        return self
    
    def logs_max(self, value):
        self.arg("--logs-max", value)
        
        return self
    
    def logs_level(self, value):
        self.arg("--logs-level", value)
        
        return self
    
    def report_csv_to(self, value):
        self.arg("--report-csv-to", value)
        
        return self
    
    def report_interval(self, value):
        self.arg("--report-interval", value)
        
        return self
    
    def log_histograms(self, value):
        self.arg("--log-histograms", value)
        
        return self
    
    def log_histostats(self, value):
        self.arg("--log-histostats", value)
        
        return self
    
    def report_summary_to(self, value):
        self.arg("--report-summary-to", value)
        
        return self
    
    def __str__(self):
        return " ".join(self.tokens)


class RunCommand(Command):
    def __init__(self):
        super().__init__()
        
        self.tokens.append("run")
    
    @staticmethod
    def from_options(**kwargs):
        return RunCommand().options(**kwargs)


class NoSQLBench:
    def __init__(self, name, docker_image,
                 conf_dir="conf",
                 data_dir="data",
                 root_path="/root/nosqlbench",
                 container_conf_path="/etc/nosqlbench",
                 container_data_path="/var/lib/nosqlbench",
                 local_template_path="templates/nb"):
        self.name = name
        self.docker_image = docker_image
        
        self.root_path = root_path
        self.conf_path = f"{root_path}/{conf_dir}"
        self.data_path = f"{root_path}/{data_dir}"
        self.container_conf_path = container_conf_path
        self.container_data_path = container_data_path
        self.local_template_path = local_template_path

        self.hosts = None
    
    @property
    def host_count(self):
        return len(self.hosts) if self.hosts is not None else 0

    def set_hosts(self, hosts):
        self.hosts = hosts

    def deploy(self):
        with en.actions(roles=self.hosts) as actions:
            # Create root directory on remote
            actions.file(path=self.root_path, state="directory")

            # Transfer file tree in root directory on remote.
            # Note: make sure to leave the ending / in src, as we want Ansible
            # to only copy inside contents of the template directory (and not
            # the template directory itself).
            actions.copy(src=f"{self.local_template_path}/", dest=self.root_path)

            # Pull Docker image
            actions.docker_image(name=self.docker_image, source="pull")
        
        logging.info("NoSQLBench has been deployed. Ready to benchmark.")

    def command(self, cmd, hosts=None):
        if isinstance(cmd, Command):
            cmd = str(cmd)

        if hosts is None:
            hosts = self.hosts

        logging.info(f"Running command: {cmd}.")    

        with en.actions(roles=hosts) as actions:
            actions.docker_container(name=self.name,
                                     image=self.docker_image,
                                     detach="no",
                                     network_mode="host",
                                     mounts=[
                                         {
                                             "source": self.conf_path,
                                             "target": self.container_conf_path,
                                             "type": "bind"
                                         },
                                         {
                                             "source": self.data_path,
                                             "target": self.container_data_path,
                                             "type": "bind"
                                         }
                                     ],
                                     command=cmd,
                                     auto_remove="yes")

    def sync_results(self, dest=None, hosts=None):
        if dest is None:
            dest = f"./results_{datetime.today().isoformat()}"

        # Ensure destination folder exists
        pathlib.Path(dest).mkdir(parents=True, exist_ok=True)

        if hosts is None:
            hosts = self.hosts

        with en.actions(roles=hosts) as actions:
            actions.synchronize(src=self.data_path, dest=dest, mode="pull")
