import logging
import pathlib
from datetime import datetime
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
        self.tokens.append(value)
        
        return self
    
    def report_csv_to(self, value):
        self.arg("--report-csv-to", value)
        
        return self
    
    def report_interval(self, value):
        self.arg("--report-interval", value)
        
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
    def __init__(self, name, docker_image, root_path="/root/nosqlbench"):
        self.name = name
        self.docker_image = docker_image
        self.root_path = root_path
        
        self.mounts = []
        self.hosts = None
    
    def reset(self):
        self.mounts = []
        self.hosts = None

    def setup(self, hosts):
        self.reset()

        self.hosts = hosts

        self.mount(source=f"{self.root_path}/conf", target="/etc/nosqlbench")
        self.mount(source=f"{self.root_path}/data", target="/var/lib/nosqlbench")

    def mount(self, source, target, type="bind"):
        self.mounts.append({
            "source": source,
            "target": target,
            "type": type
        })

    def deploy(self):
        with en.actions(roles=self.hosts) as actions:
            # Create root directory on remote
            actions.file(path=self.root_path, state="directory")

            # Transfer file tree in root directory on remote.
            # Note: make sure to leave the ending / in src, as we want Ansible
            # to only copy inside contents of the template directory (and not
            # the template directory itself).
            actions.copy(src="templates/nb/", dest=self.root_path)
            
            # Pull Docker image
            actions.docker_image(name=self.docker_image, source="pull")

    def command(self, cmd, hosts=None):
        if isinstance(cmd, Command):
            cmd = str(cmd)

        if hosts is None:
            hosts = self.hosts

        with en.actions(roles=hosts) as actions:
            actions.docker_container(name=self.name,
                                     image=self.docker_image,
                                     network_mode="host",
                                     mounts=self.mounts,
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
            actions.synchronize(src=f"{self.root_path}/data", dest=dest, mode="pull")
