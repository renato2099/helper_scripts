from pssh import ParallelSSHClient
from threading import Thread

import time
import random

class Color:
    FAIL = '\033[91m'


class ChildClient(Thread):
    def __init__(self, servers, cmd):
        Thread.__init__(self)
        self.client = ParallelSSHClient(servers, user="root")
        self.cmd = cmd

    def run(self):
        output = self.client.run_command(self.cmd)
        for host in output:
            for line in output[host]['stdout']:
                print "Host {0}: {1}".format(host, line)

            for line in output[host]['stderr']:
                print "{0}Host {1}: {2}".format(Color.FAIL, host, line)


class ThreadedClients(Thread):
    def __init__(self, servers, cmd, rnd_start=False):
        Thread.__init__(self)
        self.children = []
        for server in servers:
            print "Create child for server {0} with command {1}".format(server, cmd)
            self.children.append(ChildClient([server], cmd))
            self.rnd_start = rnd_start

    def run(self):
        for child in self.children:
            if self.rnd_start:
                time.sleep(random.randrange(0, 4))
            child.start()
