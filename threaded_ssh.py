from pssh import ParallelSSHClient
from threading import Thread

import time
import random

class Color:
    FAIL = '\033[91m'
    ENDC = '\033[0m'

class OutputClient(Thread):
    def __init__(self, host, out, prefix="", suffix=""):
        Thread.__init__(self)
        self.host = host
        self.out = out
        self.prefix = prefix
        self.suffix = suffix

    def run(self):
        for line in self.out:
            print "{0}Host {1}: {2}{3}".format(self.prefix, self.host, line, self.suffix)


class ChildClient(Thread):
    def __init__(self, servers, cmd):
        Thread.__init__(self)
        self.client = ParallelSSHClient(servers, user="root")
        self.cmd = cmd

    def run(self):
        output = self.client.run_command(self.cmd)
        threads = []
        for host in output:
            oThread = OutputClient(host, output[host]['stdout'])
            oThread.start()
            threads.append(oThread)
            oThread = OutputClient(host, output[host]['stderr'], prefix=Color.FAIL, suffix=Color.ENDC)
            oThread.start()
            threads.append(oThread)
        for t in threads:
            t.join()


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
