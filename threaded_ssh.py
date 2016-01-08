from pssh import ParallelSSHClient
from threading import Thread

class ThreadedClients(Thread):
    FAIL = '\033[91m'

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
                print "{0}Host {1}: {2}".format(self.FAIL, host, line)
