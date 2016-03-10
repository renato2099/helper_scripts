from pssh import ParallelSSHClient
from threading import Thread

import traceback
import time
import random
import sys

class Color:
    FAIL = '\033[91m'
    ENDC = '\033[0m'

class OutputClient(Thread):
    def __init__(self, host, out, prefix="", suffix="", outObs=[]):
        Thread.__init__(self)
        self.host = host
        self.out = out
        self.prefix = prefix
        self.suffix = suffix
        # registering
        self.observers = outObs

    def run(self):
        for line in self.out:
            print "{0}Host {1}: {2}{3}".format(self.prefix, self.host, line, self.suffix)
            self.notify_observers(self.host, line)

    # observable methods
    def register(self, observer): 
        if not observer in self.observers:
           self.observers.append(observer)
    def unregister (self, observer):
        if observer in self.observers:
           self.observers.remove(observer)
    def notify_observers(self, *args):
#        print "qwert", self.observers
        for obs in self.observers:
          obs.notify(self, *args)

class ChildClient(Thread):
    nextPid = 0
    def __init__(self, servers, cmd, asRoot = True, outputObservers=[]):
        Thread.__init__(self)
        if asRoot:
            self.client = ParallelSSHClient(servers, user="root")
        else:
            self.client = ParallelSSHClient(servers)
        self.cmd = cmd
        self.observers = outputObservers

    def kill(self):
        self.client.run_command("cat {0} | xargs kill -9".format(self.pidFile))

    def run(self):
        try:
            thisPid = ChildClient.nextPid
            ChildClient.nextPid += 1
            self.pidFile = '/tmp/awesome_{0}.pid'.format(thisPid)
            output = self.client.run_command("{0} & echo $! > {1}; wait < {1} ".format(self.cmd, self.pidFile))
            threads = []
            for host in output:
                oThread = OutputClient(host, output[host]['stdout'], outObs=self.observers)
                oThread.start()
                threads.append(oThread)
                oThread = OutputClient(host, output[host]['stderr'], prefix=Color.FAIL, suffix=Color.ENDC, outObs=self.observers)
                oThread.start()
                threads.append(oThread)
            for t in threads:
                t.join()
        except:
            type, value, tb = sys.exc_info()
            traceback.print_exc()
            last_frame = lambda tb=tb: last_frame(tb.tb_next) if tb.tb_next else tb
            frame = last_frame().tb_frame
            ns = dict(frame.f_globals)
            ns.update(frame.f_locals)
            code.interact(local=ns)

class ThreadedClients(Thread):
    def __init__(self, servers, cmd, rnd_start=False, root=True, observers=[]):
        Thread.__init__(self)

        self.children = []
        for server in servers:
            print "Create child for server {0} with command {1}".format(server, cmd)
            self.children.append(ChildClient([server], cmd, asRoot=root, outputObservers=observers))
            self.rnd_start = rnd_start

    def kill(self):
        for child in self.children:
            child.kill()

    def run(self):
        for child in self.children:
            if self.rnd_start:
                time.sleep(random.randrange(0, 4))
            child.start()
