#!/usr/bin/env python
import os, sys, time
from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Cassandra

# cassandra itselve should now be stopped by the expRunner, therefore only clear ramdisk

def execssh(hosts, cmd):
    print hosts
    for host in hosts:
        print "{0}: {1}".format(host, cmd)
        os.system('ssh -A root@{0} {1}'.format(host, cmd))

#stop_cas_cmd = "ps -a | grep cassandra | grep -v grep | awk '{print $2}' | xargs kill -9 "
#stop_cas_cmd = "killall java"
#execssh(Storage.servers, stop_cas_cmd)
#time.sleep(3)

execssh(Storage.servers, "umount {0}".format(Cassandra.datadir))
execssh(Storage.servers, "rm -r {0}".format(Cassandra.datadir))
execssh(Storage.servers, "rm -r {0}".format(Cassandra.logdir))

