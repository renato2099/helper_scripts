#!/usr/bin/env python
import os, sys, time
from threaded_ssh import ThreadedClients

from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Cassandra
from ServerConfig import Hadoop
from ServerConfig import Hbase

def execssh(hosts, cmd):
    clients = ThreadedClients(hosts, cmd, root=True)
    clients.start()
    clients.join()

def stop_java_unmount_memfs(): 
    if Storage.storage == Cassandra:
        execssh(Storage.servers, "umount {0}; rm -r {0}; rm -r {1}".format(Cassandra.datadir, Cassandra.logdir))
        execssh(Storage.servers1, "umount {0}; rm -r {0}; rm -r {1}".format(Cassandra.datadir1, Cassandra.logdir1))
    elif Storage.storage == Hbase or Storage.storage == Hadoop:
        execssh([Storage.master] + Storage.servers, "umount {0}; rm -r {0}".format(Hadoop.datadir)) 
        execssh(Storage.servers1, "umount {0}; rm -r {0}".format(Hadoop.datadir1)) 

if __name__ == '__main__':
    stop_java_unmount_memfs()
