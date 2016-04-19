#!/usr/bin/env python
import os

from ServerConfig import Storage
from ServerConfig import Cassandra
from ServerConfig import Hadoop
from ServerConfig import Hbase

def execssh(hosts, cmd):
    for host in hosts:
        os.system('ssh root@{0} {1}'.format(host, cmd))

def unmount_memfs(): 
    if Storage.storage == Cassandra:
        execssh(Storage.servers, "umount {0} --force".format(Cassandra.datadir))
        execssh(Storage.servers, "rm -rf {0} {1}".format(Cassandra.datadir, Cassandra.logdir))
        execssh(Storage.servers, "umount {0} --force".format(Cassandra.datadir1))
        execssh(Storage.servers, "rm -rf {0} {1}".format(Cassandra.datadir1, Cassandra.logdir1))
    elif Storage.storage == Hbase or Storage.storage == Hadoop:
        execssh([Storage.master] + Storage.servers, "umount {0} --force".format(Hadoop.datadir)) 
        execssh([Storage.master] + Storage.servers, "rm -rf {0}".format(Hadoop.datadir)) 
        execssh(Storage.servers1, "umount {0} --force".format(Hadoop.datadir1)) 
        execssh(Storage.servers1, "rm -rf {0}".format(Hadoop.datadir1)) 

if __name__ == '__main__':
    unmount_memfs()
