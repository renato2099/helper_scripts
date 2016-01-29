#!/usr/bin/env python
import os, sys
from ServerConfig import General
from ServerConfig import Hadoop
from ServerConfig import Zookeeper
from ServerConfig import Hbase

def execssh(hosts, cmd):
    for host in hosts:
        os.system('ssh -A root@{0} {1}'.format(host, cmd))

def stopZk():
    zk_stop_cmd = '{0}/bin/zkServer.sh stop'.format(Zookeeper.zkdir)
    execssh([Zookeeper.zkserver], zk_stop_cmd)
    execssh([Zookeeper.zkserver], "rm -r {0}".format(Zookeeper.datadir))

def main():
   hbase_stop_cmd = '{0}/bin/stop-hbase.sh'.format(Hbase.hbasedir)
   stopZk()
   execssh([Hbase.hmaster], hbase_stop_cmd)

if __name__ == "__main__":
   main()
