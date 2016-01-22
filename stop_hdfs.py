#!/usr/bin/env python
import os, sys
from ServerConfig import General
from ServerConfig import Hadoop

def execssh(hosts, cmd):
    for host in hosts:
        os.system('ssh -A root@{0} {1}'.format(host, cmd))

execssh([Hadoop.namenode], '{0}/sbin/stop-dfs.sh'.format(Hadoop.hadoopdir))
execssh(Hadoop.datanodes + [Hadoop.namenode], "umount {0}".format(Hadoop.datadir))
execssh(Hadoop.datanodes + [Hadoop.namenode], "rm -r {0}".format(Hadoop.datadir))

#hadoop-end.sh
hadoopEnv = '{0}/etc/hadoop/hadoop-env.sh'.format(Hadoop.hadoopdir)
hadoopEnvFile = open(hadoopEnv)
lines = hadoopEnvFile.readlines()
hadoopEnvFile.close()
w = open(hadoopEnv,'w')
w.writelines([item for item in lines[:-4]])
w.close()

stopHdfsCmd = '{0}/sbin/stop-dfs.sh'.format(Hadoop.hadoopdir)
os.system('ssh root@{0} {1}'.format(Hadoop.namenode, stopHdfsCmd))
#hdfsClients = ThreadedClients(Hadoop.datanodes, stopHdfsCmd, root=True)
#hdfsClients.start()
#hdfsClients.join()
