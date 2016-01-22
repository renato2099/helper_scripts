#!/usr/bin/env python
import os, sys
from ServerConfig import General
from ServerConfig import Hadoop
from threaded_ssh import ThreadedClients

umntClients = ThreadedClients(Hadoop.datanodes, "umount {0}".format(Hadoop.datadir), root=True)
umntClients.start()
umntClients.join()

rmClients = ThreadedClients(Hadoop.datanodes, "rm -r {0}".format(Hadoop.datadir), root=True)
rmClients.start()
rmClients.join()

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