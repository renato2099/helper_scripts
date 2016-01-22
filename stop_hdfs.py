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

stopHdfsCmd = '{0}/sbin/stop-dfs.sh'.format(Hadoop.hadoopdir)
hdfsClients = ThreadedClients(Hadoop.datanodes, stopHdfsCmd, root=True)
hdfsClients.start()
hdfsClients.join()

#hadoop-end.sh
hadoopEnv = '{0}/etc/hadoop/hadoop-env.sh'.format(Hadoop.hadoopdir)
hadoopEnvFile = open(hadoopEnv)
lines = hadoopEnvFile.readlines()
hadoopEnvFile.close()
w = open(hadoopEnv,'w')
w.writelines([item for item in lines[:-4]])
w.close()

