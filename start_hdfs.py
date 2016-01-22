#!/usr/bin/env python
import os
import time

from pssh import ParallelSSHClient
from ServerConfig import General
from ServerConfig import Hadoop
from ServerConfig import Storage
from ServerConfig import TellStore
from threaded_ssh import ThreadedClients

dfs_start_cmd = "JAVA_HOME={1} {0}/sbin/start-dfs.sh".format(Hadoop.hadoopdir, General.javahome)
nn_format_cmd = "JAVA_HOME={1} {0}/bin/hadoop namenode -format".format(Hadoop.hadoopdir, General.javahome)

mkClients = ThreadedClients(Hadoop.datanodes, "mkdir -p {0}".format(Hadoop.datadir), root=True)
mkClients.start()
mkClients.join()

mntClients = ThreadedClients(Hadoop.datanodes, "mount -t tmpfs -o size={0}G tmpfs {1}".format(Hadoop.datadirSz, Hadoop.datadir), root=True)
mntClients.start()
mntClients.join()

xmlProp = lambda key, value: "<property><name>" + key  +"</name><value>" + value + "</value></property>\n"

# modify core-site.xml
coreSiteXml = '{0}/etc/hadoop/core-site.xml'.format(Hadoop.hadoopdir)
with open(coreSiteXml, 'w+') as f:
    f.write("<configuration>\n")
    f.write(xmlProp("fs.default.name", "hdfs://{0}:{1}".format(Hadoop.namenode, Hadoop.hdfsport)))
    f.write(xmlProp("hadoop.tmp.dir", Hadoop.datadir))
    f.write("</configuration>")

# hadoop_env.sh
hadoopEnv = '{0}/etc/hadoop/hadoop-env.sh'.format(Hadoop.hadoopdir)
with open(hadoopEnv, 'a') as f:
    f.write("export JAVA_HOME={0}\n".format(General.javahome))
    f.write("export HADOOP_LOG_DIR={0}\n".format(Hadoop.datadir))
    f.write("export HADOOP_SECURE_DN_LOG_DIR={0}\n".format(Hadoop.datadir))
    f.write("export HADOOP_CONF_DIR={0}/etc/hadoop/\n".format(Hadoop.hadoopdir))

# hdfs-site.xml
hdfsSiteXml = '{0}/etc/hadoop/hdfs-site.xml'.format(Hadoop.hadoopdir)
with open(hdfsSiteXml, 'w+') as f:
   f.write("<configuration>\n")
   f.write(xmlProp("dfs.replication", Hadoop.dfsreplication))
   f.write(xmlProp("dfs.permissions", "true"))
   f.write(xmlProp("dfs.namenode.rpc-address", "{0}:{1}".format(Hadoop.namenode, Hadoop.hdfsport)))
   f.write("</configuration>")

# master file
masterFile = open('{0}/etc/hadoop/masters'.format(Hadoop.hadoopdir), 'w')
masterFile.write(Hadoop.namenode)
masterFile.close()

# slaves file
slavesFile = '{0}/etc/hadoop/slaves'.format(Hadoop.hadoopdir)
with open(slavesFile, 'w') as f:
   for host in Hadoop.datanodes:
      f.write(host + "\n")

print dfs_start_cmd
nnClient = ThreadedClients([Hadoop.namenode], dfs_start_cmd, root=True)
nnClient.start()
nnClient.join()

#time.sleep(2)

print nn_format_cmd
#nnFormat = ThreadedClient(Hadoop.namenode, nn_format_cmd, root=True)
#nnFormat.start()
#nnFormat.join()
