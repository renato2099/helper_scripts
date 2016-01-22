#!/usr/bin/env python
from pssh import ParallelSSHClient
from threaded_ssh import ThreadedClients

from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Kudu
from ServerConfig import TellStore
from ServerConfig import Hadoop
import logging

logging.basicConfig()

master = Storage.master
servers = Storage.servers
master_cmd = ""
server_cmd = ""

numa1Args = ''

def startHdfs():
    dfs_start_cmd ="{0}/sbin/start-dfs.sh".format(Hadoop.hadoopdir)
    nn_format_cmd = "{0}/bin/hadoop namenode -format".format(Hadoop.hadoopdir)
    
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
    
    print nn_format_cmd
    os.system('ssh root@{0} {1}'.format(Hadoop.namenode, nn_format_cmd))
    print dfs_start_cmd
    os.system('ssh root@{0} {1}'.format(Hadoop.namenode, dfs_start_cmd))

if Storage.storage == Kudu:
    master_dir = Kudu.master_dir
    tserver_dir = Kudu.tserver_dir

    master_cmd  = '/mnt/local/tell/kudu_install/bin/kudu-master --fs_data_dirs={0} --fs_wal_dir={0} --block_manager=file'.format(master_dir)
    server_cmd = '/mnt/local/tell/kudu_install/bin/kudu-tserver --fs_data_dirs={0} --fs_wal_dir={0} --block_cache_capacity_mb 51200 --tserver_master_addrs {1}'.format(tserver_dir, master)
    if Kudu.clean:
        rmcommand = 'rm -rf {0}/*'
        master_client = ParallelSSHClient([master], user="root")
        output = master_client.run_command(rmcommand.format(master_dir))
        tserver_client = ParallelSSHClient(servers, user="root")
        tservers_out = tserver_client.run_command(rmcommand.format(tserver_dir))
        for host in output:
            for line in output[host]['stdout']:
                print "Host {0}: {1}".format(host, line)
    
            for line in output[host]['stderr']:
                print "{0}Host {1}: {2}".format(self.FAIL, host, line)
        for host in tservers_out:
            for line in tservers_out[host]['stdout']:
                print "Host {0}: {1}".format(host, line)
    
            for line in tservers_out[host]['stderr']:
                print "{0}Host {1}: {2}".format(self.FAIL, host, line)
elif Storage.storage == TellStore:
    master_cmd = "{0}/commitmanager/server/commitmanagerd".format(TellStore.builddir)
    server_cmd = "{0}/tellstore/server/tellstored-{1} -l INFO --scan-threads {2} --network-threads 1 --gc-interval {5} -m {3} -c {4}".format(TellStore.builddir, TellStore.approach, TellStore.scanThreads, TellStore.memorysize, TellStore.hashmapsize, TellStore.gcInterval)
    numa1Args = '-p 7240'
elif Storage.storage == Hadoop:
    startHdfs()
    return

mclient = ThreadedClients([master], "numactl -m 0 -N 0 {0}".format(master_cmd))
mclient.start()

tclient = ThreadedClients(servers, "numactl -m 0 -N 0 {0}".format(server_cmd))
tclient.start()

tclient2 = ThreadedClients(Storage.servers1, 'numactl -m 1 -N 1 {0} {1}'.format(server_cmd, numa1Args))
tclient2.start()

mclient.join()
tclient.join()
tclient2.join()


