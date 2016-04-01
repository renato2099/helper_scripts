#!/usr/bin/env python
import os
import time
from threaded_ssh import ThreadedClients
from pssh import ParallelSSHClient

from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Kudu
from ServerConfig import TellStore
from ServerConfig import Hadoop
from ServerConfig import Zookeeper
from ServerConfig import Hbase
from ServerConfig import Cassandra

import logging

logging.basicConfig()

concatStr = lambda servers, sep: sep.join(servers)
xmlProp = lambda key, value: "<property><name>" + key  +"</name><value>" + value + "</value></property>\n"

def copyToHost(hosts, path):
    for host in hosts:
        os.system('scp {0} root@{1}:{0}'.format(path, host))

# modify hbase-site.xml
def prepHbaseSite():
    hbaseSiteXml = '{0}/conf/hbase-site.xml'.format(Hbase.hbasedir)
    with open(hbaseSiteXml, 'w+') as f:
         f.write("<?xml version=\"1.0\"?>\n")
         f.write("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n")
         f.write("<configuration>\n")
         f.write(xmlProp("hbase.rootdir", "hdfs://" + Hadoop.namenode + ":" + Hadoop.hdfsport + "/hbase"))
         f.write(xmlProp("hbase.cluster.distributed", "true"))
         f.write(xmlProp("zookeeper.znode.parent", "/hbase-unsecure"))
         f.write(xmlProp("hbase.zookeeper.property.dataDir", Zookeeper.datadir))
         f.write(xmlProp("hbase.zookeeper.quorum", concatStr([Zookeeper.zkserver], ',')))
         f.write(xmlProp("hbase.hregion.max.filesize", Hbase.regionsize))
         f.write(xmlProp("hbase.zookeeper.property.clientPort", Zookeeper.clientport))
         f.write("</configuration>\n")
    copyToHost([Hbase.hmaster] + Hbase.hregions, hbaseSiteXml)

def prepHbaseEnv():
    hbaseEnv = '{0}/conf/hbase-env.sh'.format(Hbase.hbasedir)
    with open(hbaseEnv, 'w+') as f:
         f.write("export JAVA_HOME={0}\n".format(General.javahome))
         f.write("export HBASE_OPTS=\"-XX:+UseConcMarkSweepGC\"\n")
         f.write("export HBASE_OPTS=\"-XX:+UseConcMarkSweepGC\"\n")
         f.write("export HBASE_MASTER_OPTS=\"$HBASE_MASTER_OPTS -XX:PermSize=128m -XX:MaxPermSize=128m\"\n")
         f.write("export HBASE_REGIONSERVER_OPTS=\"$HBASE_REGIONSERVER_OPTS -XX:PermSize=129m -XX:MaxPermSize=128m\"\n")
         f.write("export HBASE_MANAGES_ZK=false")
    copyToHost([Hbase.hmaster] + Hbase.hregions, hbaseEnv)

# conf/regionservers
def prepRegionServers():
    regions = '{0}/conf/regionservers'.format(Hbase.hbasedir)
    with open(regions, 'w+') as f:
         f.write(concatStr(Hbase.hregions, '\n'))
    copyToHost([Hbase.hmaster], regions)


def startZk():
    zooCfg = '{0}/conf/zoo.cfg'.format(Zookeeper.zkdir)
    with open(zooCfg, 'w+') as f:
         f.write("maxClientCnxns={0}\n".format(Zookeeper.maxclients))
         f.write("tickTime={0}\n".format(Zookeeper.ticktime))
         f.write("dataDir={0}\n".format(Zookeeper.datadir))
         f.write("clientPort={0}\n".format(Zookeeper.clientport))
    os.system('ssh -A root@{0} {1}'.format(Zookeeper.zkserver, "mkdir -p {0}".format(Zookeeper.datadir)))
    zk_cmd = '{0}/bin/zkServer.sh start'.format(Zookeeper.zkdir)
    copyToHost([Zookeeper.zkserver], zooCfg)
    print "{0} : {1}".format(Zookeeper.zkserver, zk_cmd)
    os.system('ssh -A root@{0} {1}'.format(Zookeeper.zkserver, zk_cmd))

def startHbase():
    prepRegionServers()
    prepHbaseSite()
    prepHbaseEnv()
    start_hbase_cmd = "JAVA_HOME={1} {0}/bin/start-hbase.sh".format(Hbase.hbasedir, General.javahome)
    os.system('ssh -A root@{0} {1}'.format(Hbase.hmaster, start_hbase_cmd))

def startHdfs():
    dfs_start_cmd ="{0}/sbin/start-dfs.sh".format(Hadoop.hadoopdir)
    nn_format_cmd = "{0}/bin/hadoop namenode -format".format(Hadoop.hadoopdir)

    mkClients = ThreadedClients([Hadoop.namenode] + Hadoop.datanodes, "mkdir -p {0}".format(Hadoop.datadir), root=True)
    mkClients.start()
    mkClients.join()

    mntClients = ThreadedClients([Hadoop.namenode] + Hadoop.datanodes, "mount -t tmpfs -o size={0}G tmpfs {1}".format(Hadoop.datadirSz, Hadoop.datadir), root=True)
    mntClients.start()
    mntClients.join()

    time.sleep(2)

    # modify core-site.xml
    coreSiteXml = '{0}/etc/hadoop/core-site.xml'.format(Hadoop.hadoopdir)
    with open(coreSiteXml, 'w+') as f:
        f.write("<configuration>\n")
        f.write(xmlProp("fs.default.name", "hdfs://{0}:{1}".format(Hadoop.namenode, Hadoop.hdfsport)))
        f.write(xmlProp("hadoop.tmp.dir", Hadoop.datadir))
        f.write(xmlProp("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem"))
        f.write(xmlProp("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem"))
        f.write("</configuration>")

    # hadoop_env.sh
    hadoopEnv = '{0}/etc/hadoop/hadoop-env.sh'.format(Hadoop.hadoopdir)
    with open(hadoopEnv, 'w+') as f:
        f.write('export HADOOP_NAMENODE_OPTS="-Dhadoop.security.logger=INFO,DRFAS -Dhdfs.audit.logger=INFO,DRFAAUDIT $HADOOP_NAMENODE_OPTS"\n')
        f.write('HADOOP_JOBTRACKER_OPTS="-Dhadoop.security.logger=INFO,DRFAS -Dmapred.audit.logger=INFO,MRAUDIT -Dhadoop.mapreduce.jobsummary.logger=INFO,JSA $HADOOP_JOBTRACKER_OPTS"\n')
        f.write('HADOOP_TASKTRACKER_OPTS="-Dhadoop.security.logger=ERROR,console -Dmapred.audit.logger=ERROR,console $HADOOP_TASKTRACKER_OPTS"\n')
        f.write('HADOOP_DATANODE_OPTS="-Dhadoop.security.logger=ERROR,DRFAS $HADOOP_DATANODE_OPTS"\n')
        f.write('export HADOOP_SECONDARYNAMENODE_OPTS="-Dhadoop.security.logger=INFO,DRFAS -Dhdfs.audit.logger=INFO,DRFAAUDIT $HADOOP_SECONDARYNAMENODE_OPTS"\n')
        f.write('export HADOOP_OPTS="-Djava.net.preferIPv4Stack=true $HADOOP_CLIENT_OPTS"\n')
        f.write('export HADOOP_CLIENT_OPTS="-Xmx2048m $HADOOP_CLIENT_OPTS"\n')
        f.write('export HADOOP_SECURE_DN_USER=\n')
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

    copyToHost(Hadoop.datanodes + [Hadoop.namenode], coreSiteXml)
    copyToHost(Hadoop.datanodes + [Hadoop.namenode], hadoopEnv)
    copyToHost(Hadoop.datanodes + [Hadoop.namenode], hdfsSiteXml)

    # master file
    masterFile = open('{0}/etc/hadoop/masters'.format(Hadoop.hadoopdir), 'w')
    masterFile.write(Hadoop.namenode)
    masterFile.close()

    # slaves file
    slavesFile = '{0}/etc/hadoop/slaves'.format(Hadoop.hadoopdir)
    with open(slavesFile, 'w+') as f:
       for host in Hadoop.datanodes:
          f.write(host + "\n")
    
    copyToHost([Hadoop.namenode], slavesFile)
    copyToHost([Hadoop.namenode], masterFile)

    print nn_format_cmd
    os.system('ssh -A root@{0} {1}'.format(Hadoop.namenode, nn_format_cmd))
    print dfs_start_cmd
    os.system('ssh -A root@{0} {1}'.format(Hadoop.namenode, dfs_start_cmd))

def confCassandraCluster():
    templateconf = None
    allServers = Storage.servers 
    for host in allServers:
       f = open('cassandra.yaml.template', 'r')
       templateconf = f.read()
       f.close()
       templateconf = templateconf.replace("casseeds", "\"" + allServers[0] + "\"")
       templateconf = templateconf.replace("casdatadir", Cassandra.datadir)
       templateconf = templateconf.replace("caslogdir", Cassandra.logdir)
       templateconf = templateconf.replace("caslistenaddr", host)
       templateconf = templateconf.replace("casnativeport", Cassandra.nativeport)
       templateconf = templateconf.replace("casrpcaddr", Cassandra.rpcaddr)
       templateconf = templateconf.replace("casrpcport", Cassandra.rpcport)
       cassandraConf = '{0}/conf/cassandra.yaml'.format(Cassandra.casdir)
       with open(cassandraConf, 'w') as f:
          f.write(templateconf)
          f.close()
       copyToHost([host], cassandraConf)
    
    mkClients = ThreadedClients(allServers, "mkdir -p {0}".format(Cassandra.datadir), root=True)
    mkClients.start()
    mkClients.join()
    time.sleep(2)
    mntClients = ThreadedClients(allServers, "mount -t tmpfs -o size={0}G tmpfs {1}".format(Cassandra.datadirSz, Cassandra.datadir), root=True)
    mntClients.start()
    mntClients.join()
    time.sleep(2)
    mkClients = ThreadedClients(allServers , "mkdir -p {0}".format(Cassandra.logdir), root=True)
    mkClients.start()
    mkClients.join()
    time.sleep(2)

def startCassandra(start_cas_cmd, obs):
    seedClient = ThreadedClients([Storage.servers[0]], start_cas_cmd, observers=obs)
    seedClient.start()

    print "waiting for seeds"
    time.sleep(60)

    nodeClients = []
    if len(Storage.servers) > 1:
        nodeClients = ThreadedClients(Storage.servers[1:], start_cas_cmd, observers=obs)
        nodeClients.start()

    return [seedClient, nodeClients]

def startStorageThreads(master_cmd, server_cmd, obs):
    mclient = ThreadedClients([Storage.master], "numactl -m 0 -N 0 {0}".format(master_cmd), observers=obs)
    mclient.start()
    
    tclient = ThreadedClients(Storage.servers, "numactl -m 0 -N 0 {0}".format(server_cmd), observers=obs)
    tclient.start()
    
    tclient2 = ThreadedClients(Storage.servers1, 'numactl -m 1 -N 1 {0} {1}'.format(server_cmd, '-p 7240'), observers=obs)
    tclient2.start()
    
    return [mclient, tclient, tclient2]

def startStorage(observers = []):
    if Storage.storage == Kudu:
        master_dir = Kudu.master_dir
        tserver_dir = Kudu.tserver_dir
    
        master_cmd  = '/mnt/local/tell/kudu_install/bin/kudu-master --fs_data_dirs={0} --fs_wal_dir={0} --block_manager=file'.format(master_dir)
        server_cmd = '/mnt/local/tell/kudu_install/bin/kudu-tserver --fs_data_dirs={0} --fs_wal_dir={0} --block_cache_capacity_mb 51200 --tserver_master_addrs {1}'.format(tserver_dir, Storage.master)
        if Kudu.clean:
            rmcommand = 'rm -rf {0}/*'
            master_client = ParallelSSHClient([Storage.master], user="root")
            output = master_client.run_command(rmcommand.format(master_dir))
            tserver_client = ParallelSSHClient(Storage.servers, user="root")
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
        TellStore.rsyncBuild()
        master_cmd = "{0}/commitmanager/server/commitmanagerd".format(TellStore.builddir)
        server_cmd = "{0}/tellstore/server/tellstored-{1} -l INFO --scan-threads {2} --network-threads 1 --gc-interval {5} -m {3} -c {4}".format(TellStore.builddir, TellStore.approach, TellStore.scanThreads, TellStore.memorysize, TellStore.hashmapsize, TellStore.gcInterval)
        numa1Args = '-p 7240'
    elif Storage.storage == Hadoop:
        startHdfs()
        exit(0)
    elif Storage.storage == Hbase:
        startHdfs()
        startZk()
        startHbase()
        exit(0)
    elif Storage.storage == Cassandra:
        confCassandraCluster()
        start_cas_cmd = "numactl -m 0 -N 0 {0}/bin/cassandra -f".format(Cassandra.casdir)
        return startCassandra(start_cas_cmd, observers)
    return startStorageThreads(master_cmd, server_cmd, observers)
        
if __name__ == "__main__":
    startStorage()
