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
from ServerConfig import Ramcloud

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
         f.write(xmlProp("hbase.rootdir", "hdfs://" + Storage.master + ":" + Hadoop.hdfsport + "/hbase"))
         f.write(xmlProp("hbase.cluster.distributed", "true"))
         f.write(xmlProp("zookeeper.znode.parent", "/hbase-unsecure"))
         f.write(xmlProp("hbase.zookeeper.property.dataDir", Zookeeper.datadir))
         f.write(xmlProp("hbase.hregion.max.filesize", Hbase.regionsize))
         f.write(xmlProp("hbase.zookeeper.quorum", concatStr([Storage.master], ',')))
         f.write(xmlProp("hbase.zookeeper.property.clientPort", Zookeeper.clientport))
         f.write(xmlProp("hbase.coprocessor.region.classes", "org.apache.hadoop.hbase.coprocessor.AggregateImplementation"))
         f.write("</configuration>\n")
    copyToHost([Storage.master] + Storage.servers, hbaseSiteXml)

def prepHbaseEnv():
    hbaseEnv = '{0}/conf/hbase-env.sh'.format(Hbase.hbasedir)
    with open(hbaseEnv, 'w+') as f:
         f.write("export JAVA_HOME={0}\n".format(General.javahome))
         f.write("export HBASE_OPTS=\"-XX:+UseConcMarkSweepGC\"\n")
         f.write("export HBASE_OPTS=\"-XX:+UseConcMarkSweepGC\"\n")
         f.write("export HBASE_MASTER_OPTS=\"$HBASE_MASTER_OPTS -XX:PermSize=128m -XX:MaxPermSize=128m\"\n")
         f.write("export HBASE_REGIONSERVER_OPTS=\"$HBASE_REGIONSERVER_OPTS -XX:PermSize=129m -XX:MaxPermSize=128m\"\n")
         f.write("export HBASE_MANAGES_ZK=false")
    copyToHost([Storage.master] + Storage.servers, hbaseEnv)

# conf/regionservers
def prepRegionServers():
    # not sure whether that region file is actually used in the master
    regions = '{0}/conf/regionservers'.format(Hbase.hbasedir)
    with open(regions, 'w+') as f:
         f.write(concatStr(Storage.servers, '\n'))
    copyToHost([Storage.master], regions)

def startZk():
    zk_cmd = 'numactl -m 0 -N 0 {0}/bin/zkServer.sh start-foreground'.format(Zookeeper.zkdir)
    zkClient = ThreadedClients([Storage.master], zk_cmd, root=True)
    zkClient.start()
    time.sleep(30)
    return zkClient

def confZk():
    zooCfg = '{0}/conf/zoo.cfg'.format(Zookeeper.zkdir)
    with open(zooCfg, 'w+') as f:
         f.write("maxClientCnxns={0}\n".format(Zookeeper.maxclients))
         f.write("tickTime={0}\n".format(Zookeeper.ticktime))
         f.write("dataDir={0}\n".format(Zookeeper.datadir))
         f.write("clientPort={0}\n".format(Zookeeper.clientport))

    copyClient = ThreadedClients([Storage.master], "mkdir -p {0}".format(Zookeeper.datadir), root=True)
    copyClient.start()
    copyClient.join()
    copyToHost([Storage.master], zooCfg)

def confHbase():
    prepRegionServers()
    prepHbaseSite()
    prepHbaseEnv()

def startHbase():
    master_cmd = "JAVA_HOME={1} numactl -m 0 -N 0 {0}/bin/hbase-daemon.sh foreground_start master".format(Hbase.hbasedir, General.javahome)
    masterClient = ThreadedClients([Storage.master], master_cmd, root=True)
    masterClient.start()
    time.sleep(30)

    # start region servers on numa 0
    region_cmd = "JAVA_HOME={1} numactl -m 0 -N 0 {0}/bin/hbase-daemon.sh foreground_start regionserver".format(Hbase.hbasedir, General.javahome)
    regionClients= ThreadedClients(Storage.servers, region_cmd, root=True)
    regionClients.start()
    time.sleep(30)

    # start region servers on numa 1
    region_cmd = "JAVA_HOME={1} numactl -m 1 -N 1 {0}/bin/hbase-daemon.sh foreground_start regionserver -Dhadoop.tmp.dir={1} -Ddfs.datanode.address=0.0.0.0:50011 -Ddfs.datanode.http.address=0.0.0.0:50081 -Ddfs.datanode.ipc.address=0.0.0.0:50021".format(Hbase.hbasedir, General.javahome)
    regionClients1= ThreadedClients(Storage.servers1, region_cmd, root=True)
    regionClients1.start()
    time.sleep(30)

    return [masterClient, regionClients, regionClients1]

def confHdfs():
    # mount tmpfs for master and servers on numa 0
    mkClients = ThreadedClients([Storage.master] + Storage.servers, "mkdir -p {0}; mount -t tmpfs -o size={1}G tmpfs {0}".format(Hadoop.datadir, Hadoop.datadirSz), root=True)
    mkClients.start()
    mkClients.join()

    # mount tmpfs for servers on numa 1
    mkClients = ThreadedClients(Storage.servers1, "mkdir -p {0}; mount -t tmpfs -o size={1}G tmpfs {0}".format(Hadoop.datadir1, Hadoop.datadirSz), root=True)
    mkClients.start()
    mkClients.join()

    # modify core-site.xml
    coreSiteXml = '{0}/etc/hadoop/core-site.xml'.format(Hadoop.hadoopdir)
    with open(coreSiteXml, 'w+') as f:
        f.write("<configuration>\n")
        f.write(xmlProp("fs.default.name", "hdfs://{0}:{1}".format(Storage.master, Hadoop.hdfsport)))
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
       f.write(xmlProp("dfs.namenode.rpc-address", "{0}:{1}".format(Storage.master, Hadoop.hdfsport)))
       f.write("</configuration>")

    copyToHost(Storage.servers + Storage.servers1 + [Storage.master], coreSiteXml)
    copyToHost(Storage.servers + Storage.servers1 + [Storage.master], hadoopEnv)
    copyToHost(Storage.servers + Storage.servers1 + [Storage.master], hdfsSiteXml)

    # master file - probably not used anymore as we do not use start-dfs.sh
    masterFile = open('{0}/etc/hadoop/masters'.format(Hadoop.hadoopdir), 'w')
    masterFile.write(Storage.master)
    masterFile.close()

    # slaves file - probably not used anymore as we do not use start-dfs.sh
    slavesFile = '{0}/etc/hadoop/slaves'.format(Hadoop.hadoopdir)
    with open(slavesFile, 'w+') as f:
       for host in Storage.servers:
          f.write(host + "\n")
    
    copyToHost([Storage.master], slavesFile)
    copyToHost([Storage.master], masterFile)

def startHdfs():
    nn_format_cmd = "numactl -m 0 -N 0 {0}/bin/hadoop namenode -format".format(Hadoop.hadoopdir)
    nnFormatClients = ThreadedClients([Storage.master], nn_format_cmd, root=True)
    nnFormatClients.start()
    nnFormatClients.join()

    nn_start_cmd = "numactl -m 0 -N 0 {0}/bin/hdfs namenode".format(Hadoop.hadoopdir)
    nnClient = ThreadedClients([Storage.master], nn_start_cmd, root=True)
    nnClient.start()
    time.sleep(30)

    # start slaves on numa 0
    dn_start_cmd = "numactl -m 0 -N 0 {0}/bin/hdfs datanode".format(Hadoop.hadoopdir)
    dnClients = ThreadedClients(Storage.servers, dn_start_cmd, root=True)
    dnClients.start()
    time.sleep(30)

    # start slaves on numa 1
    dn_start_cmd = "numactl -m 1 -N 1 {0}/bin/hdfs datanode -Dhadoop.tmp.dir={1} -Ddfs.datanode.address=0.0.0.0:50011 -Ddfs.datanode.http.address=0.0.0.0:50081 -Ddfs.datanode.ipc.address=0.0.0.0:50021".format(Hadoop.hadoopdir, Hadoop.datadir1)
    dn1Clients = ThreadedClients(Storage.servers1, dn_start_cmd, root=True)
    dn1Clients.start()
    time.sleep(30)

    return [nnClient, dnClients, dn1Clients]

def confHbaseCluster():
    confHdfs()
    confZk()
    confHbase()

def startHbaseThreads():
    hdfsClients = startHdfs()
    zkClient = startZk()
    hbaseClients = startHbase()
    return [hdfsClients, zkClient, hbaseClients]

def confCassandraCluster():
    for numaNode in [0,1]:
       servers = Storage.servers if numaNode == 0 else Storage.servers1
       datadir = Cassandra.datadir if numaNode == 0 else Cassandra.datadir1
       logdir = Cassandra.logdir if numNodes == 0 else Cassandra.logdir1
       nativeport = Cassandra.nativeport if numaNode == 0 else Cassandra.nativeport1
       rpcport = Cassandra.rpcport if numaNode == 0 else Cassandra.rpcport1
       storageport = Cassandra.storageport if numaNode == 0 else Cassandra.storageport1
       sslport = Cassandra.sslport if numaNode == 0 else Cassandra.sslport1

       if len(server) == 0:
           continue

       for host in servers:
          f = open('cassandra.yaml.template', 'r')
          templateconf = f.read()
          f.close()

          templateconf = templateconf.replace("casseeds", "\"" + Storage.servers[0] + "\"")
          templateconf = templateconf.replace("caslistenaddr", host)
          templateconf = templateconf.replace("casdatadir", datadir)
          templateconf = templateconf.replace("caslogdir", logdir)
          templateconf = templateconf.replace("casnativeport", nativeport)
          templateconf = templateconf.replace("casrpcport", rpcport)
          templateconf = templateconf.replace("casstorageport", storageport)
          templateconf = templateconf.replace("cassslport", sslport)

          cassandraConf = '{0}/conf/cassandra.yaml'.format(Cassandra.casdir)
          with open(cassandraConf, 'w') as f:
             f.write(templateconf)
             f.close()
          copyToHost([host], cassandraConf)
       
       mkClients = ThreadedClients(servers, "mkdir -p {0}".format(datadir), root=True)
       mkClients.start()
       mkClients.join()
       mntClients = ThreadedClients(servers, "mount -t tmpfs -o size={0}G tmpfs {1}".format(Cassandra.datadirSz, datadir), root=True)
       mntClients.start()
       mntClients.join()
       mkClients = ThreadedClients(servers , "mkdir -p {0}".format(logdir), root=True)
       mkClients.start()
       mkClients.join()

def startCassandra(obs):
    start_cas_cmd = "{0}/bin/cassandra -f".format(Cassandra.casdir)
    seedClient = ThreadedClients([Storage.servers[0]], "numactl -m 0 -N 0 {0}".format(start_cas_cmd), observers=obs)
    seedClient.start()
    print "waiting for seeds"
    time.sleep(60)

    nodeClients = []
    # startup remaining nodes on NUMA 0
    if len(Storage.servers) > 1:
        for server in Storage.servers[1:]:
            nodeClient = ThreadedClients([server], "numactl -m 0 -N 0 {0}".format(start_cas_cmd), observers=obs)
            nodeClient.start()
            time.sleep(60)
            nodeClients = nodeClients + [nodeClient]

    if len(Storage.servers1) > 0:
    # startup nodes on NUMA 1
        for server in Storage.servers1:
            nodeClient = ThreadedClients([server], "numactl -m 1 -N 1 {0}".format(start_cas_cmd), observers=obs)
            nodeClient.start()
            time.sleep(60)
            nodeClients = nodeClients + [nodeClient]

    return [seedClient, nodeClients]

def confRamcloud():
    copyClient = ThreadedClients(Storage.servers + Storage.servers1, "mkdir -p {0}".format(Ramcloud.backupdir), root=True)
    copyClient.start()
    copyClient.join()
    
    deleteClient = ThreadedClients(Storage.servers, "rm -rf {0} {1}".format(Ramcloud.backupfile, Ramcloud.backupfile1), root=True)
    deleteClient.start()
    deleteClient.join()

    confZk()

def startRamcloud(obs):
    startZk()
    master_cmd = "numactl -m 0 -N 0 {0}/coordinator -C infrc:host={1}-infrc,port=11100 -x zk:{1}:{2}".format(Ramcloud.ramclouddir, Storage.master, Zookeeper.clientport)
    masterClient = ThreadedClients([Storage.master], master_cmd, root=True)
    masterClient.start()
    time.sleep(30)

    nodeClients = []

    storage_cmd = "numactl -m 0 -N 0 {0}/server -L infrc:host={3}-infrc,port={4} -x zk:{1}:{2} --totalMasterMemory {5} -f {6} --segmentFrames 10000 -r 0"
    storage_cmd = storage_cmd.format(Ramcloud.ramclouddir, Storage.master, Zookeeper.clientport, "{0}", Ramcloud.storageport, Ramcloud.memorysize, Ramcloud.backupfile)

    # startup nodes on NUMA 0
    for server in Storage.servers:
        nodeClient = ThreadedClients([server],  storage_cmd.format(server), observers=obs)
        nodeClient.start()
        nodeClients = nodeClients + [nodeClient]

    # startup nodes on NUMA 1
    if len(Storage.servers1) > 0:
        storage_cmd = "numactl -m 1 -N 1 {0}/server -L infrc:host={3}-infrc,port={4} -x zk:{1}:{2} --totalMasterMemory {5} -f {6} --segmentFrames 10000 -r 0"
        storage_cmd = storage_cmd.format(Ramcloud.ramclouddir, Storage.master, Zookeeper.clientport, "{0}", Ramcloud.storageport1, Ramcloud.memorysize, Ramcloud.backupfile1)
        for server in Storage.servers1:
            nodeClient = ThreadedClients([server],  storage_cmd.format(server), observers=obs)
            nodeClient.start()
            nodeClients = nodeClients + [nodeClient]

    time.sleep(60)

    return [masterClient, nodeClients]

def startStorageThreads(master_cmd, server_cmd, numa1Args, obs):
    mclient = ThreadedClients([Storage.master], "numactl -m 0 -N 0 {0}".format(master_cmd), observers=obs)
    mclient.start()

    # if storage is not Tell, we wait a small amount of time to let the system start up (for Tell we can use the observer instead)
    if (Storage.storage != TellStore):
        time.sleep(2)
    
    tclient = ThreadedClients(Storage.servers, "numactl -m 0 -N 0 {0}".format(server_cmd), observers=obs)
    tclient.start()

    # servers1 should only exist for Tell, nobody else
    tclient1 = ThreadedClients(Storage.servers1, 'numactl -m 1 -N 1 {0} {1}'.format(server_cmd, numa1Args), observers=obs)
    tclient1.start()
    
    # if storage is not Tell, we wait a small amount of time to let the system start up (for Tell we can use the observer instead)
    if (Storage.storage != TellStore):
        time.sleep(2)
    
    return [mclient, tclient, tclient1]

def startStorage(observers = []):
    if Storage.storage == Kudu:
        master_dir = Kudu.master_dir
        tserver_dir = Kudu.tserver_dir
    
        master_cmd  = '/mnt/local/tell/kudu_install/bin/kudu-master --fs_data_dirs={0} --fs_wal_dir={0} --block_manager=file'.format(master_dir)
        server_cmd = '/mnt/local/tell/kudu_install/bin/kudu-tserver --fs_data_dirs={0} --fs_wal_dir={0} --block_cache_capacity_mb 51200 --tserver_master_addrs {1}'.format(tserver_dir, Storage.master)
        numa1Args = "--rpc_bind_addresses=0.0.0.0:7049"
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
    elif Storage.storage == Ramcloud:
        confRamcloud()
        return startRamcloud(observers)
    elif Storage.storage == Hadoop:
        confHdfs()
        return startHdfs()
    elif Storage.storage == Hbase:
        confHbaseCluster()
        return startHbaseThreads()
    elif Storage.storage == Cassandra:
        confCassandraCluster()
        return startCassandra(observers)
    return startStorageThreads(master_cmd, server_cmd, numa1Args, observers)
        
if __name__ == "__main__":
    startStorage()
