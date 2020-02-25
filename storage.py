#!/usr/bin/env python
import sys
import signal
import os
import sys
import time

from threaded_ssh import ThreadedClients
from observer import *

from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Kudu
from ServerConfig import TellStore
from ServerConfig import Hadoop
from ServerConfig import Zookeeper
from ServerConfig import Hbase
from ServerConfig import Cassandra
from ServerConfig import Ramcloud

from functools import partial

from unmount_memfs import unmount_memfs

import logging

if 'threading' in sys.modules:
        del sys.modules['threading']
        import gevent
        import gevent.socket
        import gevent.monkey
        gevent.monkey.patch_all()

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
    copyToHost([Storage.master] + Storage.servers + Storage.servers1, hbaseSiteXml)

def prepHbaseEnv():
    hbaseEnv = '{0}/conf/hbase-env.sh'.format(Hbase.hbasedir)
    with open(hbaseEnv, 'w+') as f:
         f.write("export JAVA_HOME={0}\n".format(General.javahome))
         f.write("export HBASE_OPTS=\"-XX:+UseConcMarkSweepGC\"\n")
         f.write("export HBASE_OPTS=\"-XX:+UseConcMarkSweepGC\"\n")
         f.write("export HBASE_MASTER_OPTS=\"$HBASE_MASTER_OPTS -XX:PermSize=128m -XX:MaxPermSize=128m\"\n")
         f.write("export HBASE_REGIONSERVER_OPTS=\"$HBASE_REGIONSERVER_OPTS -XX:PermSize=129m -XX:MaxPermSize=128m\"\n")
         f.write("export HBASE_MANAGES_ZK=false")
    copyToHost([Storage.master] + Storage.servers + Storage.servers1, hbaseEnv)

# conf/regionservers
def prepRegionServers():
    # not sure whether that region file is actually used in the master
    regions = '{0}/conf/regionservers'.format(Hbase.hbasedir)
    with open(regions, 'w+') as f:
         f.write(concatStr(Storage.servers + Storage.servers1, '\n'))
    copyToHost([Storage.master], regions)

def startZk():
    observer = Observer("binding to port")
    zk_cmd = 'numactl -m 0 -N 0 {0}/bin/zkServer.sh start-foreground'.format(Zookeeper.zkdir)
    zkClient = ThreadedClients([Storage.master], zk_cmd, root=True, observers=[observer])
    zkClient.start()
    observer.waitFor(1)
    return zkClient

def confZk():
    zooCfg = '{0}/conf/zoo.cfg'.format(Zookeeper.zkdir)
    with open(zooCfg, 'w+') as f:
         f.write("maxClientCnxns={0}\n".format(Zookeeper.maxclients))
         f.write("tickTime={0}\n".format(Zookeeper.ticktime))
         f.write("dataDir={0}\n".format(Zookeeper.datadir))
         f.write("clientPort={0}\n".format(Zookeeper.clientport))

    deleteClient = ThreadedClients([Storage.master], "rm -rf {0}".format(Zookeeper.datadir), root=True)
    deleteClient.start()
    deleteClient.join()

    copyClient = ThreadedClients([Storage.master], "mkdir -p {0}".format(Zookeeper.datadir), root=True)
    copyClient.start()
    copyClient.join()
    copyToHost([Storage.master], zooCfg)

def confHbase():
    prepRegionServers()
    prepHbaseSite()
    prepHbaseEnv()

def confHdfs():

    # mount tmpfs for master and servers on numa 0
    cmdHadoopDir = "rm -r {0}; mkdir -p {0}".format(Hadoop.datadir) if not Hadoop.ramfs else "mkdir -p {0}; mount -t tmpfs -o size={1}G tmpfs {0}".format(Hadoop.datadir, Hadoop.datadirSz)
    #cmdHadoopDir = "rm -r {0}; mkdir -p {0}".format(Hadoop.datadir) if not Hadoop.ramfs else "mkdir -p {0}; mount -t tmpfs -o size={1}G tmpfs {0}".format(Hadoop.datadir, Hadoop.datadirSz)
    mkClients = ThreadedClients([Storage.master] + Storage.servers, cmdHadoopDir, root=True)
    mkClients.start()
    mkClients.join()

    # mount tmpfs for servers on numa 1
    cmdHadoopDir = "echo {0}; mkdir -p {0}".format(Hadoop.datadir1) if not Hadoop.ramfs else "mkdir -p {0}; mount -t tmpfs -o size{1}G tmpfs {0}".format(Hadoop.datadir1, Hadoop.datadirSz)
    #cmdHadoopDir = "rm -r {0}; mkdir -p {0}".format(Hadoop.datadir1) if not Hadoop.ramfs else "mkdir -p {0}; mount -t tmpfs -o size{1}G tmpfs {0}".format(Hadoop.datadir1, Hadoop.datadirSz)
    #mkClients = ThreadedClients(Storage.servers1, "mkdir -p {0}; mount -t tmpfs -o size={1}G tmpfs {0}".format(Hadoop.datadir1, Hadoop.datadirSz), root=True)
    mkClients = ThreadedClients(Storage.servers1, cmdHadoopDir, root=True)
    mkClients.start()
    mkClients.join()
    print("-- DONE setting up Hadoop dirs --")

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
    masterFile = '{0}/etc/hadoop/masters'.format(Hadoop.hadoopdir)
    with open(masterFile, "w+") as f:
        f.write(Storage.master)

    # slaves file - probably not used anymore as we do not use start-dfs.sh
    slavesFile = '{0}/etc/hadoop/slaves'.format(Hadoop.hadoopdir)
    with open(slavesFile, 'w+') as f:
       for host in (Storage.servers + Storage.servers1):
          f.write(host + "\n")
    
    copyToHost([Storage.master], masterFile)
    copyToHost([Storage.master], slavesFile)

    # format namenode
    nn_format_cmd = "numactl -m 0 -N 0 {0}/bin/hdfs namenode -format".format(Hadoop.hadoopdir)
    nnFormatClients = ThreadedClients([Storage.master], nn_format_cmd, root=True)
    nnFormatClients.start()
    nnFormatClients.join()

def confHbaseCluster():
    confZk()
    confHdfs()
    confHbase()


def confCassandraCluster():
    os.system("mkdir -p {0}/conf1".format(Cassandra.casdir))
    dirClient = ThreadedClients(Storage.servers1, "mkdir -p {0}/conf1".format(Cassandra.casdir))
    dirClient.start()
    dirClient.join()

    # copy all conf files over, cassandra.yaml will be overwritten later
    os.system("cp -a {0}/conf/* {0}/conf1/".format(Cassandra.casdir))
    copyClient = ThreadedClients(Storage.servers1, "cp -a {0}/conf/* {0}/conf1/".format(Cassandra.casdir))
    copyClient.start()
    copyClient.join()

    # we also have to change cassandra-env.sh
    f = open('{0}/conf/cassandra-env.sh'.format(Cassandra.casdir), 'r')
    templateEnv = f.read()
    f.close()
    templateEnv = templateEnv.replace('JMX_PORT="7199"', 'JMX_PORT="7198"')
    cassandraEnv = '{0}/conf1/cassandra-env.sh'.format(Cassandra.casdir)
    with open (cassandraEnv, 'w') as f:
        f.write(templateEnv)
        f.close()
    copyToHost(Storage.servers1, cassandraEnv)
        
    for numaNode in [0,1]:
        servers = Storage.servers if numaNode == 0 else Storage.servers1
        datadir = Cassandra.datadir if numaNode == 0 else Cassandra.datadir1
        logdir = Cassandra.logdir if numaNode == 0 else Cassandra.logdir1
        nativeport = Cassandra.nativeport if numaNode == 0 else Cassandra.nativeport1
        rpcport = Cassandra.rpcport if numaNode == 0 else Cassandra.rpcport1
        storageport = Cassandra.storageport if numaNode == 0 else Cassandra.storageport1
        sslport = Cassandra.sslport if numaNode == 0 else Cassandra.sslport1
        cassandraConf = '{0}/conf/cassandra.yaml' if numaNode == 0 else '{0}/conf1/cassandra.yaml'
        cassandraConf = cassandraConf.format(Cassandra.casdir)

        if len(servers) == 0:
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

def startCassandra():
    observerString = "No host ID found"
    start_cas_cmd = "{0}/bin/cassandra -f".format(Cassandra.casdir)
    javaHome = "JAVA_HOME={0}".format(General.javahome)

    # startup seed node
    obs = Observer(observerString)
    seedClient = ThreadedClients([Storage.servers[0]], "{1} numactl -m 0 -N 0 {0}".format(start_cas_cmd, javaHome), observers=[obs])
    seedClient.start()
    obs.waitFor(1)

    nodeClients = []
    # startup remaining nodes on NUMA 0
    if len(Storage.servers) > 1:
        for server in Storage.servers[1:]:
            obs = Observer(observerString)
            nodeClient = ThreadedClients([server], "{1} numactl -m 0 -N 0 {0}".format(start_cas_cmd, javaHome), observers=[obs])
            nodeClient.start()
            obs.waitFor(1)
            nodeClients = nodeClients + [nodeClient]

    if len(Storage.servers1) > 0:
    # startup nodes on NUMA 1
        for server in Storage.servers1:
            obs = Observer(observerString)
            nodeClient = ThreadedClients([server], '{2} CASSANDRA_CONF={1}/conf1 numactl -m 1 -N 1 {0} -Dcassandra.logdir={3}'.format(start_cas_cmd, Cassandra.casdir, javaHome, Cassandra.logdir1), observers=[obs])
            nodeClient.start()
            obs.waitFor(1)
            nodeClients = nodeClients + [nodeClient]

    return nodeClients + [seedClient]

def confRamcloud():
    deleteClient = ThreadedClients(Storage.servers, "rm -rf {0}".format(Ramcloud.backupdir), root=True)
    deleteClient.start()
    deleteClient.join()
    
    deleteClient = ThreadedClients(Storage.servers1, "rm -rf {0}".format(Ramcloud.backupdir1), root=True)
    deleteClient.start()
    deleteClient.join()
    
    copyClient = ThreadedClients(Storage.servers, "mkdir -p {0}".format(Ramcloud.backupdir), root=True)
    copyClient.start()
    copyClient.join()
    
    copyClient = ThreadedClients(Storage.servers1, "mkdir -p {0}".format(Ramcloud.backupdir1), root=True)
    copyClient.start()
    copyClient.join()

    confZk()

def startRamcloud():
    zkClient = startZk()

    master_cmd = "LD_LIBRARY_PATH={3} numactl -m 0 -N 0 {0}/coordinator -C infrc:host={1}-infrc,port=11100 -x zk:{1}:{2} --timeout {4}".format(Ramcloud.ramclouddir, Storage.master, Zookeeper.clientport, Ramcloud.boost_lib, Ramcloud.timeout)

    masterObs = Observer("Memory usage now")
    masterClient = ThreadedClients([Storage.master], master_cmd, observers=[masterObs])
    masterClient.start()
    masterObs.waitFor(1)

    # create observer list
    storageObs = []
    for i in range(len(Storage.servers) + len(Storage.servers1)):
        storageObs = storageObs + [Observer("Server " + str(i+1) + ".0 is up")]

    nodeClients = []
    storage_cmd = "LD_LIBRARY_PATH={7} numactl -m 0 -N 0 {0}/server -L infrc:host={3}-infrc,port={4} -x zk:{1}:{2} --totalMasterMemory {5} -f {6} --segmentFrames 10000 -r 0 --timeout {8}"
    storage_cmd = storage_cmd.format(Ramcloud.ramclouddir, Storage.master, Zookeeper.clientport, "{0}", Ramcloud.storageport, Ramcloud.memorysize, Ramcloud.backupfile, Ramcloud.boost_lib, Ramcloud.timeout)

    # startup nodes on NUMA 0
    for server in Storage.servers:
        nodeClient = ThreadedClients([server],  storage_cmd.format(server), observers=storageObs)
        nodeClient.start()
        nodeClients = nodeClients + [nodeClient]

    # startup nodes on NUMA 1
    if len(Storage.servers1) > 0:
        storage_cmd = "LD_LIBRARY_PATH={7} numactl -m 1 -N 1 {0}/server -L infrc:host={3}-infrc,port={4} -x zk:{1}:{2} --totalMasterMemory {5} -f {6} --segmentFrames 10000 -r 0 --timeout {8}"
        storage_cmd = storage_cmd.format(Ramcloud.ramclouddir, Storage.master, Zookeeper.clientport, "{0}", Ramcloud.storageport1, Ramcloud.memorysize, Ramcloud.backupfile1, Ramcloud.boost_lib, Ramcloud.timeout)
        for server in Storage.servers1:
            nodeClient = ThreadedClients([server],  storage_cmd.format(server))
            nodeClient.start()
            nodeClients = nodeClients + [nodeClient]

    # wait until all storages are up (the message is displayed at least once at at least one storage server)
    for storageOb in storageObs:
        storageOb.waitFor(1)

    return  nodeClients + [masterClient, zkClient]

def confKudu():
   if Kudu.clean:
       rmcommand = 'rm -rf {0}/*'
       master_client = ThreadedClients([Storage.master], rmcommand.format(Kudu.master_dir), root=True)
       master_client.start()
       tserver_client = ThreadedClients(Storage.servers, rmcommand.format(Kudu.tserver_dir), root=True)
       tserver_client.start()
       tserver_client1 = ThreadedClients(Storage.servers1, rmcommand.format(Kudu.tserver_dir1), root=True)
       tserver_client1.start()

       master_client.join()
       tserver_client.join()
       tserver_client1.join()

def startStorageThreads(master_cmd, server_cmd, numa0Args, numa1Args, masterObserverString="", serverObserverString="", envVars = ""):
    masterObservers = []
    if len(masterObserverString) > 0:
        masterObservers = [Observer(masterObserverString)]

    # if Storage.storage == Hbase:
    #    masterObservers.append(Observer("Master has completed initialization"))

    mclient = ThreadedClients([Storage.master], "{0}numactl -m 0 -N 0 {1}".format(envVars, master_cmd), observers=masterObservers)
    mclient.start()

    if len(masterObservers) > 0:
       masterObservers[0].waitFor(1)
    else:
        time.sleep(2)
    
    storageObservers = []
    if len(serverObserverString) > 0:
        storageObservers = [Observer(serverObserverString)]

    tclient = ThreadedClients(Storage.servers, "{0}numactl -m 0 -N 0 {1} {2}".format(envVars, server_cmd, numa0Args), observers=storageObservers)
    tclient.start()

    tclient1 = ThreadedClients(Storage.servers1, '{0}numactl -m 1 -N 1 {1} {2}'.format(envVars, server_cmd, numa1Args), observers=storageObservers)
    tclient1.start()
    
    if len(storageObservers) > 0:
        storageObservers[0].waitFor(len(Storage.servers) + len(Storage.servers1))
    else:
        time.sleep(2)

    if Storage.storage == Hbase:
        time.sleep(5)

    return [tclient, tclient1, mclient]

def startHdfs():
    javaHome = "JAVA_HOME={0} ".format(General.javahome)
    masterObs = "NameNode RPC up at"
    master_cmd = "{0}/bin/hdfs namenode".format(Hadoop.hadoopdir)
    slaveObs = "Acknowledging ACTIVE Namenode"
    server_cmd = "{0}/bin/hdfs datanode".format(Hadoop.hadoopdir)
    numa0Args = ""
    numa1Args = "-Dhadoop.log.dir={0} -Dhadoop.tmp.dir={0} -Ddfs.datanode.address=0.0.0.0:50011 -Ddfs.datanode.http.address=0.0.0.0:50081 -Ddfs.datanode.ipc.address=0.0.0.0:50021".format(Hadoop.datadir1)
    return startStorageThreads(master_cmd, server_cmd, numa0Args, numa1Args, masterObs, slaveObs, javaHome) 

def startHbaseThreads():
    zkClient = startZk()
    hdfsClients = startHdfs()

    # starting Hbase Threads
    masterObs = "Waiting for region servers count to settle"
    master_cmd = "{0}/bin/hbase master start".format(Hbase.hbasedir)
    regionObs = "wal.FSHLog: Rolled WAL"
    region_cmd = "{0}/bin/hbase regionserver".format(Hbase.hbasedir)
    numa0Args = "start"
    numa1Args = "-Dhadoop.log.dir={0} -Dhadoop.tmp.dir={0} -Ddfs.datanode.address=0.0.0.0:50011 -Ddfs.datanode.http.address=0.0.0.0:50081 -Ddfs.datanode.ipc.address=0.0.0.0:50021 -Dhbase.regionserver.port=16021 -Dhbase.regionserver.info.port=16031 start".format(Hadoop.datadir1)
    hbaseClients = startStorageThreads(master_cmd, region_cmd, numa0Args, numa1Args, masterObs, regionObs)
    return hbaseClients + hdfsClients + [zkClient] 

def startStorage():
    # to be on the safe side, first unmount the drives if they are still mounted for whatever reason
    unmount_memfs()
    javaHome = ""
    masterObs = ""
    storageObs = ""
    numa0Args = ""
    numa1Args = ""

    if Storage.storage == Kudu:
        confKudu()
        master_cmd  = '/mnt/local/tell/kudu_install/bin/kudu-master --fs_data_dirs={0} --fs_wal_dir={0} --block_manager=file'.format(Kudu.master_dir)
        server_cmd = '/mnt/local/tell/kudu_install/bin/kudu-tserver'
        numa0Args = "--fs_data_dirs={0} --fs_wal_dir={0} --block_cache_capacity_mb 51200 --tserver_master_addrs {1}".format(Kudu.tserver_dir, Storage.master)
        numa1Args = "--fs_data_dirs={0} --fs_wal_dir={0} --block_cache_capacity_mb 51200 --tserver_master_addrs {1} --rpc_bind_addresses=0.0.0.0:7049 --webserver_port=8049".format(Kudu.tserver_dir1, Storage.master)
    elif Storage.storage == TellStore:
        TellStore.rsyncBuild()
        master_cmd = "{0}/commitmanager/server/commitmanagerd".format(TellStore.builddir)
        server_cmd = "{0}/tellstore/server/tellstored-{1} -l INFO --scan-threads {2} --network-threads 1 --gc-interval {5} -m {3} -c {4}".format(TellStore.builddir, TellStore.approach, TellStore.scanThreads, TellStore.memorysize, TellStore.hashmapsize, TellStore.gcInterval)
        numa1Args = '-p 7240'
        storageObs = "Storage ready"
    elif Storage.storage == Hadoop:
        confHdfs()
        return startHdfs()
    elif Storage.storage == Hbase:
        confHbaseCluster()
        return startHbaseThreads()
    elif Storage.storage == Cassandra:
        confCassandraCluster()
        return startCassandra()
    elif Storage.storage == Ramcloud:
        confRamcloud()
        return startRamcloud()
    return startStorageThreads(master_cmd, server_cmd, numa0Args, numa1Args, masterObs, storageObs, javaHome)

def exitGracefully(storageClients, signal, frame):
    print ""
    print "\033[1;31mShutting down Storage\033[0m"
    for client in storageClients:
        client.kill()
    unmount_memfs()
    exit(0)
        
if __name__ == "__main__":
    storageClients = startStorage()
    signal.signal(signal.SIGINT, partial(exitGracefully, storageClients))
    print "\033[1;31mStorage started\033[0m"
    print "\033[1;31mHit Ctrl-C to shut it down\033[0m"
    for client in storageClients:
        client.join()

