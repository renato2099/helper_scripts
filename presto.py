#!/usr/bin/env python
import os
import sys
import time
from ServerConfig import General
from ServerConfig import Hadoop
from ServerConfig import Presto
from ServerConfig import Hive
from ServerConfig import TellStore

concatStr = lambda servers, sep: sep.join(servers) 

def copyToHost(hosts, path):
    for host in hosts:
        os.system('scp {0} root@{1}:{0}'.format(path, host))

def log2(n):
    res = 0
    while n > 0:
        res += 1
        n /= 2
    return res

def confNode(host, coordinator = False):
    print "\nCONFIGURING {0}".format(host)
    # node properties
    nodeProps = "{0}/etc/node.properties".format(Presto.prestodir)
    with open (nodeProps, 'w+') as f:
         f.write("node.environment=ethz\n")
         f.write("node.id=ffffffff-ffff-ffff-ffff-{0}\n".format(host))
         f.write("node.data-dir={0}\n".format(Presto.datadir))
    copyToHost([host], nodeProps)
    # jvm config
    jvmConf = "{0}/etc/jvm.config".format(Presto.prestodir)
    with open (jvmConf, 'w+') as f:
         f.write("-server\n")
         f.write("-Xmx{0}\n".format(Presto.jvmheap))
         f.write("-XX:+UseG1GC\n")
         f.write("-XX:G1HeapRegionSize={0}\n".format(Presto.jvmheapregion))
         f.write("-XX:+UseGCOverheadLimit\n")
         f.write("-XX:+ExplicitGCInvokesConcurrent\n")
         f.write("-XX:+HeapDumpOnOutOfMemoryError\n")
         f.write("-XX:OnOutOfMemoryError=kill -9 %p\n")
    copyToHost([host], jvmConf)
    # config properties
    confProps = "{0}/etc/config.properties".format(Presto.prestodir)
    with open (confProps, 'w+') as f:
         if (coordinator):
            f.write("coordinator=true\n")
            f.write("node-scheduler.include-coordinator=false\n")
            f.write("discovery-server.enabled=true\n")
         else:
            f.write("coordinator=false\n")
         f.write("http-server.http.port={0}\n".format(Presto.httpport))
         f.write("query.max-memory={0}\n".format(Presto.querymaxmem))
         f.write("query.max-memory-per-node={0}\n".format(Presto.querymaxnode))
         f.write("discovery.uri=http://{0}:8080\n".format(Presto.coordinator))
    copyToHost([host], confProps)
    # catalog:
    if Storage.storage == Hadoop:
        hiveCat = "{0}/etc/catalog/hive.properties".format(Presto.prestodir)
        with open (hiveCat, 'w+') as f:
             f.write("connector.name=hive-hadoop2\n")
             f.write("hive.metastore.uri=thrift://{0}:{1}\n".format(Hive.metastoreuri, Hive.metastoreport))
             f.write("hive.metastore-timeout={0}\n".format(Hive.metastoretimeout))
        copyToHost([host], hiveCat)
    elif Storage.storage == Tell:
        tellCat = "{0}/etc/catalog/tell.properties".format(Presto.prestodir)
        numChunks = Presto.splitsPerMachine * TellStore.numServers()
        with open (hiveCat, 'w+') as f:
            f.write('connector.name=tell')
            f.write('tell.commitManager={0}'.format(TellStore.getCommitManagerAddress()))
            f.write('tell.storages={0}'.format(TellStore.getServerList()))
            f.write('tell.numPartitions={0}'.format(Presto.splitsPerMachine))
            f.write('tell.partitionShift={0}'.format(log2(TellStore.numServers())))
            f.write('tell.chunkCount={0}'.format(numChunks))
            f.write('tell.chunkSize={0}'.format(((TellStore.scanMemory // numChunks) // 8) * 8))
        copyToHost([host], tellCat)
    # log level
    logProps = "{0}/etc/log.properties".format(Presto.prestodir)
    f = open(logProps, 'w+')
    f.write("com.facebook.presto={0}".format(Presto.loglevel))
    f.close()
    copyToHost([host], logProps)
    # tmp files for logging
    os.system("ssh root@{0} 'rm -rf {1}; mkdir {1}'".format(host, Presto.datadir))
    
def confCluster():
    for host in Presto.nodes:
        confNode(host)
    confNode(Presto.coordinator, True)

def startPresto():
    start_presto_cmd = "JAVA_HOME={1} {0}/bin/launcher start".format(Presto.prestodir, General.javahome)
    os.system('ssh -A root@{0} {1}'.format(Presto.coordinator, start_presto_cmd))
    time.sleep(5)
    for host in Presto.nodes:
        os.system('ssh -A root@{0} {1}'.format(host, start_presto_cmd))

def stopPresto():
    hosts = [Presto.coordinator] + Presto.nodes
    stop_presto_cmd = "JAVA_HOME={1} {0}/bin/launcher stop".format(Presto.prestodir, General.javahome)
    for host in hosts:
        os.system('ssh -A root@{0} {1}'.format(host, stop_presto_cmd))

def main(argv):
    if ((len(argv) == 0) or (argv[0] == 'start')):
       confCluster()
       startPresto()
    elif ((len(argv) == 1) and (argv[0] == 'stop')):
       stopPresto()

if __name__ == "__main__":
    main(sys.argv[1:])
