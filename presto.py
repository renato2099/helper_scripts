#!/usr/bin/env python
import os
import signal
import sys
import time

from threaded_ssh import ThreadedClients
from observer import *

from functools import partial
from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Hadoop
from ServerConfig import Presto
from ServerConfig import Hive
from ServerConfig import TellStore
from ServerConfig import Java

concatStr = lambda servers, sep: sep.join(servers) 

import logging

if 'threading' in sys.modules:
        del sys.modules['threading']
        import gevent
        import gevent.socket
        import gevent.monkey
        gevent.monkey.patch_all()

logging.basicConfig()

def copyToHost(hosts, path):
    for host in hosts:
        os.system('scp {0} root@{1}:{0}'.format(path, host))

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
         f.write("-Djava.library.path={0}\n".format(Java.telljava))
         if Presto.debug:
             f.write('-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005\n')
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
         f.write("node-scheduler.max-splits-per-node={0}\n".format(Presto.splitsPerMachine))
         f.write("node-scheduler.max-pending-splits-per-node-per-task={0}\n".format(0))
         if Presto.read_opt == True:
             f.write("optimizer.processing-optimization=columnar_dictionary\n")
    copyToHost([host], confProps)
    # catalog:
    if Storage.storage == Hadoop:
        hiveCat = "{0}/etc/catalog/hive.properties".format(Presto.prestodir)
        with open (hiveCat, 'w+') as f:
             f.write("connector.name=hive-hadoop2\n")
             f.write("hive.allow-drop-table=true\n")
             f.write("hive.metastore.uri=thrift://{0}:{1}\n".format(Hive.metastoreuri, Hive.metastoreport))
             f.write("hive.metastore-timeout={0}\n".format(Hive.metastoretimeout))
             if Presto.read_opt == True:
                 f.write("hive.parquet-predicate-pushdown.enabled=true\n")
                 f.write("hive.parquet-optimized-reader.enabled=true\n")
        copyToHost([host], hiveCat)
    elif Storage.storage == TellStore:
        tellCat = "{0}/etc/catalog/tell.properties".format(Presto.prestodir)
        numChunks = Presto.splitsPerMachine * TellStore.numServers()
        with open (tellCat, 'w+') as f:
            f.write('connector.name=tell\n')
            f.write('tell.commitManager={0}\n'.format(TellStore.getCommitManagerAddress()))
            f.write('tell.storages={0}\n'.format(TellStore.getServerList()))
            f.write('tell.numPartitions={0}\n'.format(Presto.splitsPerMachine * len(Presto.nodes)))
            f.write('tell.partitionShift={0}\n'.format(TellStore.scanShift))
            f.write('tell.chunkCount={0}\n'.format(numChunks))
            f.write('tell.chunkSize={0}\n'.format(((TellStore.scanMemory // numChunks) // 8) * 8))
        copyToHost([host], tellCat)
    # log level
    logProps = "{0}/etc/log.properties".format(Presto.prestodir)
    f = open(logProps, 'w+')
    f.write("com.facebook.presto={0}\n".format(Presto.loglevel))
    f.close()
    copyToHost([host], logProps)
    # tmp files for logging
    os.system("ssh root@{0} 'rm -rf {1}; mkdir {1}'".format(host, Presto.datadir))
    
def confCluster():
    for host in Presto.nodes:
        confNode(host)
    confNode(Presto.coordinator, True)

def rsyncCommand(host):
    return 'rsync -ra {0}/ root@{1}:{2}'.format(Presto.localPresto, host, Presto.prestodir)

def sync():
    cmd = rsyncCommand(Presto.coordinator)
    print "exec {0}".format(cmd)
    os.system(cmd)
    for host in Presto.nodes:
        cmd = rsyncCommand(host)
        print "exec {0}".format(cmd)
        os.system(cmd)

def startPresto():
    sync()
    confCluster()
    #start_presto_cmd = "'JAVA_HOME={1} PATH={1}/bin:$PATH {0}/bin/launcher run'".format(Presto.prestodir, General.javahome)
    prestoObsStr = "======== SERVER STARTED ========"
    start_presto_cmd = "PATH={0}/bin:$PATH {1}/bin/launcher run".format(General.javahome, Presto.prestodir)
    print start_presto_cmd
    print
    observer = Observer(prestoObsStr)
    coordClient = ThreadedClients([Presto.coordinator], start_presto_cmd, root=True, observers=[observer])
    coordClient.start()
    observer.waitFor(1)
    
    # starting worker nodes
    workerClients = []
    for worker in Presto.nodes:
        obs = Observer(prestoObsStr)
        workerClient = ThreadedClients([worker], start_presto_cmd, root=True, observers=[obs])
        workerClient.start()
        obs.waitFor(1)
        workerClients = workerClients + [workerClient]
    
    return workerClients + [coordClient]

def stopPresto():
    hosts = [Presto.coordinator] + Presto.nodes
    stop_presto_cmd = "'JAVA_HOME={1} PATH={1}/bin:$PATH {0}/bin/launcher stop'".format(Presto.prestodir, General.javahome)
    for host in hosts:
        c = 'ssh -A root@{0} {1}'.format(host, stop_presto_cmd)
        print 'executing {0}'.format(c)
        os.system(c)

def main(argv):
    if ((len(argv) == 0) or (argv[0] == 'start')):
        #sync()
        #confCluster()
        startPresto()
    elif ((len(argv) == 1) and (argv[0] == 'stop')):
        stopPresto()

def exitGracefully(storageClients, signal, frame):
    print ""
    print "\033[1;31mShutting down Presto\033[0m"
    for client in storageClients:
        client.kill()
    exit(0)
        
#if __name__ == "__main__":
#    storageClients = startStorage()
#    signal.signal(signal.SIGINT, partial(exitGracefully, storageClients))
#    print "\033[1;31mStorage started\033[0m"
#    print "\033[1;31mHit Ctrl-C to shut it down\033[0m"
#    for client in storageClients:
#        client.join()

if __name__ == "__main__":
    prestoClients = startPresto()
    signal.signal(signal.SIGINT, partial(exitGracefully, prestoClients))
    print "\033[1;31mPresto started\033[0m"
    print "\033[1;31mHit Ctrl-C to shut it down\033[0m"
    for client in prestoClients:
        client.join()
