#!/usr/bin/env python
from threaded_ssh import ThreadedClients
from ServerConfig import Storage
from ServerConfig import TellStore
from ServerConfig import Kudu
from ServerConfig import Cassandra
from ServerConfig import Hbase
from ServerConfig import Ramcloud
from ServerConfig import Microbench
from ServerConfig import General

getNodes = lambda l, o: o + o.join(l)

def startMBServer(observers):

    Microbench.rsyncBuild()
    path = ""
    params = '-t {0} -n {1} -s {2} '.format(Microbench.threads, Microbench.numColumns, Microbench.scaling)

    if Storage.storage == TellStore:
        cmd = '{0}/watch/microbench/mbserver_tell {1}'.format(TellStore.builddir, params)
        cmd += '-c "{0}" --storage "{1}" --network-threads {2} -m {3}'.format(TellStore.getCommitManagerAddress(), TellStore.getServerList(), Microbench.networkThreads, Microbench.infinioBatch)
    elif Storage.storage == Kudu:
        cmd = '{0}/watch/microbench/mbserver_kudu {1}'.format(TellStore.builddir, params)
        cmd += '-c {0}'.format(Storage.master)
    elif Storage.storage == Cassandra:
        Microbench.rsyncJars()
        path = "PATH={0}/bin:$PATH ".format(General.javahome)
        cmd ='java -jar {0}/mbserver_cassandra.jar {1}'.format(Microbench.javaDir, params)
        cmd += getNodes(Storage.servers, " -cn ")
    elif Storage.storage == Hbase:
        path = "PATH={0}/bin:$PATH ".format(General.javahome)
        cmd = 'java -jar {0}/mbserver_hbase.jar {1}'.format(Microbench.javaDir, params)
        cmd += '-hm {0}'.format(Storage.master)
        cmd += '-zm {0}'.format(Storage.master)
    elif Storage.storage == Ramcloud:
        cmd = '{0}/watch/microbench/mbserver_ramcloud {1}'.format(TellStore.builddir, params)
        cmd += '-c main -l "infrc:host={0}-infrc,port=11100" -x {1}'.format(Storage.master, len(Storage.servers) + len(Storage.servers1))
  
    client0 = ThreadedClients(Microbench.servers0, "{0}numactl -m 0 -N 0 {1}".format(path, cmd), observers=observers)
    client1 = ThreadedClients(Microbench.servers1, "{0}numactl -m 1 -N 1 {1} -p 8712".format(path, cmd), observers=observers)
    
    client0.start()
    client1.start()

    return [client0, client1]
    
if __name__ == "__main__":
    clients = startMBServer([])
    for client in clients:
        client.join()
