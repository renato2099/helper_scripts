#!/usr/bin/env python
from threaded_ssh import ThreadedClients
from ServerConfig import Storage
from ServerConfig import TellStore
from ServerConfig import Kudu
from ServerConfig import Cassandra
from ServerConfig import Hbase
from ServerConfig import Microbench
from ServerConfig import General

getNodes = lambda l, o: o + o.join(l)

def startMBServer(observers):

    Microbench.rsyncBuild()
    params = '-t {0} -n {1} -s {2} '.format(Microbench.threads, Microbench.numColumns, Microbench.scaling)

    if Storage.storage == TellStore:
        cmd = '{0}/watch/microbench/mbserver_{1} {2}'.format(TellStore.builddir, "tell", params)
        cmd += '-c "{0}" --storage "{1}" --network-threads {2} -m {3}'.format(TellStore.getCommitManagerAddress(), TellStore.getServerList(), Microbench.networkThreads, Microbench.infinioBatch)
    elif Storage.storage == Kudu:
        cmd = '{0}/watch/microbench/mbserver_{1} {2}'.format(TellStore.builddir, "kudu", params)
        cmd += '-c {0}'.format(Storage.master)
    elif Storage.storage == Cassandra:
        Microbench.rsyncJars()
        cmd ='PATH={0}/bin:$PATH java -jar {1}/mbserver_{2} {3}'.format(General.javahome, Microbench.javaDir, "cassandra.jar", params)
        cmd += getNodes([Cassandra.master]+Cassandra.servers, " -cn ")
    elif Storage.storage == Hbase:
        cmd ='PATH={0}/bin:$PATH java -jar {0}/mbserver_{1} {3}'.format(General.javahome, Microbench.javaDir, "hbase.jar", params)
  
    client0 = ThreadedClients(Microbench.servers0, "numactl -m 0 -N 0 {0}".format(cmd), observers=observers)
    client1 = ThreadedClients(Microbench.servers1, "numactl -m 1 -N 1 {0} -p 8712".format(cmd), observers=observers)
    
    client0.start()
    client1.start()

    return [client0, client1]
    
if __name__ == "__main__":
    clients = startMBServer([])
    for client in clients:
        client.join()
