#!/usr/bin/env python
from threaded_ssh import ThreadedClients
from ServerConfig import Aim
from ServerConfig import TellStore
from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Kudu

def hostToIp(host):
    return General.infinibandIp[host]

def semicolonReduce(x, y):
    return x + ';' + y

def startAimServers(observers = []):
    Aim.rsyncBuild()
    numChunks = (len(Storage.servers) + len(Storage.servers1)) * Aim.numRTAClients * 16
    chunkSize = ((TellStore.scanMemory // numChunks) // 8) * 8
    
    serverExec = ""
    if Storage.storage == Kudu:
        serverExec = "aim_kudu -P {0} -s {1}".format((len(Storage.servers) + len(Storage.servers1)) * 2, Storage.master)
    elif Storage.storage == TellStore:
        serverExec = 'aim_server -M {0} -m {1} -c "{2}" -s "{3}" --processing-threads {4}'.format(numChunks, chunkSize, TellStore.getCommitManagerAddress(), TellStore.getServerList(), Aim.serverthreads)
    
    cmd = '{0}/watch/aim-benchmark/{3} -f {1} -b {2}'.format(Aim.builddir, Aim.schemaFile, Aim.batchSize, serverExec)
    
    client0 = ThreadedClients(Aim.sepservers0 + Aim.rtaservers0, "numactl -m 0 -N 0 {0}".format(cmd), root=True)
    client1 = ThreadedClients(Aim.sepservers1 + Aim.rtaservers1, "numactl -m 1 -N 1 {0} -p 8715 -u 8716".format(cmd), root=True)
    
    client0.start()
    client1.start()
    return [client0, client1]

if __name__ == '__main__':
    clients = startAimServers()
    for c in clients:
        c.join()

