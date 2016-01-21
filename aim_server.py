#!/usr/bin/env python
from threaded_ssh import ThreadedClients
from ServerConfig import Aim
from ServerConfig import TellStore
from ServerConfig import General

def hostToIp(host):
    return General.infinibandIp[host]

def semicolonReduce(x, y):
    return x + ';' + y

numChunks = len(TellStore.servers) * Aim.numRTAClients * 16
chunkSize = ((TellStore.scanMemory // numChunks) // 8) * 8

cmd = '{0}/watch/aim-benchmark/aim_server -f {1} -b {2} -M {3} -m {4} --processing-threads 4 -c "{5}" -s "{6}"'.format(Aim.builddir, Aim.schemaFile, Aim.batchSize, numChunks, chunkSize, General.infinibandIp[TellStore.commitmanager] + ":7242", reduce(semicolonReduce, map(lambda x: hostToIp(x) + ":7241", TellStore.servers)))

client0 = ThreadedClients(Aim.sepservers0 + Aim.rtaservers0, "numactl -m 0 -N 0 {0}".format(cmd))
client1 = ThreadedClients(Aim.sepservers1 + Aim.rtaservers1, "numactl -m 1 -N 1 {0} -p 8715 -u 8716".format(cmd))

client0.start()
client1.start()

client0.join()
client1.join()

