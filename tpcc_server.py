#!/usr/bin/env python
from threaded_ssh import ThreadedClients
from ServerConfig import Tpcc
from ServerConfig import Kudu
from ServerConfig import TellStore
from ServerConfig import General

def hostToIp(host):
    return General.infinibandIp[host]

def semicolonReduce(x, y):
    return x + ';' + y

cmd = ""

if Tpcc.storage == Kudu:
    cmd = "{0}/watch/tpcc/tpcc_kudu -H `hostname` -W {1} --network-threads 8 -s {2} -P {3}".format(Tpcc.builddir, Tpcc.warehouses, Kudu.master, len(Kudu.tservers)*4)
elif Tpcc.storage == TellStore:
    Tpcc.rsyncBuild()
    cmd = '{0}/watch/tpcc/tpcc_server -W {1} --network-threads 4 -c "{2}" -s "{3}"'.format(Tpcc.builddir, Tpcc.warehouses, General.infinibandIp[TellStore.commitmanager] + ":7242", reduce(semicolonReduce, map(lambda x: hostToIp(x) + ":7241", TellStore.servers)))

client0 = ThreadedClients(Tpcc.servers0, "numactl -m 0 -N 0 {0}".format(cmd), rnd_start=True, root=False)
client1 = ThreadedClients(Tpcc.servers1, "numactl -m 1 -N 1 {0} -p 8712".format(cmd), rnd_start=True, root=False)

client0.start()
client1.start()

client0.join()
client1.join()

