#!/usr/bin/env python
from threaded_ssh import ThreadedClients
from ServerConfig import Storage
from ServerConfig import TellStore
from ServerConfig import Kudu
from ServerConfig import Microbench

cmd = '{0}/watch/microbench/mbserver_{1} -t {2} -n {3} '.format(TellStore.builddir, '{0}', Microbench.threads, Microbench.numColumns)

Microbench.rsyncBuild()

if Storage.storage == TellStore:
    cmd = cmd.format("tell")
    cmd += '-c "{0}" --storage "{1}" --network-threads {2}'.format(TellStore.getCommitManagerAddress(), TellStore.getServerList(), Microbench.networkThreads)
elif Storage.storage == Kudu:
    cmd = cmd.format("kudu")
    cmd += '-c {0}'.format(Kudu.master)

client0 = ThreadedClients(Microbench.servers0, "numactl -m 0 -N 0 {0}".format(cmd))
client1 = ThreadedClients(Microbench.servers1, "numactl -m 1 -N 1 {0} -p 8712".format(cmd))

client0.start()
client1.start()

client0.join()
client1.join()

