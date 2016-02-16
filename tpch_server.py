#!/usr/bin/env python
from argparse import ArgumentParser
from threaded_ssh import ThreadedClients
from ServerConfig import Tpch
from ServerConfig import TellStore
from ServerConfig import Kudu
from ServerConfig import General

import time

cmd = ""

if Tpch.storage == Kudu:
    cmd = '{0}/watch/tpch/tpch_server -s "{1}" -n {2} -k'.format(Tpch.builddir, Kudu.master, len(Kudu.tservers)*4)
elif Tpch.storage == TellStore:
    cmd = '{0}/watch/tpch/tpch_server -s "{1}" -c "{2}"'.format(Tpch.builddir, TellStore.getServerList(), TellStore.getCommitManagerAddress())

server0 = ThreadedClients(Tpch.servers0, "numactl -m 0 -N 0 {0}".format(cmd))
server1 = ThreadedClients(Tpch.servers1, "numactl -m 1 -N 1 {0} -p 8712".format(cmd))

server0.start()
server1.start()

server0.join()
server1.join()

