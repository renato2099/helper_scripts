#!/usr/bin/env python
from argparse import ArgumentParser
from threaded_ssh import ThreadedClients
from ServerConfig import Tpch
from ServerConfig import TellStore
from ServerConfig import General

import time

server_cmd = '{0}/watch/tpch/tpch_server -s "{1}" -c "{2}"'.format(Tpch.builddir, TellStore.getServerList(), TellStore.getCommitManagerAddress())
client_cmd = '{0}/watch/tpch/tpch_client -H "{1}" -s {2} -P'.format(Tpch.builddir, Tpch.server, Tpch.scaling)

server = ThreadedClients([Tpch.server], server_cmd)
server.start()
time.sleep(5)

client = ThreadedClients([Tpch.client], client_cmd)
client.start()

client.join()
print "Population done, please hit Ctr+C to finish"
server.join()

