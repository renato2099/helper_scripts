#!/usr/bin/env python
import os
from argparse import ArgumentParser
from threaded_ssh import ThreadedClients
from ServerConfig import TellStore
from ServerConfig import Microbench

cmd = '{0}/watch/microbench/mbclient -H "{1}" -s {2} -c {3} -t {4}'.format(TellStore.builddir, Microbench.getServerList(), Microbench.scaling, Microbench.clientsPerThread, Microbench.clientThreads)

parser = ArgumentParser()
parser.add_argument("-P", dest='populate', help="Populate data", action="store_true")
args = parser.parse_args()

if (args.populate):
    cmd += ' -P'

print "Execute {0}".format(cmd)
exit(os.system(cmd))

