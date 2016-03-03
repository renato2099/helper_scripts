#!/usr/bin/env python
import os
from argparse import ArgumentParser
from threaded_ssh import ThreadedClients
from ServerConfig import Storage
from ServerConfig import TellStore
from ServerConfig import Kudu
from ServerConfig import Microbench

default_out = "mbench"
if Storage.storage == TellStore:
    default_out = "mbench_{0}".format(TellStore.approach)
elif Storage.storage == Kudu:
    default_out = "mbench_kudu"

parser = ArgumentParser()
parser.add_argument("-P", dest='populate', help="Populate data", action="store_true")
parser.add_argument("outfile", help="Result database", default=default_out, nargs='?')
args = parser.parse_args()

outfile = args.outfile
appendFile = 0
while os.path.isfile(outfile + ".db"):
    appendFile = appendFile + 1
    outfile = "{0}_{1}".format(args.outfile, appendFile)

cmd = '{0}/watch/microbench/mbclient -H "{1}" -s {2} -c {3} -t {4} -a {5} -o {6}'.format(TellStore.builddir, Microbench.getServerList(), Microbench.scaling, Microbench.clientsPerThread, Microbench.clientThreads, Microbench.analyticalClients, outfile + ".db")


if (args.populate):
    cmd += ' -P'

print "Execute {0}".format(cmd)
exit(os.system(cmd))

