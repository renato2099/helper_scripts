#!/usr/bin/env python
from argparse import ArgumentParser
from threading import Thread
import time
import os

from ServerConfig import Client
from ServerConfig import Tpcc
from ServerConfig import TellStore

parser = ArgumentParser()
parser.add_argument("-a", dest="ch", help="Populate for CH-Benchmark", action="store_true")
parser.add_argument("-P", dest='populate', help="Populate data", action="store_true")
parser.add_argument("outfile", help="CSV file for results", default="out.csv", nargs='?')
args = parser.parse_args()

def reduceComma(x, y):
    return x + ',' + y

def addPort(x):
    return x + ':8712'

cmd = '{0}/watch/tpcc/tpcc_client -H "{1}" -c {2} -W {3}'.format(TellStore.builddir, reduce(reduceComma, Tpcc.servers0 + map(addPort, Tpcc.servers1)), Client.numClients, Tpcc.warehouses) 

if (args.populate):
    if args.ch:
        cmd = cmd + " -P -a"
    else:
        cmd = cmd + " -P"
else:
    cmd = cmd + " -t {0} -o {1} -l {2}".format(Client.runTime, args.outfile, Client.logLevel)

print "Execute {0}".format(cmd)
exit(os.system(cmd))
