#!/usr/bin/env python
from argparse import ArgumentParser
from threading import Thread
import time
import os

from ServerConfig import Client
from ServerConfig import Tpch
from ServerConfig import TpchWorkload
from ServerConfig import TellStore
from ServerConfig import Kudu

parser = ArgumentParser()
parser.add_argument("-P", dest='populate', help="Populate data", action="store_true")
parser.add_argument("outfile", help="CSV file for results", default="out.csv", nargs='?')
args = parser.parse_args()

def reduceComma(x, y):
    return x + ',' + y

def addPort0(x):
    return x + ':8713'

def addPort1(x):
    return x + ':8712'

cmd = '{0}/watch/tpch/tpch_client -H "{1}" -c {2}'.format(TellStore.builddir, reduce(reduceComma, map(addPort0, Tpch.servers0) + map(addPort1, Tpch.servers1)), Client.numClients) 

if (args.populate):
    cmd = cmd + " -d {0} -P".format(TpchWorkload.dbgenFiles)
else:
    cmd = cmd + ' -t {0} -o {1} -l {2} -d {3}'.format(Client.runTime, args.outfile, Client.logLevel, TpchWorkload.updateFiles)

if Tpch.storage == Kudu:
    cmd = cmd + " -k"

print "Execute {0}".format(cmd)
exit(os.system(cmd))
