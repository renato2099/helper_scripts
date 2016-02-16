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

cmd = '{0}/watch/tpch/tpch_client'.format(TellStore.builddir) 

if (args.populate):
    cmd = cmd + " -d {0} -P -s".format(TpchWorkload.dbgenFiles)
    if Tpch.storage == Kudu:
        cmd = cmd + ' "{0}"'.format(Kudu.master)
    elif Tpch.storage == TellStore:
        cmd = cmd + ' "{0}" -x "{1}"'.format(TellStore.getServerList(), TellStore.getCommitManagerAddress())
else:
    cmd = cmd + ' -t {0} -o {1} -l {2} -d {3} -H "{4}" -c {5}'.format(Client.runTime, args.outfile, Client.logLevel, TpchWorkload.updateFiles, reduce(reduceComma, map(addPort0,Tpch.servers0) + map(addPort1, Tpch.servers1)), Client.numClients)

if Tpch.storage == Kudu:
    cmd = cmd + " -k"

print "Execute {0}".format(cmd)
exit(os.system(cmd))
