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

cmd = "{0}/watch/tpch/tpch_client -H '{1}' -c {2} -l {3} -d {4} -o {5}".format(TellStore.builddir, Tpch.getServerList(), Client.numClients, Client.logLevel, TpchWorkload.dbgenFiles, args.outfile) 
if (args.populate):
    cmd = cmd + ' -P'

print "Execute {0}".format(cmd)
exit(os.system(cmd))
