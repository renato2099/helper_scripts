#!/usr/bin/env python
import os
from argparse import ArgumentParser

from ServerConfig import YCSB
from ServerConfig import Storage
from ServerConfig import Kudu
from ServerConfig import TellStore

parser = ArgumentParser()
parser.add_argument("-P", dest="load", help="Populate data", action="store_true")
parser.add_argument("outfile", help="CSV file for results", default="out.csv", nargs='?')
args = parser.parse_args()

command = "run"
if args.load:
    command = "load"

serverArgs = ""
if Storage.storage == TellStore:
    serverArgs = "-p ycsb-tell.server={0} -p ycsb-tell.server-port=8712".format(YCSB.servers1[0])

cmd = "cd {1}; PATH={0}:$PATH bin/ycsb {2} tellstore -threads {3} -P {1}/workloads/{4} {5}".format(YCSB.mvnDir, YCSB.ycsbdir, command, YCSB.clientThreads, YCSB.workload, serverArgs)

print "running {0}".format(cmd)
exit(os.system(cmd))
