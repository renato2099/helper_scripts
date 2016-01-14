#!/usr/bin/env python
import os
from argparse import ArgumentParser

import ServerConfig
from ServerConfig import YCSB
from ServerConfig import YCSBWorkload
from ServerConfig import Storage
from ServerConfig import Kudu
from ServerConfig import TellStore

parser = ArgumentParser()
parser.add_argument("-P", dest="load", help="Populate data", action="store_true")
parser.add_argument("-R", dest="rt", help="Do response time test (only one client)", action="store_true")
args = parser.parse_args()

if not args.rt:
    YCSBWorkload.operationcount = YCSBWorkload.operationcount * YCSB.clientThreads

with open('workload', 'w+') as f:
    f.writelines(map(lambda (x,y): "{0} = {1}\n".format(x, y), map(lambda x: (x, getattr(YCSBWorkload, x)), filter(lambda x: not x.startswith('__'), dir(YCSBWorkload)))))

command = "run"
if args.load:
    command = "load"

storage = ""

serverArgs = ""
if Storage.storage == TellStore:
    reducer = lambda l, p: reduce(lambda x,y: x + ";" + y, map(lambda x: x + ":" + p, l))
    serverArgs = '-p ycsb-tell.servers="{0}"'.format((reducer(YCSB.servers0, "8713") + ";" + reducer(YCSB.servers1, "8712")))
    storage = "tellstore"
elif Storage.storage == ServerConfig.Kudu:
    serverArgs= ' -p kudu_table_num_replicas=1 -p kudu_pre_split_num_tablets={0} -p kudu_master_addresses="{1}" '.format(len(Kudu.tservers)*4, Kudu.master)
    storage = "kudu"

cmd = "cd {1}; PATH={0}:$PATH bin/ycsb {2} {3} -threads {4} -P {5} {6}".format(YCSB.mvnDir, YCSB.ycsbdir, command, storage, 1 if args.rt else YCSB.clientThreads, '{0}/workload'.format(os.path.dirname(os.path.realpath(__file__))), serverArgs)

print "running {0}".format(cmd)
exit(os.system(cmd))
