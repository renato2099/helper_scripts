#!/usr/bin/env python
from argparse import ArgumentParser
from threaded_ssh import ThreadedClients
from ServerConfig import YCSB
from ServerConfig import TellStore
from ServerConfig import General

parser = ArgumentParser()
parser.add_argument("-C", dest='createTable', help="Create user table", action="store_true")
args = parser.parse_args()

def hostToIp(host):
    return General.infinibandIp[host]

def semicolonReduce(x, y):
    return x + ';' + y


cmd = '{0}/watch/ycsb-server/ycsb-server -H `hostname` --network-threads {3} -c "{1}" -s "{2}"'.format(YCSB.builddir, General.infinibandIp[TellStore.commitmanager] + ":7242", reduce(semicolonReduce, map(lambda x: hostToIp(x) + ":7241", TellStore.servers)), YCSB.networkThread)

fst = None
fstZero = True
cCommand = "-C" if args.createTable else ""

if len(YCSB.servers0) > 0:
    fst = YCSB.servers0.pop()
else:
    fstZero = False
    fst = YCSB.servers1.pop()

if fstZero:
    fst = ThreadedClients([fst], "numactl -m 0 -N 0 {0} {1}".format(cmd, cCommand))
else:
    fst = ThreadedClients([fst], "numactl -m 1 -N 1 {0} -p 8712 {1}".format(cmd, cCommand))


client0 = ThreadedClients(YCSB.servers0, "numactl -m 0 -N 0 {0}".format(cmd))
client1 = ThreadedClients(YCSB.servers1, "numactl -m 1 -N 1 {0} -p 8712".format(cmd))

fst.start()
client0.start()
client1.start()

fst.join()
client0.join()
client1.join()


