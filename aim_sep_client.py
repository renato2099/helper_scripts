#!/usr/bin/env python
from argparse import ArgumentParser
from threading import Thread
import time
import os

from ServerConfig import Client
from ServerConfig import Aim
from ServerConfig import TellStore


def reduceComma(x, y):
    return x + ',' + y

def addPort(x):
    return x + ':8715:8716'

def startSepClient(populate, outfile):
    messageRate = Aim.messageRate // (Aim.numSEPClients * (len(Aim.sepservers0) + len(Aim.sepservers1)))
    
    cmd = '{0}/watch/aim-benchmark/sep_client -H "{1}" -c {2} -n {3} -r {4}'.format(Aim.builddir, reduce(reduceComma, Aim.sepservers0 + map(addPort, Aim.sepservers1)), Aim.numSEPClients, Aim.subscribers, messageRate)
    
    if (args.populate):
        cmd = cmd + " -P"
    else:
        cmd = cmd + " -t {0}".format(Client.runTime)
    
    print "Execute {0}".format(cmd)
    res = Thread(target=os.system, args=(cmd,))
    res.start()
    return res

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-a", dest="ch", help="Populate for CH-Benchmark", action="store_true")
    parser.add_argument("-P", dest='populate', help="Populate data", action="store_true")
    parser.add_argument("outfile", help="CSV file for results", default="out_sep.csv", nargs='?')
    args = parser.parse_args()
    client = startSepClient(populate, outfile)
    client.join()

