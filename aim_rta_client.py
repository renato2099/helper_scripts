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
    return x + ':8715'

def startRtaClient(out):
    cmd = '{0}/watch/aim-benchmark/rta_client -H "{1}" -c {2} -n {3} -t {4} -o {5}'.format(Aim.builddir, reduce(reduceComma, Aim.rtaservers0 + map(addPort, Aim.rtaservers1)), Aim.numRTAClients, Aim.subscribers, Client.runTime, args.outfile)

    print "Execute {0}".format(cmd)
    res = Thread(target=os.system, (cmd,))
    res.start()
    return res

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("outfile", help="CSV file for results", default="out_rta.csv", nargs='?')
    args = parser.parse_args()
    res.join()
