'''
AIM-Benchmark
=============
experiment1: 4 storage nodes, only RTA queries, scale number of clients
experiment2: 4 storage nodes, with events, scale number of clients
'''
from ServerConfig import Storage
from ServerConfig import Kudu
from ServerConfig import TellStore
from ServerConfig import Aim
from observer import Observer
from aim_server import startAimServers
from aim_sep_client import startSepClient
from aim_rta_client import startRtaClient

from argparse import ArgumentParser
import time
import os

def sqliteOut(withEvents):
    storage = Storage.storage().__class__.__name__.lower()
    if Storage.storage == TellStore:
        storage = storage + "_{0}".format(TellStore.approach)
    rtaClients = Aim.numRTAClients * (len(Aim.rtaservers0) + len(Aim.rtaservers1))
    return "{}_clients_{}".format(str(rtaClients).zfill(2), storage)

def runOnTell(fun):
    Storage.storage = TellStore
    for approach in ['logstructured', 'rowstore', 'columnmap']:
        TellStore.approach = approach
        fun()

def scaleRTA(fun):
# with these configs, we can run up to 16 rta clients
    servers0 = ['euler02', 'euler03', 'euler08', 'euler09']
    servers1 = servers0
    Aim.numRTAClients = 1
    Aim.rtaservers0 = []
    Aim.rtaservers1 = []
    Aim.rtaservers0.append(servers0.pop(0))
    for numClients in [1,2,4,8,16]:
        while numClients < (Aim.numRTAClients * (len(Aim.rtaservers0) + len(Aim.rtaservers1))):
            if Aim.numRTAClients == 1:
                Aim.numRTAClients = 2
            elif len(servers0) > 0:
                Aim.rtaservers0.append(servers0.pop(0))
                Aim.numRTAClients = 1
            else:
                Aim.rtaservers1.append(servers1.pop(0))
                Aim.numRTAClients = 1
        fun()

def runWithStorageRestart(outdir, experiments):
    Aim.sepservers0 = ['euler10', 'euler11']
    Aim.sepservers1 = []
    srvObserver = Observer('AIM server started')
    for experiment in experiments:
        storageClients = []
        if Storage.storage == TellStore:
            stObserver = Observer("Initialize network server")
            storageClients = startStorage([stObserver])
            stObserver.waitFor(len(Storage.servers) + len(Storage.servers1))
        else:
            storageClients = startStorage([])
            time.sleep(2)
# Populate
        outfile = "{}/{}_population.db".format(outdir, sqliteOut())
        aimObserver = Observer("AIM server started")
        aimClients = startAimServers([aimObserver])
        aimObserver.waitFor(len(Aim.sepservers0))
        populationClient = startSepClient(True, outfile)
        populationClient.join()
        Aim.rtaservers0 = rta0
        Aim.rtaservers1 = rta1
        experiment()
        for aimClient in aimClients:
            aimClient.kill()
            aimClient.join()
        for client in storageClients:
            client.kill()
        for client in storageClients:
            client.join()

def experiment1(outdir):
    odir = "{}/experiment1"
    if not os.path.isdir(odir):
        os.mkdir(odir)
    out = "{}/{}.db".format(odir, sqliteOut(False))
    client = startRtaClient(out)
    client.join()

def experiment2(outdir):
    odir = "{}/experiment2"
    if not os.path.isdir(odir):
        os.mkdir(odir)
    out = "{}/{}.db".format(odir, sqliteOut(True))
    sepOut = "{}/{}_sep.csv".format(odir, sqliteOut(True))
    rtaClient = startRtaClient(out)
    sepClient = startSepClient(False, sepOut)
    rtaClient.join()
    sepClient.join()

def simpleRunner(funs):
    for f in funs:
        f()

def runAllBenchmarks(out, experiments):
    funs = []
    if len(experiments) == 0 or "experiment1" in experiments:
        funs.append(partial(experiment1, out))
    if len(experiments) == 0 or 'experiment2' in experiments:
        funs.append(partial(experiment2, out))
    f = partial(simpleRunner, funs)
    scaleRTA(partial(runWithStorageRestart, out, f))

def benchmarks(out, experiments, onKudu, onTell):
    Storage.master = 'euler01'
    Storage.servers = ['euler04', 'euler05', 'euler06', 'euler07']
    if onKudu:
        Storage.storage = Kudu
        runAllBenchmarks(out, experiments)
    elif onTell:
        Storage.storage = TellStore
        runOnTell(partial(runAllBenchmarks, out, experiments))

if __name__ == '__main__':
    out = 'aim_results'
    parser = ArgumentParser()
    parser.add_argument("-o", help="Output directory", default=out)
    parser.add_argument('experiments', metavar='E', type=str, nargs='*', help='Experiments to run (none defaults to all)')
    parser.add_argument('--nokudu', action='store_false', help='Do not run on kudu')
    parser.add_argument('--notell', action='store_false', help='Do not run on tell')
    args = parser.parse_args()
    if not os.path.isdir(out):
        os.mkdir(out)
    else
        raise RuntimeError('{} exists'.format(out))
    benchmarks(out, args.experiments, not parser.nokudu, not parser.notell)

