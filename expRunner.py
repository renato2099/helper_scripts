#!/usr/bin/env python

from argparse import ArgumentParser
from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Microbench
from ServerConfig import TellStore
from mbclient import startMBClient
from mbserver import startMBServer
from storage import *
from observer import *
from functools import partial
import time
import os
import sys

import logging

logging.basicConfig()

def sqliteOut():
    storage = Storage.storage.__class__.__name__.lower()
    if Storage.storage == TellStore:
        storage = storage + "_{0}".format(TellStore.approach)
#     else:
#         print "Error: current storage not supported"
#         exit(1)
    numStorages = len(Storage.servers) + len(Storage.servers1)
    numMBServers = len(Microbench.servers0) + len(Microbench.servers1)
    numClients = Microbench.clients
    numAnalytical = Microbench.analyticalClients
    res = "{0}_{1}storages_{2}clients_{3}scans".format(storage, numStorages, numClients, numAnalytical)
    res += "_{0}infinioBatch".format(Microbench.infinioBatch)
    return res

def runMBench(outdir, onlyPopulation = False):
    # do as for many experiments we have to run
    ## start storages
    stObserver = Observer("Initialize network server")
    storageClients = startStorage([stObserver])
    # wait for notification that they have started
    if Storage.storage == TellStore:
    	stObserver.waitFor(len(Storage.servers) + len(Storage.servers1))
    print "Storage started"
    
    ## start microbenchmark server
    mbObserver = Observer("Started mbench server")
    serverClients = startMBServer([mbObserver])
    mbObserver.waitFor(len(Microbench.servers0) + len(Microbench.servers1))
    print "Server started... Continue with population"

    clients = Microbench.clients
    Microbench.clients = 10*(len(Microbench.servers0) + len(Microbench.servers1)) - 1
    res = startMBClient(True, "{0}/{1}_population".format(outdir, sqliteOut()))
    Microbench.clients = clients
    if res != 0:
        print "Population failed"
        exit(res)
    print "Population done"
    if not onlyPopulation:
        res = startMBClient(False, "{0}/{1}".format(outdir, sqliteOut()))
        if res != 0:
            print "Benchmark failed"
            exit(res)

    for client in serverClients:
        client.kill()

    for client in storageClients:
        client.kill()

    for client in serverClients:
        client.join()

    for client in storageClients:
        client.join()

    if (Storage.storage == Cassandra):
        os.system('./stop_cas.py')

def configForAnalytics():
    Microbench.analyticalClients = 1
    Microbench.clients = 0
    Microbench.infinioBatch = 16
    if len(Microbench.servers0) > 0:
        Microbench.servers1 = []
        while len(Microbench.servers0) > 1:
            del Microbench.servers0[-1]

def configGetPut():
    Microbench.analyticalClients = 0
    Microbench.clientThreads = 4
    Microbench.clients = 10*(len(Microbench.servers0) + len(Microbench.servers1)) - 1
    Microbench.threads = 1 if Storage.storage == TellStore else 4
    Microbench.insertProb = 0.166
    Microbench.updateProb = 0.166
    Microbench.deleteProb = 0.166

def configMixed():
    configGetPut()
    Microbench.analyticalClients = 1

def experiment1a(outdir):
    # edit Microbenchmark class
    configGetPut()
    runMBench(outdir)

def experiment1a_singlebatch(outdir):
    configGetPut()
    old = Microbench.infinioBatch
    Microbench.infinioBatch = 1
    runMBench(outdir)
    Microbench.infinioBatch = old

def experiment1b(outdir):
    configGetPut()
    Microbench.insertProb = 0.0
    Microbench.updateProb = 0.0
    Microbench.deleteProb = 0.0
    runMBench(outdir)

def experiment2a(outdir):
    configForAnalytics()
    runMBench(outdir)

def experiment3(outdir):
    configGetPut()
    Microbench.analyticalClients = 1
    runMBench(outdir)

def varyBatching(experiment, outdir):
    for i in [1,2,4,8,16,32,64]:
        Microbench.infinioBatch = i
        experiment(outdir)
    Microbench.infinioBatch = 16

def scalingExperiment(experiment, outdir, numNodes):
    Storage.master = 'euler03'
    Storage.servers = []
    servers = ['euler04', 'euler05', 'euler06', 'euler02']
    servers.reverse()
    mservers0 = ['euler07', 'euler08', 'euler09', 'euler10', 'euler11', 'euler01']
    mservers1 = servers + ['euler07', 'euler08', 'euler09', 'euler10', 'euler11', 'euler01'] 
    mservers0.reverse()
    mservers1.reverse()
    Microbench.servers0 = []
    Microbench.servers1 = []
    while len(Storage.servers) < numNodes:
        Storage.servers.append(servers.pop())
        while 3*len(Storage.servers) > len(Microbench.servers0) + len(Microbench.servers1):
            if len(mservers0) != 0:
                Microbench.servers0.append(mservers0.pop())
            else:
                Microbench.servers1.append(mservers1.pop())
    experiment(outdir)

def runOnTell(experiment, outdir, numNodes):
    for approach in ["rowstore"]: #["logstructured", "columnmap", "rowstore"]:
        TellStore.approach = approach
        TellStore.setDefaultMemorySize()
        for num in numNodes:
            experiment(outdir, num)

def runOnOthers(experiment, outdir, numNodes):
    for num in numNodes:
       experiment(outdir,num)

def runAllBenchmarks(outdir, experiments):
    #print "#######################################"
    #print " RUN EXPERIMENT 1a_singlebatch"
    #print "#######################################"
    #o = '{0}/experiment1a_singlebatch'.format(outdir)
    #if os.path.isdir(o):
    #    raise RuntimeError('{0} exists'.format(o))
    #os.mkdir(o)
    #runOnTell(partial(scalingExperiment, experiment1a_singlebatch), o, [1,2,3,4])

    if Storage.storage == TellStore:
        runOn = runOnTell
    else
        runOn = runOnOthers
    if len(experiments) == 0 or "experiment1a" in experiments:
        print "#######################################"
        print " RUN EXPERIMENT 1a"
        print "#######################################"
        o = '{0}/experiment1a'.format(outdir)
        if os.path.isdir(o):
            raise RuntimeError('{0} exists'.format(o))
        os.mkdir(o)
        runOn(partial(scalingExperiment, experiment1a), o, [1,2,3,4])
    if len(experiments) == 0 or "experiment1b" in experiments:
        # Experiment 1b
        print "#######################################"
        print " RUN EXPERIMENT 1b"
        print "#######################################"
        o = '{0}/experiment1b'.format(outdir)
        if os.path.isdir(o):
            raise RuntimeError('{0} exists'.format(o))
        os.mkdir(o)
        runOn(partial(scalingExperiment, experiment1b), o, [1,2,3,4])
    if len(experiments) == 0 or "experiment1c" in experiments:
        # Experiment 1c
        # No experiment needed here (inserts are measured for all experiments)
        # o = '{0}/experiment1c'.format(outdir)
        # if os.path.isdir(o):
        #     raise RuntimeError('{0} exists'.format(o))
        # os.mkdir(o)
        # runOnTell(partial(scalingExperiment, experiment1c), o, [1,2,3,4])
        # Experiment 1d
        print "#######################################"
        print " RUN EXPERIMENT 1d"
        print "#######################################"
        o = '{0}/experiment1d'.format(outdir)
        if os.path.isdir(o):
            raise RuntimeError('{0} exists'.format(o))
        os.mkdir(o)
        runOn(partial(scalingExperiment, partial(varyBatching, experiment1a)), o, [2])
    if len(experiments) == 0 or "experiment2a" in experiments:
        # Experiment 2a
        print "#######################################"
        print " RUN EXPERIMENT 2a"
        print "#######################################"
        o = '{0}/experiment2a'.format(outdir)
        if os.path.isdir(o):
            raise RuntimeError('{0} exists'.format(o))
        os.mkdir(o)
        runOn(partial(scalingExperiment, experiment2a), o, [2])
    if len(experiments) == 0 or "experiment3" in experiments:
        # Experiment 3
        print "#######################################"
        print " RUN EXPERIMENT 3"
        print "#######################################"
        o = '{0}/experiment3'.format(outdir)
        if os.path.isdir(o):
            raise RuntimeError('{0} exists'.format(o))
        os.mkdir(o)
        runOn(partial(scalingExperiment, experiment3), o, [1, 2, 3, 4])

if __name__ == "__main__":
    out = 'results'
    parser = ArgumentParser()
    parser.add_argument("-o", help="Output directory", default=out)
    parser.add_argument('experiments', metavar='E', type=str, nargs='*', help='Experiments to run (none defaults to all)')
    args = parser.parse_args()
    if not os.path.isdir(out):
        os.mkdir(out)
    runAllBenchmarks(out, args.experiments)

