#!/usr/bin/env python

from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Microbench
from mbclient import startMBClient
from storage import *
from observer import *
import time

import logging

logging.basicConfig()

def runMBench():
    # do as for many experiments we have to run
    ## start storages
    stObserver = Observer("Initialize network server")
    clients = startStorage([stObserver])
    # wait for notification that they have started
    stObserver.waitFor(len(Storage.servers) + len(Storage.servers1))
    print "Storage started"
    
    ## start microbenchmark server
    mbObserver = Observer("Started mbench server")
    startMicrobenchmark(mbObserver)
    mbObserver.waitFor(len(Microbench.servers0) + len(Microbench.servers1))

    startMBClient(True)
    startMBClient()
    
    for client in clients:
        client.kill()

runMBench()
