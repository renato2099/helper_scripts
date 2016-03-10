#!/usr/bin/env python

from ServerConfig import General
from storage import *
from observer import *
import time

import logging

logging.basicConfig()

# do as for many experiments we have to run
## start storages
stObserver = Observer("Initialize network server")
startStorage([stObserver])
# wait for notification that they have started
#while (observer.semaphore  == 0):

## start microbenchmark server
mbObserver = Observer("Microbenchmark started")
#startMicrobenchmark(mbObserver)
#while(mbObserver.isNotified)

## wait for notification that it has started
## start microbenchmark client
#### this will block...
