#!/usr/bin/env python
import os
import time

import pdb, traceback, sys
from ServerConfig import General
from ServerConfig import Hadoop
from ServerConfig import Storage
from ServerConfig import TellStore
from threaded_ssh import ThreadedClients

#nnClient = ThreadedClients([Hadoop.namenode], dfs_start_cmd, root=True)
#nnClient.start()
#nnClient.join()

#time.sleep(2)

#nnFormat = ThreadedClient(Hadoop.namenode, nn_format_cmd, root=True)
#nnFormat.start()
#nnFormat.join()
