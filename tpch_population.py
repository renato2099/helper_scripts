#!/usr/bin/env python
from argparse import ArgumentParser
import os

from ServerConfig import Client
from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Kudu
from ServerConfig import Tpch
from ServerConfig import TellStore

storageAddr = Kudu.master + " -k"
if Storage.storage == TellStore:
    storageAddr = TellStore.getServerList()
    masterAddr = TellStore.getCommitManagerAddress()

cmd = '{0}/watch/tpcc/tpch -S "{1}" -d {2} -C "{3}"'.format(TellStore.builddir, storageAddr, Tpch.dbgenFiles, masterAddr)
exit(os.system(cmd))
