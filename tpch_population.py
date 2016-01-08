#!/usr/bin/env python
from argparse import ArgumentParser
import os

from ServerConfig import Client
from ServerConfig import Storage
from ServerConfig import Kudu
from ServerConfig import Tpch
from ServerConfig import TellStore

storageAddr = Kudu.master + " -k"
if Storage.storage == TellStore:
    storageAddr = '"{0}"'.format(reduce(lambda x, y: x + ";" + y, TellStore.servers))

cmd = "{0}/watch/tpcc/tpch -S {1} -d {2}".format(TellStore.builddir, storageAddr, Tpch.dbgenFiles)
print cmd
