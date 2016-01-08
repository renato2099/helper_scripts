#!/usr/bin/env python
from pssh import ParallelSSHClient
from threaded_ssh import ThreadedClients

from ServerConfig import General
from ServerConfig import Storage
from ServerConfig import Kudu
from ServerConfig import TellStore
import logging

logging.basicConfig()

master = Storage.master
servers = Storage.servers
master_cmd = ""
server_cmd = ""

if Storage.storage == Kudu:
    master_dir = Kudu.master_dir
    tserver_dir = Kudu.tserver_dir

    master_cmd  = '/mnt/local/tell/kudu_install/bin/kudu-master --fs_data_dirs={0} --fs_wal_dir={0} --block_manager=file'.format(master_dir)
    server_cmd = '/mnt/local/tell/kudu_install/bin/kudu-tserver --fs_data_dirs={0} --fs_wal_dir={0} -block_cache_capacity_mb 51200 --tserver_master_addrs {1}'.format(tserver_dir, master)
    if Kudu.clean:
        rmcommand = 'rm -rf {0}/*'
        master_client = ParallelSSHClient([master], user="root")
        output = master_client.run_command(rmcommand.format(master_dir))
        tserver_client = ParallelSSHClient(servers, user="root")
        tservers_out = tserver_client.run_command(rmcommand.format(tserver_dir))
        for host in output:
            for line in output[host]['stdout']:
                print "Host {0}: {1}".format(host, line)
    
            for line in output[host]['stderr']:
                print "{0}Host {1}: {2}".format(self.FAIL, host, line)
        for host in tservers_out:
            for line in tservers_out[host]['stdout']:
                print "Host {0}: {1}".format(host, line)
    
            for line in tservers_out[host]['stderr']:
                print "{0}Host {1}: {2}".format(self.FAIL, host, line)
else:
    master_cmd = "{0}/commitmanager/server/commitmanagerd".format(TellStore.builddir)
    server_cmd = "{0}/tellstore/server/tellstored-{1} -l INFO --scan-threads 1 --network-threads 2 --gc-interval 20 -m {3} -c {4}".format(TellStore.builddir, TellStore.approach, TellStore.commitmanager, TellStore.memorysize, TellStore.hashmapsize)

mclient = ThreadedClients([master], "numactl -m 0 -N 0 {0}".format(master_cmd))
mclient.start()
tclient = ThreadedClients(servers, "numactl -m 0 -N 0 {0}".format(server_cmd))
tclient.start()

mclient.join()
tclient.join()


