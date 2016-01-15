#!/usr/bin/env python
from ServerConfig import Spark
from threaded_ssh import ThreadedClients

master_cmd ="{0}/sbin/stop-master.sh".format(Spark.sparkdir)
slave_cmd  ="{0}/sbin/stop-slave.sh".format(Spark.sparkdir, Spark.master)

print master_cmd
master = ThreadedClients([Spark.master], master_cmd, root=True)
master.start()
master.join()

print slave_cmd
slave = ThreadedClients(Spark.slaves, slave_cmd, root=True)
slave.start()
slave.join()
