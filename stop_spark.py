#!/usr/bin/env python
from ServerConfig import Spark

master_cmd ="{0}/spark-master/sbin/stop-master.sh".format(Spark.sparkdir)
slave_cmd  ="{0}/spark-slave/sbin/stop-slave.sh".format(Spark.sparkdir, Spark.master)

print master_cmd
master = ThreadedClients([Spark.master], master_cmd)
master.start()
master.join()

print slave_cmd
slave = ThreadedClients(Spark.slaves, slave_cmd)
slave.start()
slave.join()
