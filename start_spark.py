#!/usr/bin/env python
from ServerConfig import Spark
from threaded_ssh import ThreadedClients

master_cmd ="JAVA_HOME={0} {1}/sbin/start-master.sh".format(Spark.javahome, Spark.sparkdir)
slave_cmd  ="JAVA_HOME={0} {1}/sbin/start-slave.sh spark://{2}:7077".format(Spark.javahome, Spark.sparkdir, Spark.master)

print master_cmd
master = ThreadedClients([Spark.master], master_cmd)
master.start()
master.join()

print slave_cmd
slave = ThreadedClients([Spark.slaves], slave_cmd)
slave.start()
slave.join()
