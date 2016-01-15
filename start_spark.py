#!/usr/bin/env python
import os
from ServerConfig import General
from ServerConfig import Spark
from ServerConfig import Storage
from ServerConfig import TellStore
from threaded_ssh import ThreadedClients

master_cmd ="{0}/spark-master/sbin/start-master.sh".format(Spark.sparkdir)
slave_cmd  ="{0}/spark-slave/sbin/start-slave.sh spark://{1}:7077".format(Spark.sparkdir, Spark.master)

jars = map(lambda x: '{0}/{1}'.format(Spark.jarsDir, x), filter(lambda x: x.endswith('jar'), os.listdir(Spark.jarsDir)))

classpath = reduce(lambda x, y: '{0}:{1}'.format(x, y), [Spark.telljar] + jars)

with open('spark-defaults.conf', 'w+') as f:
    f.write('spark.driver.extraClassPath {0}\n'.format(classpath))
    f.write('spark.executor.extraClassPath {0}\n'.format(classpath))
    f.write('spark.driver.memory 20g\n')
    # TellStore
    f.write('spark.sql.tell.chunkSizeSmall 100000000\n')
    f.write('spark.sql.tell.chunkSizeBig 2194304000\n')
    f.write('spark.sql.tell.chunkCount 20\n')
    f.write('spark.sql.tell.commitmanager {0}\n'.format(TellStore.getCommitManagerAddress()))
    f.write('spark.sql.tell.storagemanager {0}\n'.format(TellStore.getServerList()))

with open('spark-env.sh', 'w+') as f:
    f.write('export JAVA_HOME={0}\n'.format(Spark.javahome))
    f.write('export LD_LIBRARY_PATH={0}\n'.format(Spark.telljava))

configCopyCommand = lambda host: os.system('scp spark-env.sh spark-defaults.conf {0}:{1}/conf/'.format(host, Spark.sparkdir))
jarCopyCommand = lambda host: os.system('scp {0}/*.jar {1}:{0}'.format(Spark.jarsDir, host))
configCopyCommand(Spark.master)
jarCopyCommand(Spark.master)
for host in Spark.slaves:
    configCopyCommand(host)
    jarCopyCommand(host)


print master_cmd
master = ThreadedClients([Spark.master], master_cmd)
master.start()
master.join()

print slave_cmd
slave = ThreadedClients(Spark.slaves, slave_cmd)
slave.start()
slave.join()
