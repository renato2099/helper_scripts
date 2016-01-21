#!/usr/bin/env python
import os
import time
from ServerConfig import General
from ServerConfig import Spark
from ServerConfig import Storage
from ServerConfig import TellStore
from threaded_ssh import ThreadedClients

master_cmd ="{0}/sbin/start-master.sh".format(Spark.sparkdir)
slave_cmd  ="{0}/sbin/start-slave.sh spark://{1}:7077".format(Spark.sparkdir, Spark.master)

jars = map(lambda x: '{0}/{1}'.format(Spark.jarsDir, x), filter(lambda x: x.endswith('jar'), os.listdir(Spark.jarsDir)))

classpath = reduce(lambda x, y: '{0}:{1}'.format(x, y), [Spark.telljar] + jars)

sparkEnv ='{0}/conf/spark-env.sh'.format(Spark.sparkdir) 
sparkDefault = '{0}/conf/spark-defaults.conf'.format(Spark.sparkdir)

with open(sparkDefault, 'w+') as f:
    f.write('spark.driver.extraClassPath {0}\n'.format(classpath))
    f.write('spark.executor.extraClassPath {0}\n'.format(classpath))
    f.write('spark.serializer org.apache.spark.serializer.KryoSerializer\n')
    f.write('spark.driver.memory 10g\n')
    f.write('spark.executor.memory 80g\n')
    f.write('spark.executor.cores {0}\n'.format(Spark.numCores))
    # TellStore
    f.write('spark.sql.tell.numPartitions {0}\n'.format(Spark.tellPartitions))
    #f.write('spark.sql.tell.chunkSizeSmall 104857600\n')
    numChunks = len(TellStore.servers) * Spark.numCores
    if (Storage.twoPerNode):
        numChunks *= 2
    f.write('spark.sql.tell.chunkSizeBig   {0}\n'.format(((TellStore.scanMemory // numChunks) // 8) * 8))
    f.write('spark.sql.tell.chunkCount {0}\n'.format(numChunks))
    f.write('spark.sql.tell.commitmanager {0}\n'.format(TellStore.getCommitManagerAddress()))
    f.write('spark.sql.tell.storagemanager {0}\n'.format(TellStore.getServerList()))

with open(sparkEnv, 'w+') as f:
    f.write('export JAVA_HOME={0}\n'.format(Spark.javahome))
    f.write('export LD_LIBRARY_PATH={0}\n'.format(Spark.telljava))

configCopyCommand = lambda host: os.system('scp {0} {1} root@{2}:{3}/conf/'.format(sparkEnv, sparkDefault, host, Spark.sparkdir))
jarCopyCommand = lambda host: os.system('scp {0}/*.jar root@{1}:{0}'.format(Spark.jarsDir, host))
configCopyCommand(Spark.master)
jarCopyCommand(Spark.master)
for host in Spark.slaves:
    configCopyCommand(host)
    jarCopyCommand(host)

rmClients = ThreadedClients([Spark.master] + Spark.slaves, "rm -rf {0}/work".format(Spark.sparkdir), root=True)
rmClients.start()
rmClients.join()

print master_cmd
master = ThreadedClients([Spark.master], master_cmd, root=True)
master.start()
master.join()

time.sleep(1)

print slave_cmd
slave = ThreadedClients(Spark.slaves, slave_cmd, root=True)
slave.start()
slave.join()
