#!/usr/bin/env python
import os
import sys
import time
from ServerConfig import General
from ServerConfig import Hadoop
from ServerConfig import Hive

xmlProp = lambda key, value: "<property><name>" + key  +"</name><value>" + value + "</value></property>\n"

concatStr = lambda servers, sep: sep.join(servers) 

def copyToHost(hosts, path):
    for host in hosts:
        os.system('scp {0} root@{1}:{0}'.format(path, host))
    
def confMaster():
    # hive-env.sh
    hiveEnv = "{0}/conf/hive-env.sh".format(Hive.hivedir)
    with open (hiveEnv, 'w+') as f:
         f.write("export HADOOP_HOME={0}\n".format(Hadoop.hadoopdir))
         f.write("export HADOOP_USER_CLASSPATH_FIRST=true\n")
    copyToHost([Hive.master], hiveEnv)
    # hive-site.xml
    hiveSiteXml = "{0}/conf/hive-site.xml".format(Hive.hivedir)
    with open (hiveSiteXml, 'w+') as f:
         f.write("<configuration>\n")
         f.write(xmlProp("hive.server2.thrift.port", Hive.thriftport))
         f.write(xmlProp("hive.server2.thrift.bind.host", Hive.thriftbindhost))
         f.write("</configuration>\n")
    copyToHost([Hive.master], hiveSiteXml)

def startHive():
    # metastore
    start_hivems_cmd = "JAVA_HOME={1} {0}/bin/hive --service metastore &".format(Hive.hivedir, General.javahome)
    os.system('ssh -A root@{0} {1}'.format(Hive.master, start_hivems_cmd))
    time.sleep(2)
    # hiveserver
    start_hiveserver_cmd = "JAVA_HOME={1} {0}/bin/hive --service hiveserver2 &".format(Hive.hivedir, General.javahome)
    os.system('ssh -A root@{0} {1}'.format(Hive.master, start_hiveserver_cmd))
    time.sleep(2)

def stopHive():
    stop_hivems_cmd = "ps -a | grep HiveMetaStore | grep -v grep | awk '{print $2}' | xargs kill -9"
    os.system('ssh -A root@{0} {1}'.format(Hive.master, stop_hivems_cmd))
    # hiveserver
    stop_hiveserver_cmd = "ps -a | grep HiverServer2 | grep -v grep | awk '{print $2}' | xargs kill -9"
    os.system('ssh -A root@{0} {1}'.format(Hive.master, stop_hiveserver_cmd))

def main(argv):
    if ((len(argv) == 0) or (argv[0] == 'start')):
       confMaster()
       startHive()
    elif ((len(argv) == 1) and (argv[0] == 'stop')):
       stopHive()
    else:
       print "Usage: <start|stop> Default: start"

if __name__ == "__main__":
    main(sys.argv[1:])
