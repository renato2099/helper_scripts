#!/usr/bin/env python
import os
import sys
import time
from ServerConfig import General
from ServerConfig import Hadoop
from ServerConfig import Hive
from ServerConfig import TpchWorkload
from ServerConfig import Storage

from argparse import ArgumentParser

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
         f.write(xmlProp("hive.metastore.warehouse.dir", "/usr/hive/warehouse"))
         #f.write(xmlProp("hive.metastore.uris", "thrift://{0}:{1}".format(Hive.thriftbindhost, Hive.thriftport)))
         f.write(xmlProp("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName={0}/metastore_db;create=true".format(Hive.hivedir)))
         f.write(xmlProp("hive.server2.thrift.port", Hive.thriftport))
         f.write(xmlProp("hive.server2.thrift.bind.host", Hive.thriftbindhost))
         f.write("</configuration>\n")
    copyToHost([Hive.master], hiveSiteXml)

def startHive():
    # metastore
    start_hivems_cmd = "JAVA_HOME={1} {0}/bin/hive --service metastore".format(Hive.hivedir, General.javahome)
    print "{1} :{0}".format(start_hivems_cmd, Hive.master)
    os.system('ssh -n -f root@{0} "sh -c \'{1} > /dev/null 2>&1 &\'"'.format(Hive.master, start_hivems_cmd))
    time.sleep(2)

    # hiveserver
    if Hive.hiveserver == True:
        start_hiveserver_cmd = "JAVA_HOME={1} {0}/bin/hive --service hiveserver2".format(Hive.hivedir, General.javahome)
        print "{1} : {0}".format(start_hiveserver_cmd, Hive.master)
        os.system('ssh -A root@{0} {1}'.format(Hive.master, start_hiveserver_cmd))
        os.system('ssh -n -f root@{0} "sh -c \'{1} > /dev/null 2>&1 &\'"'.format(Hive.master, start_hiveserver_cmd))
        time.sleep(2)

def stopHive():
    stop_hivems_cmd = "ps -ax | grep HiveMetaStore | grep -v grep | awk '{print $1}' | xargs sudo kill -9"
    os.system('ssh -A root@{0} {1}'.format(Hive.master, stop_hivems_cmd))
    print "{1} : {0}".format(stop_hivems_cmd, Hive.master)
    # hiveserver
    if Hive.hiveserver == True:
        stop_hiveserver_cmd = "ps -ax | grep HiverServer2 | grep -v grep | awk '{print $1}' | xargs sudo kill -9"
        os.system('ssh -A root@{0} {1}'.format(Hive.master, stop_hiveserver_cmd))
        print "{1} : {0}".format(stop_hiveserver_cmd, Hive.master)

def cmdExecute(server, cmd):
    print "{0} : {1}".format(server, cmd)
    os.system('ssh -A root@{0} {1}'.format(server, cmd))

def loadTpchData():
    # load tpch data 
    cmd = "sudo {0}/bin/hadoop fs -mkdir /tpch_data".format(Hadoop.hadoopdir)
    cmdExecute(Storage.master, cmd)

    cmd = "sudo {0}/bin/hadoop fs -chmod -R 777 /tpch_data".format(Hadoop.hadoopdir)
    cmdExecute(Storage.master, cmd)
    relations = ["lineitem", "orders", "part"]
    for rel in relations:
        print("Loading Relation == {}".format(rel))
        cmd = "sudo {0}/bin/hadoop fs -mkdir /tpch_data/{1}".format(Hadoop.hadoopdir, rel)
        cmdExecute(Storage.master, cmd)

        cmd = "sudo {0}/bin/hadoop fs -chmod -R 777 /tpch_data/{1}".format(Hadoop.hadoopdir, rel)
        cmdExecute(Storage.master, cmd)
        cmd = "{0}/bin/hadoop fs -copyFromLocal {1}/{2}.*.parquet /tpch_data/{2}/.".format(Hadoop.hadoopdir, TpchWorkload.dbgenParquet, rel)
        cmdExecute(Storage.master, cmd)

    #cmd = "{0}/bin/hadoop fs -copyFromLocal {1}/* /tpch_data/.".format(Hadoop.hadoopdir, TpchWorkload.dbgenParquet)
    #cmdExecute(Storage.master, cmd)
    
    cmd = "{0}/bin/hadoop fs -ls /tpch_data".format(Hadoop.hadoopdir)
    cmdExecute(Storage.master, cmd)

def loadTpchSchema():
    # create schema into hive
    cmd = "sudo rm -r {0}/metastore_db".format(Hive.hivedir)
    cmdExecute(Hive.master, cmd)

    cmd = "sudo {0}/bin/beeline -u jdbc:hive2:// -f {1}".format(Hive.hivedir, Hive.tpchschema)
    cmdExecute(Hive.master, cmd)

def setupHiveHdfs():
    # create dir
    cmd = "{0}/bin/hadoop fs -mkdir -p /usr/hive/warehouse".format(Hadoop.hadoopdir)
    cmdExecute(Storage.master, cmd)
    # set permissions
    cmd = "{0}/bin/hadoop fs -chmod -R 777 /usr/hive/warehouse/".format(Hadoop.hadoopdir)
    cmdExecute(Storage.master, cmd)

def main(action, use_tpch, schema):
    if (action == 'start'):
       if schema:
           print "\nLoading TPCH schema"
           loadTpchSchema()
       if use_tpch:
           print "\nLoading TPCH data"
           loadTpchData()
       setupHiveHdfs()
       print "\nStarting Hive"
       confMaster()
       startHive()
    elif (action == 'stop'):
       print "\nStopping Hive"
       stopHive()
    else:
       print "Usage: <start|stop> Default: start"

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-a", "--action", help="Starts/stops hive. Options:start|stop")
    parser.add_argument("-l", "--load_tpch", help="Loads into HDFS", action="store_true", required=False)
    parser.add_argument("-s", "--schema", help="Setup TPCH schema.", action="store_true", required=False)
    args = parser.parse_args()
    action = "start"
    if (args.action is not None):
        action = args.action
    main(action, args.load_tpch, args.schema)
