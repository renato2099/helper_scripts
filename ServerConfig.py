import getpass
import os

class General:
    infinibandIp = {
            'euler01': '192.168.0.11',
            'euler02': '192.168.0.12',
            'euler03': '192.168.0.13',
            'euler04': '192.168.0.14',
            'euler05': '192.168.0.15',
            'euler06': '192.168.0.16',
            'euler07': '192.168.0.17',
            'euler08': '192.168.0.18',
            'euler09': '192.168.0.19',
            'euler10': '192.168.0.10',
            'euler11': '192.168.0.21',
            'euler12': '192.168.0.22'
            }
    username     = getpass.getuser()
    sourceDir    = "/mnt/local/{0}/tell".format(username)
    builddir     = "/mnt/local/{0}/builddirs/tellrelease".format(username)
    javahome     = "/mnt/local/tell/java8"

class Storage:
    #servers    = ['euler04', 'euler05', 'euler06']
    servers    = ['euler08', 'euler09', 'euler10', 'euler12']
    servers1   = []
    master     = "euler07"

##########################
# Storage Implementations
##########################

class Kudu:
    clean       = True
    master_dir  = '/mnt/data/kudu-master'
    tserver_dir = '/mnt/data/kudu-tserver'
    tserver_dir1= '/mnt/data/kudu-tserver1'

class TellStore:
    approach           = "columnmap"
    defaultMemorysize  = 0xC90000000 if approach == "logstructured" else 0xE00000000
    defaultHashmapsize = 0x20000000  if approach == "logstructured" else 0x2000000
    memorysize         = defaultMemorysize
    hashmapsize        = defaultHashmapsize
    builddir           = General.builddir
    scanMemory         = 10*1024*1024*1024 # 10GB
    scanThreads        = 3 if approach == "logstructured" else 2
    gcInterval         = 20
    scanShift          = 3

    @staticmethod
    def numServers():
        return len(Storage.servers) + len(Storage.servers1)

    @staticmethod
    def getCommitManagerAddress():
        return '{0}:7242'.format(General.infinibandIp[Storage.master])

    @staticmethod
    def getServerList():
        serversForList = lambda l, p: map(lambda x: '{0}:{1}'.format(General.infinibandIp[x], p), l)
        l = serversForList(Storage.servers, "7241") + serversForList(Storage.servers1, "7240")
        return reduce(lambda x,y: '{0};{1}'.format(x,y), l)

    @staticmethod
    def rsyncBuild():
        rsync = lambda host: os.system('rsync -ra {0}/ {1}@{2}:{0}'.format(General.builddir, General.username, host))
        hosts = set([Storage.master] + Storage.servers + Storage.servers1)
        for host in hosts:
            rsync(host)

    @staticmethod
    def setDefaultMemorySizeAndScanThreads():
        if TellStore.approach == "logstructured":
            TellStore.memorysize  = 0xC90000000
            TellStore.hashmapsize = 0x20000000
            TellStore.scanThreads = 3
        else:
            TellStore.memorysize  = 0xE00000000
            TellStore.hashmapsize = 0x2000000
            TellStore.scanThreads = 2


class Hadoop:
    #namenode       = Storage.master
    hdfsport       = '20042'
    #datanodes      = Storage.servers
    hadoopdir      = "/mnt/local/tell/hadoop"
    datadir        = "/mnt/ramfs/hadoop"
    datadir1        = "/mnt/ramfs/hadoop1"
    datadirSz      = "100"
    dfsreplication = "1"

class Zookeeper:
    #zkserver      = Storage.master
    zkdir         = "/mnt/local/tell/zookeeper"
    ticktime      = '60000'
    datadir       = '/mnt/data/zk_data'
    clientport    = '2181'
    maxclients    = '6000'

class Hbase:
    #hmaster       = Storage.master
    #hregions      = Storage.servers 
    hbasedir      = "/mnt/local/tell/hbase"
    regionsize    = '49294967296'
    #hdfsNamenode  = Hadoop.namenode
    zkDataDir     = Zookeeper.datadir

class Cassandra:
    datadirSz     = 50
    listenaddr    = ""
    casdir        = "/mnt/local/tell/cassandra"
    logdir        = "/mnt/data/cassandra/cass_log"
    datadir       = "/mnt/ramfs/cassandra/cass_data"
    nativeport    = '9042'
    rpcport       = '9160'
    storageport   = '7000'
    sslport       = '7001'
    logdir1        = "/mnt/data/cassandra/cass_log1"
    datadir1       = "/mnt/ramfs/cassandra/cass_data1"
    nativeport1    = '9041'
    rpcport1       = '9161'
    storageport1   = '6999'
    sslport1       = '7002'
    jmxport1       = '7198'

class Hive:
    master            = Storage.master
    hivedir           = "/mnt/local/tell/hive"
    metastoreuri      = master
    metastoreport     = "9083"
    metastoretimeout  = "1m"
    thriftport        = "10000"
    thriftbindhost    = master

class Ramcloud:
    ramclouddir       = "/mnt/local/tell/RAMCloud/obj.master"
    backupdir         = "/mnt/data/rc_data"
    backupdir1        = "/mnt/data/rc_data1"
    backupfile        = backupdir + "/rc.bk"
    backupfile1       = backupdir1 + "/rc.bk"
    storageport       = 1101
    storageport1      = 1102
    memorysize        = 48000
    boost_lib         = "/mnt/local/tell/boost/lib"
    timeout           = 12000

class Memsql:
    msqlopsdir      = "/mnt/local/tell/memsql-ops"
    msqlbin         = "/mnt/local/tell/memsqlbin_amd64.tar.gz"
    msqlopsdata     = "/mnt/data/memsql_data"
    msqlinstalldir  = "/mnt/data/memsql_installs"
    msqlopsport     = "9000"

#############################
# Used Storage Implementation
#############################

Storage.storage = Memsql

###################
# Processing Server
###################

class Microbench:
    servers0          = ['euler06', 'euler07']
    servers1          = []
    threads           = 1 if Storage.storage == TellStore else 4
    networkThreads    = 3
    numColumns        = 10
    scaling           = 50
    clients           = 10
    clientThreads     = 4
    analyticalClients = 1
    insertProb        = 0.166
    deleteProb        = 0.166
    updateProb        = 0.166
    time              = 5
    noWarmUp          = False
    infinioBatch      = 16
    txBatch           = 200
    onlyQ1            = False
    oltpWaitTime      = 0 # for batch size 50: 119000 (50,000 per sec), 11900 (500,000 per sec), 2975000 (2000 per sec)
    result_dir        = '/mnt/local/{0}/mbench_results'.format(General.username)
    javaDir           = '/mnt/local/{0}/mbench_jars'.format(General.username)

    @staticmethod
    def rsyncJars():
        rsync = lambda host: os.system('rsync -ra {0}/ {1}@{2}:{0}'.format(Microbench.javaDir, General.username, host))
        mkjavadir = lambda host: os.system('ssh -A {1}@{2} mkdir -p {0}'.format(Microbench.javaDir, General.username, host))
        hosts = set(Microbench.servers0 + Microbench.servers1)
        for host in hosts:
            mkjavadir(host)
            rsync(host)

    @staticmethod
    def rsyncBuild():
        rsync = lambda host: os.system('rsync -ra {0}/ {1}@{2}:{0}'.format(General.builddir, General.username, host))
        hosts = set(Microbench.servers0 + Microbench.servers1)
        for host in hosts:
            rsync(host)

    @staticmethod
    def getServerList():
        serversForList = lambda l, p: map(lambda x: '{0}:{1}'.format(x, p), l)
        l = serversForList(Microbench.servers0, "8713") + serversForList(Microbench.servers1, "8712")
        return reduce(lambda x,y: '{0};{1}'.format(x,y), l)

class Java:
    telljava       = General.builddir + "/telljava"
    telljar        = telljava + "/telljava-1.0.jar"
    javahome       = "/mnt/local/tell/java8"


class Spark:
    master         = 'euler11'
    slaves         = ['euler05', 'euler06', 'euler07', 'euler08', 'euler09', 'euler10']
    sparkdir       = "/mnt/local/tell/spark"
    telljava       = General.builddir + "/telljava"
    telljar        = telljava + "/telljava-1.0.jar"
    javahome       = "/mnt/local/tell/java8"
    jarsDir        = "/mnt/local/{0}/spark_jars".format(General.username)
    tmpDir         = "/mnt/data/sparktmp"
    numCores       = 8
    tellPartitions = 48

class Presto:    
    coordinator      = 'euler04'
    nodes            = ["euler01", "euler02"]
    prestodir        = "/mnt/local/tell/presto"
    localPresto      = "/mnt/local/mpilman/presto/presto-server-0.138-SNAPSHOT"
    datadir          = "/mnt/data/prestotmp"
    querymaxmem      = "50GB"
    querymaxnode     = "30GB"
    jvmheap          = "100G" # java memory is specified differently than presto
    jvmheapregion    = "32M"
    httpport         = "8080"
    loglevel         = "INFO"
    splitsPerMachine = 8
    debug            = True

class Tpcc:
    servers0      = ['euler02']
    servers1      = []
    warehouses    = 50
    storage       = Storage.storage
    builddir      = General.builddir

    @staticmethod
    def rsyncBuild():
        rsync = lambda host: os.system('rsync -ra {0}/ {1}@{2}:{0}'.format(General.builddir, General.username, host))
        hosts = set(Tpcc.servers0 + TellStore.servers1)
        for host in hosts:
            rsync(host)

class Tpch:
    builddir      = General.builddir
    servers0      = ["euler10"]
    servers1      = []
    clients       = ["euler12"]
    storage       = Storage.storage
    builddir      = General.builddir
    scalingFactor = 10
    @staticmethod
    def getServerList():
        serversForList = lambda l, p: map(lambda x: '{0}:{1}'.format(x, p), l)
        l = serversForList(Tpch.servers0, "8713") + serversForList(Tpch.servers1, "8712")
        return reduce(lambda x,y: '{0};{1}'.format(x,y), l)

class YCSB:
    servers0      = Tpcc.servers0
    servers1      = Tpcc.servers1
    builddir      = General.builddir
    ycsbdir       = "/mnt/local/{0}/YCSB".format(General.username)
    mvnDir        = "/mnt/local/tell/apache-maven-3.3.9/bin"
    networkThread = 4
    clientThreads = 32 #32 * (len(servers0) + len(servers1))

class Aim:
    rtaservers0   = ['euler02']
    rtaservers1   = []
    sepservers0   = ['euler03', 'euler08', 'euler09', 'euler10', 'euler11']
    sepservers1   = [] #rtaservers0 + ['euler01', 'euler02']
    serverthreads = 4
    schemaFile    = General.builddir + "/watch/aim-benchmark/meta_db.db"
    subscribers   = 10 * 1024 * 1024
    messageRate   = 20 * 1000
    batchSize     = 5
    numSEPClients = 5
    numRTAClients = 2
    builddir      = General.builddir

    @staticmethod
    def rsyncBuild():
        rsync = lambda host: os.system('rsync -ra {0}/ {1}@{2}:{0}'.format(General.builddir, General.username, host))
        hosts = set(Aim.sepservers0 + Aim.sepservers1 + Aim.rtaservers0 + Aim.rtaservers1)
        for host in hosts:
            rsync(host)

#####################
# Client and Workload
#####################

class Client:
    numClients = 8
    logLevel   = 'FATAL'
    runTime    = 7*60

class TpchWorkload:
    dbgenFiles  = '/mnt/SG/braunl-tpch-data/all/{0}'.format(Tpch.scalingFactor)
    updateFiles = '/mnt/SG/braunl-tpch-data/updates/{0}'.format(Tpch.scalingFactor)

class YCSBWorkload:
    recordcount         = (len(Storage.servers) + len(Storage.master)) * 7500000
    #recordcount         = len(Hbase.hregions) * 7 500 000
    operationcount      = 170000 # operations per client!
    workload            = "com.yahoo.ycsb.workloads.CoreWorkload"
    
    readallfields       = True
    
    readproportion      = 0.4
    updateproportion    = 0.3
    scanproportion      = 0
    insertproportion    = 0.3
    
    requestdistribution = "uniform"
