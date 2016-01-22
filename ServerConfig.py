import getpass

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
    sourceDir     = "/mnt/local/{0}/tell".format(getpass.getuser())
    builddir      = "/mnt/local/{0}/builddirs/tellrelease".format(getpass.getuser())
    javahome       = "/mnt/local/tell/java8"

class Storage:
    servers    = ['euler02', 'euler03', 'euler04', 'euler05']
    master     = 'euler01'
    twoPerNode = False

class Kudu:
    clean       = True
    master      = Storage.master
    tservers    = Storage.servers
    master_dir  = '/mnt/data/kudu-master'
    tserver_dir = '/mnt/data/kudu-tserver'

class TellStore:
    commitmanager      = Storage.master
    servers            = Storage.servers
    approach           = "columnmap"
    defaultMemorysize  = "0xD00000000" if approach == "logstructured" else "0xE00000000"
    defaultHashmapsize = "0x10000000" if approach == "logstructured" else "0x20000"
    memorysize         = defaultMemorysize
    hashmapsize        = defaultHashmapsize
    builddir           = General.builddir
    scanMemory         = 10*1024*1024*1024 # 1GB
    scanThreads        = 2

    @staticmethod
    def getCommitManagerAddress():
        return '{0}:7242'.format(General.infinibandIp[TellStore.commitmanager])

    @staticmethod
    def getServerList():
        serversForList = lambda l, p: map(lambda x: '{0}:{1}'.format(General.infinibandIp[x], p), l)
        l = serversForList(TellStore.servers, "7241")
        if Storage.twoPerNode:
            l += serversForList(TellStore.servers, "7240")
        return reduce(lambda x,y: '{0};{1}'.format(x,y), l)

class Cassandra:
    servers = Storage.servers
    master  = Storage.master

Storage.storage = TellStore

class Tpcc:
    servers0      = ['euler02']
    servers1      = [] + TellStore.servers
    warehouses    = 50
    storage       = Storage.storage
    builddir      = General.builddir

class YCSB:
    servers0      = Tpcc.servers0
    servers1      = Tpcc.servers1
    builddir      = General.builddir
    ycsbdir       = "/mnt/local/{0}/YCSB".format(getpass.getuser())
    mvnDir        = "/mnt/local/tell/apache-maven-3.3.9/bin"
    networkThread = 4
    clientThreads = 32 #32 * (len(servers0) + len(servers1))

class YCSBWorkload:
    recordcount         = len(Storage.servers) * 30000000
    operationcount      = 100000 # operations per client!
    workload            = "com.yahoo.ycsb.workloads.CoreWorkload"
    
    readallfields       = True
    
    readproportion      = 0.4
    updateproportion    = 0.3
    scanproportion      = 0
    insertproportion    = 0.3
    
    requestdistribution = "uniform"

class Client:
    numClients = 8
    logLevel   = 'FATAL'
    runTime    = 7*60

class Tpch:
    builddir   = General.builddir
    server     = "euler12"
    client     = "euler12"
    scaling    = 10
    dbgenFiles = '/mnt/SG/braunl-tpch-data/all/'

class Spark:
    master         = 'euler04'
    slaves         = ['euler05', 'euler06', 'euler07', 'euler08', 'euler09', 'euler10']
    sparkdir       = "/mnt/local/tell/spark"
    telljava       = General.builddir + "/telljava"
    telljar        = telljava + "/telljava-1.0.jar"
    javahome       = "/mnt/local/tell/java8"
    jarsDir        = "/mnt/local/{0}/spark_jars".format(getpass.getuser())
    numCores       = 8
    tellPartitions = 48

class Aim:
    sepservers0   = []
    sepservers1   = TellStore.servers
    rtaservers0   = ["euler06"]
    rtaservers1   = ["euler06"]
    schemaFile    = General.sourceDir + "/watch/aim-benchmark/server/meta_db.db"
    subscribers   = 20 * 1024 * 1024
    messageRate   = 20 * 1000
    batchSize     = 5
    numSEPClients = 5
    numRTAClients = 1
    builddir      = General.builddir

class Hadoop:
    namenode       = 'euler10'
    hdfsport       = '20042'
    #datanodes     = ['euler10', 'euler11', 'euler12']
    datanodes      = ['euler10']
    hadoopdir      = "/mnt/local/tell/hadoop"
    datadir        = "/mnt/ramfs/hadoop"
    datadirSz      = "20"
    dfsreplication = "1"

class Hbase:
    hmaster       = 'euler10'
    #hmaster       = 'euler08'
    #hregions      = ['euler07', 'euler06']
    hregions      = ['euler10']
    hbasedir      = "/mnt/local/tell/hbase"
    hdfsNamenode  = Hadoop.namenode

class Zookeeper:
    zkinstances   = ['euler10']
    zkdir         = "/mnt/local/tell/zookeeper"
    zkdata        = "/mnt/ramfs/zk"
    clientport    = "2181"
    
