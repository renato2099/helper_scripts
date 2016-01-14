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

class Storage:
    servers = ['euler04', 'euler05', 'euler06', 'euler07']
    master  = "euler01"

class Kudu:
    clean       = True
    master      = Storage.master
    tservers    = Storage.servers
    master_dir  = '/mnt/data/kudu-master'
    tserver_dir = '/mnt/data/kudu-tserver'

class TellStore:
    commitmanager = Storage.master
    servers       = Storage.servers
    approach      = "columnmap"
    memorysize    = "0xD00000000" if approach == "logstructured" else "0xE00000000"
    hashmapsize   =  "0x10000000" if approach == "logstructured" else "0x20000"
    builddir      = General.builddir

Storage.storage = Kudu

class Tpcc:
    servers0      = ['euler02', 'euler08']
    servers1      = ['euler02', 'euler08'] + TellStore.servers
    warehouses    = 200
    storage       = Storage.storage
    builddir      = TellStore.builddir

class YCSB:
    servers0      = Tpcc.servers0
    servers1      = Tpcc.servers1
    builddir      = TellStore.builddir
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
    dbgenFiles = '/mnt/local/tell/tpch_2_17_0/dbgen'

