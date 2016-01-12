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
    servers = ['euler04', 'euler05']
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
    approach      = "logstructured"
    memorysize    = "0xD00000000" if approach == "logstructured" else "0xE00000000"
    hashmapsize   =  "0x10000000" if approach == "logstructured" else "0x20000"
    builddir      = General.builddir

Storage.storage = TellStore

class Tpcc:
    servers0      = ['euler02']
    servers1      = ['euler02'] + TellStore.servers
    warehouses    = 100
    storage       = Storage.storage
    builddir      = TellStore.builddir

class YCSB:
    servers0      = []
    servers1      = ["euler03"]
    builddir      = TellStore.builddir
    ycsbdir       = "/mnt/local/{0}/YCSB".format(getpass.getuser())
    mvnDir        = "/mnt/local/tell/apache-maven-3.3.9/bin"
    workload      = "tell_test"
    networkThread = 4
    clientThreads = 1

class Client:
    numClients = 4
    logLevel   = 'FATAL'
    runTime    = 7*60

class Tpch:
    dbgenFiles = '/mnt/local/tell/tpch_2_17_0/dbgen'

