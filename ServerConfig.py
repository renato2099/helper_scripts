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

class Storage:
    servers = ['euler04', 'euler05', 'euler06', 'euler07', 'euler08', 'euler09', 'euler10', 'euler11']
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
    builddir      = "/mnt/local/mpilman/builddirs/tellrelease"

Storage.storage = TellStore

class Tpcc:
    servers0      = ['euler03', 'euler12']
    servers1      = ['euler03', 'euler12'] + TellStore.servers
    warehouses    = 320
    storage       = Storage.storage
    builddir      = TellStore.builddir

class Client:
    numClients = 10
    logLevel   = 'FATAL'
    runTime    = 7*60

class Tpch:
    dbgenFiles = '/mnt/local/tell/tpch_2_17_0/dbgen'

