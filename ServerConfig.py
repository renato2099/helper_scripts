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
    servers = ['euler04', 'euler05', 'euler06', 'euler07']
    master  = "euler01"

class Kudu:
    clean       = True
    master      = Storage.master
    tservers    = Storage.servers
    master_dir  = '/mnt/data/kudu-master'
    tserver_dir = '/mnt/data/kudu-tserver'

Storage.storage = Kudu

class TellStore:
    commitmanager = Storage.master
    servers       = Storage.servers
    approach      = "columnmap"
    memorysize    = "0xE00000000"
    hashmapsize   =  "0x10000000" if approach == "logstructured" else "0x20000"
    builddir      = "/mnt/local/mpilman/builddirs/tellrelease"

class Tpcc:
    servers0      = ['euler03', 'euler08']
    servers1      = TellStore.servers + ["euler03", "euler08"]
    warehouses    = 160
    storage       = Storage.storage
    builddir      = TellStore.builddir

class Client:
    numClients = 20
    logLevel   = 'FATAL'
    runTime    = 7*60

