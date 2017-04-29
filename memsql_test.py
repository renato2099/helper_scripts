#!/usr/bin/python
import time
import threading
from memsql.common import database
from memsql.common.query_builder import multi_insert

HOST = "euler07"
PORT = 3306
USER = "root"
PASSWORD = ""

DATABASE = "test"
TABLE = "tbl"

NUM_WORKERS = 5
BATCH_SIZE = 10000000
#BATCH_SIZE = 50

QUERY_TEXT = "INSERT INTO tbl VALUES ({0});"
 #   TABLE, ",".join(["()"] * BATCH_SIZE))

def get_connection(db=DATABASE):
    """ Returns a new connection to the database. """
    return database.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, database=db)

def setup_test_db():
    """ Create a database and table for this benchmark to use. """

    with get_connection(db="information_schema") as conn:

        print('Creating database %s' % DATABASE)
        conn.query('CREATE DATABASE IF NOT EXISTS %s' % DATABASE)
        conn.query('USE %s' % DATABASE)
        print('Dropping table %s' % TABLE)
        conn.query('DROP table %s' % TABLE)

        print('Creating table %s' % TABLE)
        conn.query('CREATE TABLE IF NOT EXISTS tbl (id INT)')

class InsertWorker(threading.Thread):

    def __init__(self, num_inserts):
        super(InsertWorker, self).__init__()
        self.num_inserts = num_inserts

    def run(self):
        start_id = BATCH_SIZE * self.num_inserts
        end_id = start_id + BATCH_SIZE
        print "Range {0} - {1}".format(start_id, end_id)
        with get_connection() as conn:
            while start_id < end_id:
                conn.execute(QUERY_TEXT.format(start_id))
                start_id += 1

def insert():
    workers = [ InsertWorker(i) for i in range(NUM_WORKERS) ]
    [ worker.start() for worker in workers ]
    [ worker.join() for worker in workers ]


def query():
    with get_connection() as conn:
        start = time.clock()
        mmax = conn.get("SELECT MAX(id) AS mm FROM tbl")
        end = time.clock()

        print "Elapsed time: {0} secs".format(end-start)

if __name__ == '__main__':
    setup_test_db()
    insert()
    query()
