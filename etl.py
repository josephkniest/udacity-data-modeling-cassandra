import cassandra
import csv
import os

def main():

    # For python 2 use "next()"
    files = os.walk(os.getcwd() + '/data').__next__()

    print(files[2])

    from cassandra.cluster import Cluster
    cluster = Cluster()
    session = cluster.connect()

    session.execute('DROP KEYSPACE IF EXISTS sparkify')

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sparkify
        WITH REPLICATION = {
           'class' : 'SimpleStrategy',
           'replication_factor' : 1
        };
    """)

    session.execute('USE sparkify')

    session.execute('CREATE TABLE IF NOT EXISTS sparkify.songplays(id UUID PRIMARY KEY)')

if __name__ == "__main__":
    main()

