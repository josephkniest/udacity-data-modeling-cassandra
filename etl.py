import cassandra
import csv
import os

def process_csv(dir, paths):

    """process_csv

    Read in csv files within "dir", normalize and output a single
    large csv compilation thereof with normalized columns

    Parameters:
    dir (string): Root filepath containing csvs
    paths (array): List of csv file names without the directory portion

    """

    csvrows = []

    # cache all the csv rows from each file in memory
    for f in paths:
        file = dir + '/' + f
        print(file)
        with open(file, 'r', encoding = 'utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
                csvrows.append(line)

    # write out the csv rows to a single denormalized csv file
    csv.register_dialect('dialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as outFile:
        writer = csv.writer(outFile, dialect='dialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length','level','location','sessionId','song','userId'])
        for row in csvrows:
            if row[0] == '':
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

def insert_data_into_cassandra():

    """insert_data_into_cassandra

    Lay down cassandra keyspace and tables
    Read data from file './event_datafile_new.csv' and insert into

    """
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

    session.execute("""
        CREATE TABLE IF NOT EXISTS sparkify.playsBySessionAndItem(
            sessionId int,
            itemInSession int,
            artist text,
            songTitle text,
            songLen text,
            PRIMARY KEY (sessionId, itemInSession)
        ) WITH CLUSTERING ORDER BY (itemInSession ASC)
    """)

    with open('event_datafile_new.csv', encoding = 'utf8') as file:
        csvreader = csv.reader(file)
        csvreader.__next__()
        for line in csvreader:
            session.execute("""
                INSERT INTO sparkify.playsBySessionAndItem(sessionId, itemInSession, artist, songTitle, songLen)
                VALUES ({sessionId}, {itemInSession}, '{artist}', '{songTitle}', '{songLen}')
            """.format(sessionId = line[8], itemInSession = line[3], artist = line[0].replace("'", "''"), songTitle = line[9].replace("'", "''"), songLen = line[5].replace("'", "''")))


    rows = session.execute('SELECT * FROM sparkify.playsBySessionAndItem WHERE sessionId=338 AND itemInSession=4')
    for row in rows:
        print(row)

    session.shutdown()
    cluster.shutdown()

def main():

    dir = os.getcwd() + '/data'
    # Normalize csvs and rewrite them to disc as a single composite file (python 2: "next()"; python 3: __next__())
    process_csv(dir, os.walk(dir).__next__()[2])

    # Insert csv into cassandra
    insert_data_into_cassandra()

if __name__ == "__main__":
    main()

