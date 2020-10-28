import cassandra
import csv
import os

def process_csv(dir, paths):

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

def main():

    dir = os.getcwd() + '/data'
    # For python 2 use "next()"
    process_csv(dir, os.walk(dir).__next__()[2])

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

