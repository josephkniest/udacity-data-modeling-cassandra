import cassandra
import csv
import os
from cassandra.cluster import Cluster
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

    session.execute("""
        CREATE TABLE IF NOT EXISTS sparkify.artistSongUserByUserIdSessionId(
            sessionId int,
            itemInSession int,
            userId int,
            artist text,
            songTitle text,
            userFirst text,
            userLast text,
            PRIMARY KEY (sessionId, userId, itemInSession)
        ) WITH CLUSTERING ORDER BY (userId ASC, itemInSession ASC)
    """)

    with open('event_datafile_new.csv', encoding = 'utf8') as file:
        csvreader = csv.reader(file)
        csvreader.__next__()
        for line in csvreader:
            session.execute("""
                INSERT INTO sparkify.playsBySessionAndItem(sessionId, itemInSession, artist, songTitle, songLen)
                VALUES ({sessionId}, {itemInSession}, '{artist}', '{songTitle}', '{songLen}')
            """.format(sessionId = line[8], itemInSession = line[3], artist = line[0].replace("'", "''"), songTitle = line[9].replace("'", "''"), songLen = line[5].replace("'", "''")))
            session.execute("""
                INSERT INTO sparkify.artistSongUserByUserIdSessionId(sessionId, itemInSession, userId, artist, songTitle, userFirst, userLast)
                VALUES ({sessionId}, {itemInSession}, {userId}, '{artist}', '{songTitle}', '{userFirst}', '{userLast}')
            """.format(
                sessionId = line[8],
                itemInSession = line[3],
                userId = line[10],
                artist = line[0].replace("'", "''"),
                songTitle = line[9].replace("'", "''"),
                userFirst = line[1].replace("'", "''"),
                userLast = line[4].replace("'", "''")
            ))

    session.shutdown()
    cluster.shutdown()

def plays_by_session_and_item():

    """plays_by_session_and_item

    Pull out the artist, song title and song length listened to within
    the session id of 338 and item in session 4. Must be called after 'insert_data_into_cassandra()'
    to get a non empty record

    Return:
    (dictionary) ['artist', 'song_title', 'song_length'] or [] if no records found

    """

    cluster = Cluster()
    session = cluster.connect()

    rows = session.execute('SELECT artist, songTitle, songLen FROM sparkify.playsBySessionAndItem WHERE sessionId=338 AND itemInSession=4')

    session.shutdown()
    cluster.shutdown()

    # We are only expecting one row here
    for row in rows:
        return {"artist": row[0], "song_title": row[1], "song_len": row[2]}

    return {}

def artist_song_user_from_userid_session():

    cluster = Cluster()
    session = cluster.connect()

    rows = session.execute("""
        SELECT artist, songTitle, userFirst, userLast
        FROM sparkify.artistSongUserByUserIdSessionId
        WHERE sessionId=182 AND userId=10
        GROUP BY itemInSession
    """)

    session.shutdown()
    cluster.shutdown()

    result_set = []
    for row in rows:
        result_set.append(row)

    return result_set

def main():

    dir = os.getcwd() + '/data'
    # Normalize csvs and rewrite them to disc as a single composite file (python 2: "next()"; python 3: __next__())
    process_csv(dir, os.walk(dir).__next__()[2])

    # Insert csv into cassandra
    insert_data_into_cassandra()

    print(artist_song_user_from_userid_session())

if __name__ == "__main__":
    main()

