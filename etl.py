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
            session_id int,
            item_in_session int,
            artist text,
            song_title text,
            song_len text,
            PRIMARY KEY (session_id, item_in_session)
        ) WITH CLUSTERING ORDER BY (item_in_session ASC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS sparkify.artistSongUserByUserIdSessionId(
            session_id int,
            item_in_session int,
            user_id int,
            artist text,
            song_title text,
            user_first text,
            user_last text,
            PRIMARY KEY (session_id, user_id, item_in_session)
        ) WITH CLUSTERING ORDER BY (user_id ASC, item_in_session ASC)
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS sparkify.userFirstLastBySongListenedTo(
            song_title text,
            artist text,
            session_id int,
            user_first text,
            user_last text,
            PRIMARY KEY (song_title, session_id)
        ) WITH CLUSTERING ORDER BY (session_id ASC)
    """)

    with open('event_datafile_new.csv', encoding = 'utf8') as file:
        csvreader = csv.reader(file)
        csvreader.__next__()
        for line in csvreader:
            session.execute("""
                INSERT INTO sparkify.playsBySessionAndItem(session_id, item_in_session, artist, song_title, song_len)
                VALUES ({session_id}, {item_in_session}, '{artist}', '{song_title}', '{song_len}')
            """.format(session_id = line[8], item_in_session = line[3], artist = line[0].replace("'", "''"), song_title = line[9].replace("'", "''"), song_len = line[5].replace("'", "''")))
            session.execute("""
                INSERT INTO sparkify.artistSongUserByUserIdSessionId(session_id, item_in_session, user_id, artist, song_title, user_first, user_last)
                VALUES ({session_id}, {item_in_session}, {user_id}, '{artist}', '{song_title}', '{user_first}', '{user_last}')
            """.format(
                session_id = line[8],
                item_in_session = line[3],
                user_id = line[10],
                artist = line[0].replace("'", "''"),
                song_title = line[9].replace("'", "''"),
                user_first = line[1].replace("'", "''"),
                user_last = line[4].replace("'", "''")
            ))
            session.execute("""
                INSERT INTO sparkify.userFirstLastBySongListenedTo(song_title, artist, session_id, user_first, user_last)
                VALUES ('{song_title}', '{artist}', {session_id}, '{user_first}', '{user_last}')
            """.format(
                song_title = line[9].replace("'", "''"),
                artist = line[0].replace("'", "''"),
                session_id = line[8],
                user_first = line[1].replace("'", "''"),
                user_last = line[4].replace("'", "''")
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

    rows = session.execute('SELECT artist, song_title, song_len FROM sparkify.playsBySessionAndItem WHERE session_id=338 AND item_in_session=4')

    session.shutdown()
    cluster.shutdown()

    # We are only expecting one row here
    for row in rows:
        return {"artist": row[0], "song_title": row[1], "song_len": row[2]}

    return {}

def artist_song_user_from_userid_session():

    """artist_song_user_from_userid_session

    Return:
    (array) Set of cassandra rows for artist, song and user first/last that
    listened to the song (should all be the same user) for user id 10 with
    session id 182

    """

    cluster = Cluster()
    session = cluster.connect()

    rows = session.execute("""
        SELECT artist, song_title, user_first, user_last
        FROM sparkify.artistSongUserByUserIdSessionId
        WHERE session_id=182 AND user_id=10
        GROUP BY item_in_session
    """)

    session.shutdown()
    cluster.shutdown()

    result_set = []
    for row in rows:
        result_set.append(row)

    return result_set

def users_from_song():

    """users_from_song

    Return:
    (array) Set of cassandra rows for user first/last that listened to
    the song 'All Hands Against His Own'
    """

    cluster = Cluster()
    session = cluster.connect()

    rows = session.execute("""
        SELECT user_first, user_last
        FROM sparkify.userFirstLastBySongListenedTo
        WHERE song_title='All Hands Against His Own'
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

    print('(1):')
    print(plays_by_session_and_item())
    print('(2):')
    print(artist_song_user_from_userid_session())
    print('(3):')
    print(users_from_song())

if __name__ == "__main__":
    main()

