# Sparkify songplay ETL into Cassandra

An ETL that transfers songplay data from on disc csvs to cassandra

## Data insertion

Run ```python3 etl.py``` There is currently a python3 dependency

## Data questions

Ultimately three tables are populated within cassandra to answer
the following questions about the songplay data:

1) Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

2) Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

3) Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

The etl will print the answers to those three questions by way of cassandra queries after the data insertion is complete.


