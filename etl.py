import cassandra
import csv
import os

def main():

    # For python 2 use "next()"
    files = os.walk(os.getcwd() + '/data').__next__()

    print(files[2])

if __name__ == "__main__":
    main()

