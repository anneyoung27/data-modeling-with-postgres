import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# def get_files(filepath):
#     all_files = []
#     for root, dirs, files in os.walk(filepath):
#         files = glob.glob(os.path.join(root,'*.json'))
#         for f in files :
#             all_files.append(os.path.abspath(f))
    
#     return all_files

# song_files = get_files(os.path.join(os.getcwd(), "data/song_data"))

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == 'NextSong'].reindex() # 1. 

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms') # Time dimension
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.isocalendar().week]
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    dictionary = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(dictionary).dropna()

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        results = cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        
        songplay_data = (pd.to_datetime(row.ts, unit="ms"), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    try:
        conn = psycopg2.connect(
            user="postgres",\
            password="<password>",\
            host="localhost",\
            port="5432",\
            dbname="sparkifydb"
        )
        print(f"[INFO] Connected to PostgreSQL")
    except:
        print(f"[INFO] Connection to the PostgreSQL database failed!")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
