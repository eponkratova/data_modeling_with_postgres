#importing libabriries
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np


#The function lists all json files from the songs folder, adds them to the data frame and executes the insert query defined in the sql_queries.py file one file at a time. 
def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df.loc[:,["song_id","title", "artist_id", "year","duration"]].drop_duplicates(subset='song_id', keep="first").values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df.loc[:,["artist_id","artist_name", "artist_location", "artist_latitude","artist_longitude"]].drop_duplicates(subset='artist_id', keep="first").values[0])
    cur.execute(artist_table_insert, artist_data)

#The function lists all json files from the log folder, adds them to the data frame and executes the insert query defined in the sql_queries.py file one file at a time. 
def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts,unit='ms')
    
    # insert time data records
    time_data = np.transpose(np.array([df["ts"].values, t.dt.hour.values, t.dt.day.values, t.dt.week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values]))
    column_labels = np.asarray(['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
    time_df = pd.DataFrame(data= np.vstack((column_labels,time_data))[1:,:],
                      columns = np.vstack((column_labels,time_data))[0,:]).drop_duplicates(subset='start_time', keep="first")

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:,["userId", "firstName", "lastName", "gender","level"]].drop_duplicates(subset='userId', keep="first")

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        df["songid"]=songid
        df["artistid"] = artistid    
        df["songplay_id"] = range(1, 1+len(df))            

        # insert songplay record
        songplay_data = list(df.loc[:,["songplay_id","ts","userId","level","song_id","artist_id","sessionId","location","userAgent"]].values[0])
        cur.execute(songplay_table_insert, songplay_data)
        #conn.commit()

#The function establishes a PostgreSQL database connection; after successful execution, it closes the cursor and PostgreSQL database connection
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

#The dunction loops through the json files and creates a list of files. 
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



if __name__ == "__main__":
    main()