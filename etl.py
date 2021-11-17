import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def process_song_file(cur, filepath):
    
    """
    This function takes a file from the song_data directory and inserts specific information in both 
    the songs and artists tables. 
    
    """
    
    # open song file
    song_files = get_files(filepath)

    # insert song record
    super_df_songs = pd.DataFrame(columns=['num_songs','artist_id','artist_latitude','artist_longitude','artist_location','artist_name','song_id','title','duration','year'])
    for file in song_files:
        filepath = file
        df = pd.read_json(filepath, lines = True)
        super_df_songs = super_df_songs.append(df,ignore_index=True)
    
    song_data = super_df_songs[['song_id', 'title', 'artist_id', 'year', 'duration']]
    for i in range(len(song_data)):
        single_song = list(song_data.values[i])
        cur.execute(song_table_insert, single_song)
    
    # insert artist record
    artist_data = super_df_songs[['artist_id','artist_name','artist_location','artist_latitude', 'artist_longitude']]
    artist_data.drop_duplicates('artist_id', inplace=True)
    for i in range(len(artist_data)):
        single_artist = list(artist_data.values[i])
        cur.execute(artist_table_insert, single_artist)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    """
    This function takes a file from the log_data directory and inserts specific information in both 
    the time and users tables. 
    
    """
    
    # open log file
    log_files = get_files(filepath)
    super_df_log = pd.DataFrame(columns=pd.read_json(log_files[0], lines = True).columns)
    for file in log_files:
        filepath = file
        df = pd.read_json(filepath, lines = True)
        super_df_log = super_df_log.append(df,ignore_index=True)
        
    df = super_df_log

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit = 'ms')
    df['ts'].head()
    
    # insert time data records
    time_data = []
    for line in df['ts']:
     time_data.append([line, line.hour, line.day, line.week, line.month, line.year, line.day_name()])

    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']

    time_df = pd.DataFrame(data=time_data, columns=column_labels)
    time_df.head()
    time_df.drop_duplicates('start_time', inplace=True)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df.head()
    user_df.drop_duplicates('userId', inplace=True)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
    
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            print('found')
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    This function takes files from specific directories and will in this case process (as func)
    either process_song_file or process_log_file on each file in the directory. 
    
    """
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
    
    """
    Here the main function makes a connection to the localhost, builds the cursor, and processes all the data 
    by putting it in the database
    
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=ciroazzi")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()