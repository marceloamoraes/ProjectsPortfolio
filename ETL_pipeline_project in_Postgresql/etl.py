import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Insert selected columns to already created Pstgresql database table
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert song record
    song_data = df[['song_id','title','artist_id','year','duration']].values.tolist()
    song_data = song_data[0] 
    cur.execute(song_table_insert, song_data)
    
    # selecting artists records, 1st row only
    artist_data = artist_data = df.loc[0:1,['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values.tolist()
    artist_data  = artist_data[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Insert data from all the log files into Postgresql tables
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filtering records by NextSong, column 'page'
    df = df[df.page=='NextSong']
    
    #Creating a transformed dimension converting it from milliseconds to USC time
    df['ts_t'] = pd.to_datetime(df['ts'], unit='ms')

    # convert timestamp column to datetime
    start_time = df.ts_t.tolist()
    hour = df.ts_t.dt.hour.tolist()
    day = df.ts_t.dt.day.tolist()
    weeky = df.ts_t.dt.week.tolist()
    month = df.ts_t.dt.month.tolist()
    year = df.ts_t.dt.year.tolist()
    weekday = df.ts_t.dt.weekday.tolist()
    
    # insert time data records
    time_data = [start_time]
    lista = [(hour,day,weeky,month,year,weekday)]
    
    #Loop to add each componenent from the list to to variable time_data
    for i in lista:
        time_data.extend(i)
    
    #Defining columns names for Dataframe 
    column_labels = (['start_time','hour', 'day', 'week','month','year','weekday'])
    
    #coverting a dictionary datatype to a dataframe
    time_dic = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dic) 
    
    #Loop thru each row and save it to the table in the database per sql query
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # Loop thru each row and save data to the table in the database per sql query
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    #  Loop thru each row and save data to the table in the database per sql query
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts_t,row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Interate thru every file, collect the modeled data and store in the proper
    tables created in Postgresql database instance
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
    serves as a starting point for the execution of a program
    from the connection, to the modeling of data, storage and 
    closing connection. This function pull the whole program.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()