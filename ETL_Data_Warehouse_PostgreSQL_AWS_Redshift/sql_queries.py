import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events";
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs";
songplay_table_drop = "DROP TABLE IF EXISTS songplay";
user_table_drop = "DROP TABLE IF EXISTS users";
song_table_drop = "DROP TABLE IF EXISTS songs";
artist_table_drop = "DROP TABLE IF EXISTS artists";
time_table_drop = "DROP TABLE IF EXISTS time";

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    event_id            INTEGER     IDENTITY(0,1) NOT NULL,
    artists             VARCHAR(250),
    auth                VARCHAR(250),
    firstName           VARCHAR(250),
    gender              VARCHAR(50),
    itemInSession       INTEGER,
    lastName            VARCHAR(250),
    length              FLOAT8,
    level               VARCHAR(50),
    location            VARCHAR,
    method              VARCHAR(50),
    page                VARCHAR(50),
    registration        FLOAT8,
    sessionId           INTEGER,
    song                VARCHAR(400),
    status              INTEGER,
    ts                  BIGINT,
    userAgent           VARCHAR,
    user_id             VARCHAR
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INTEGER,
    artist_id           VARCHAR(30),
    artist_latitude     DOUBLE PRECISION,
    artist_longitude    DOUBLE PRECISION,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR(50),
    title               VARCHAR,
    duration            FLOAT8,
    year                INTEGER
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay ( 
    songplay_id INTEGER     IDENTITY(0,1) PRIMARY KEY,
    start_time  TIMESTAMP   NOT NULL,   
    user_id     VARCHAR     DISTKEY NOT NULL,         
    level       VARCHAR,
    song_id     VARCHAR     SORTKEY,
    artist_id   VARCHAR,
    sessionId   VARCHAR,
    location    VARCHAR,
    userAgent   VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users ( 
    user_id    VARCHAR      SORTKEY PRIMARY KEY,
    first_name VARCHAR,
    last_name  VARCHAR,
    gender     VARCHAR,
    level      VARCHAR
    )diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs ( 
    song_id    VARCHAR  PRIMARY KEY,
    title      VARCHAR  SORTKEY,
    artist_id  VARCHAR  NOT NULL,
    year       INTEGER  NOT NULL,
    duration   FLOAT8
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists ( 
    artist_id  VARCHAR   PRIMARY KEY,
    name       VARCHAR   SORTKEY,
    location   VARCHAR,
    latitude   DOUBLE PRECISION,
    longitude  DOUBLE PRECISION
    )diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dimtime ( 
    start_time TIMESTAMP   PRIMARY KEY,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    INTEGER
    )diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as json {};
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
region 'us-west-2'
json 'auto'
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES 

songplay_table_insert = ("""
INSERT INTO songplay (start_time,user_id,level,song_id,artist_id,sessionId,location,userAgent)
SELECT DISTINCT TIMESTAMP 'epoch'+(se.ts/1000)*INTERVAL '1 second',
        se.user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
FROM    staging_events se
INNER JOIN  staging_songs ss
ON (ss.title = se.song AND se.artists = ss.artist_name)
AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level)
SELECT DISTINCT se.user_id,
                se.firstName,
                se.lastName,
                se.gender,
                se.level
FROM staging_events se;
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
     SELECT ss.song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration
FROM staging_songs ss;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id,name,location,latitude,longitude)
     SELECT ss.artist_id,
            ss.artist_name,
            ss.artist_location,
            ss.artist_latitude,
            ss.artist_longitude
FROM staging_songs ss;
""")

time_table_insert = ("""
INSERT INTO dimtime (start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT start_time,
            EXTRACT (hour      FROM start_time),
            EXTRACT (day       FROM start_time),
            EXTRACT (week      FROM start_time),
            EXTRACT (month     FROM start_time),
            EXTRACT (year      FROM start_time),
            EXTRACT (dayofweek FROM start_time)
FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]