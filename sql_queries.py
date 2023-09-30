import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE staging_events (
    artist VARCHAR(256),
    auth VARCHAR(256),
    firstName VARCHAR(256),
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR(256),
    length FLOAT,
    level VARCHAR(256),
    location VARCHAR(256),
    method VARCHAR(256),
    page VARCHAR(256),
    registration BIGINT,
    sessionId INTEGER,
    song VARCHAR(256),
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR(256),
    userId INTEGER
);
"""

staging_songs_table_create = """
CREATE TABLE staging_songs (
  num_songs INTEGER,
  artist_id VARCHAR(40),
  artist_latitude FLOAT,
  artist_longitude FLOAT,
  artist_location VARCHAR(200),
  artist_name VARCHAR(200),
  song_id VARCHAR(40),
  title VARCHAR(200),
  duration FLOAT,
  year INTEGER
)
"""

songplay_table_create = ("""
CREATE TABLE songplay (
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
start_time BIGINT sortkey,
user_id INTEGER,
level VARCHAR(256),
song_id VARCHAR(40),
artist_id VARCHAR(40) distkey,
session_id INTEGER,
location VARCHAR(256),
user_agent VARCHAR(256)
)
""")

user_table_create = ("""
CREATE TABLE users (
user_id INTEGER PRIMARY KEY sortkey,
first_name VARCHAR(256),
last_name VARCHAR(256),
gender CHAR(1),
level VARCHAR(256)
)
diststyle all
""")

song_table_create = ("""
CREATE TABLE songs (
song_id VARCHAR(40) PRIMARY KEY,
title VARCHAR(200),
artist_id VARCHAR(40) distkey,
year INTEGER,
duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id VARCHAR(40) PRIMARY KEY sortkey,
name VARCHAR(200),
location VARCHAR(200),
lattitude FLOAT,
longitude FLOAT
)
diststyle all
""")

time_table_create = ("""
CREATE TABLE time (
ts BIGINT PRIMARY KEY sortkey,
start_time TIMESTAMP,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weekday INTEGER
)
diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE '{}'
REGION 'us-west-2'
JSON {};
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs
FROM {}
credentials 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
insert into songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct ts, userid, level, ss.song_id, ss.artist_id, sessionid, location, useragent
from staging_events se
left join staging_songs ss on se.artist = artist_name and se.song = ss.title
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct userid, firstName, lastName, gender, level
from staging_events
where userid is not null
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
select distinct song_id, title, artist_id, year, duration
from staging_songs
where song_id is not null
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, lattitude, longitude)
select distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
from staging_songs
where artist_id is not null
""")

time_table_insert = ("""
insert into time (ts, start_time, hour, day, week, month, year, weekday)
select distinct ts,
timestamp 'epoch' + ts / 1000 * interval '1 second' as start_time,
EXTRACT (HOUR FROM (timestamp 'epoch' + ts / 1000 * interval '1 second')) hour_,
EXTRACT (DAY FROM (timestamp 'epoch' + ts / 1000 * interval '1 second')) day_,
EXTRACT (WEEK FROM (timestamp 'epoch' + ts / 1000 * interval '1 second')) week_,
EXTRACT (MONTH FROM (timestamp 'epoch' + ts / 1000 * interval '1 second')) month_,
EXTRACT (YEAR FROM (timestamp 'epoch' + ts / 1000 * interval '1 second')) year_,
EXTRACT (DOW FROM (timestamp 'epoch' + ts / 1000 * interval '1 second')) weekday_
from staging_events
where ts is not null
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
