import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from time import time
import psycopg2
import sql_queries as sq
import warnings

warnings.filterwarnings('ignore')
# read data from config file
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
KEY = config.get('AWS', 'key')
SECRET = config.get('AWS', 'secret')

DWH_DB = config.get("DWH", "DWH_DB")
DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
DWH_PORT = config.get("DWH", "DWH_PORT")

DWH_ENDPOINT = config.get("CLUSTER", "HOST")
DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")

# connect to database
conn_string = "postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
print(conn_string)
db_con = psycopg2.connect(conn_string, connect_timeout=5)
cursor = db_con.cursor()

# create schema
q1 = "CREATE SCHEMA IF NOT EXISTS my_schema"
cursor.execute(q1)

q2 = "SET search_path TO my_schema"
cursor.execute(q2)

# Let's see counts of data in each table
q_staging_events = "select count(*) from staging_events"
df_staging_events = pd.read_sql(sql=q_staging_events, con=db_con)
print(df_staging_events)

q_staging_songs = "select count(*) from staging_songs"
df_staging_songs = pd.read_sql(sql=q_staging_songs, con=db_con)
print(df_staging_songs)

q_songplay = "select count(*) from songplay"
df_songplay = pd.read_sql(sql=q_songplay, con=db_con)
print(df_songplay)

q_users = "select count(*) from users"
df_users = pd.read_sql(sql=q_users, con=db_con)
print(df_users)

q_songs = "select count(*) from songs"
df_songs = pd.read_sql(sql=q_songs, con=db_con)
print(df_songs)

q_artists = "select count(*) from artists"
df_artists = pd.read_sql(sql=q_artists, con=db_con)
print(df_artists)

q_time = "select count(*) from time"
df_time = pd.read_sql(sql=q_time, con=db_con)
print(df_time)

# The most played song
q_most_played_song = """select song, count(1)
from songplay
group by song
order by 2 desc
"""
df_most_played_song = pd.read_sql(sql=q_most_played_song, con=db_con)
print(df_most_played_song)

# the most played month
q_most_played_mont = """select t.year, t.month, count(1)
from songplay s
join time t on s.start_time = t.ts
group by t.year, t.month
order by 3 desc
"""
df_most_played_mont = pd.read_sql(sql=q_most_played_mont, con=db_con)
print(df_most_played_mont)

# close database connection
db_con.close()
