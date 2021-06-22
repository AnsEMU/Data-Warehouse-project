import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
IAM_ROLE = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
SONG_DATA = config.get('S3','SONG_DATA')



# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS factsongplays;"
user_table_drop = "DROP TABLE IF EXISTS dimusers;"
song_table_drop = "DROP TABLE IF EXISTS dimsongs;"
artist_table_drop = "DROP TABLE IF EXISTS dimartists;"
time_table_drop = "DROP TABLE IF EXISTS dimtime;"

# CREATE TABLES

staging_events_table_create= """ 
CREATE TABLE IF NOT EXISTS staging_events(
event_id int identity(0,1),
artist text,
auth text,
firstName text,
gender text,
itemInSession int,
lastName text,
length numeric,
level text,
location text,
method text,
page text,
registration numeric,
sessionId int,
song text,
status int,
ts bigint,
userAgent text,
userId int);
"""

staging_songs_table_create= (""" 
CREATE TABLE IF NOT EXISTS staging_songs(
num_songs int,
artist_id text,
artist_latitude numeric,
artist_longitude numeric,
artist_location text,
artist_name text,
song_id text,
title text,
duration float,
year int)
""")
# start_time timestamp not null,
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS factsongplays(
songplay_id int identity(0,1) PRIMARY KEY NOT NULL,
start_time timestamp NOT NULL,
userId varchar NOT NULL,
level varchar NOT NULL,
song_id varchar NOT NULL,
artist_id varchar NOT NULL,
sessionId int NOT NULL,
location varchar NOT NULL,
userAgent varchar NOT NULL)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dimusers (
userId int PRIMARY KEY NOT NULL,
firstName varchar NOT NULL,
lastName varchar NOT NULL,
gender varchar NOT NULL,
level varchar NOT NULL)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dimsongs (
song_id varchar PRIMARY KEY NOT NULL,
title varchar NOT NULL,
artist_id varchar NOT NULL,
year int NOT NULL,
uration float NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dimartists (
artist_id varchar PRIMARY KEY NOT NULL,
artist_name varchar NOT NULL,
artist_location varchar NOT NULL,
artist_latitude numeric NOT NULL,
artist_longitude numeric NOT NULL)diststyle all
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dimtime (
start_time timestamp PRIMARY KEY NOT NULL,
hour int NOT NULL,
day int NOT NULL,
week int NOT NULL,
month int NOT NULL,
year int NOT NULL,
weekday int NOT NULL)diststyle all
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from '{}'
credentials 'aws_iam_role={}'
json '{}' compupdate on region 'us-west-2'
EMPTYASNULL
BLANKSASNULL
""").format(LOG_DATA,IAM_ROLE,LOG_JSONPATH)

staging_songs_copy = ("""
copy staging_songs from '{}'
credentials 'aws_iam_role={}'
json '{}' compupdate on region 'us-west-2'
EMPTYASNULL
BLANKSASNULL
""").format(SONG_DATA,IAM_ROLE,LOG_JSONPATH)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO factsongplays (start_time, userId, level, song_id, artist_id, sessionId, location, userAgent)
SELECT SELECT DISTINCT timestamp 'epoch' + e.ts * interval '0.001 seconds' as start_time,
       e.userId,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionId,
       e.location,
       e.userAgent,
FROM staging_events AS e
join staging_songs AS s
ON (e.artist = s.artist_name)
     AND (e.song = s.title)
     AND (e.length = s.duration)
     WHERE e.page = 'NextSong' AND e.userId IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO dimusers (userId,firstName,lastName,gender,level)
select distinct userId,
       firstName,
       lastName,
       gender,
       level
        from  staging_events
        WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO dimsongs (song_id, title, artist_id, year, duration)
SELECT ts                   AS start_time,
       e.userId,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionId,
       e.location,
       e.userAgent,
FROM staging_songs AS s
join staging_events AS e
ON (e.artist = s.artist_name)
     AND (e.song = s.title)
     AND (e.length = s.duration)
     WHERE e.page = 'NextSong' AND e.userId IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO dimartists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
select artist_id,
       artist_name,
       artist_location,
       artist_latitude,
       artist_longitude,       
     from staging_songs
""")

time_table_insert = ("""
INSERT INTO dimtime (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
EXTRACT (HOUR FROM a.start_time) AS hour,
EXTRACT (DAY FROM start_time) AS day,
EXTRACT (WEEKS FROM start_time) AS week,
EXTRACT (MONTH FROM start_time) AS month,
EXTRACT (YEAR FROM start_time) AS year,
EXTRACT (WEEKDAY FROM start_time) AS weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
