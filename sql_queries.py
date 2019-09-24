import configparser


# CONFIG 
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "Drop table IF EXIST staging_events"
staging_songs_table_drop = "Drop table IF EXISTS staging_songs"
songplay_table_drop = "Drop table IF EXISTS songplay"
user_table_drop = "Drop table IF EXISTS user"
song_table_drop = "Drop table IF EXISTS song"
artist_table_drop = "Drop table IF EXISTS artist"
time_table_drop = "Drop table IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_songs (
 song_id VARCHAR(100),
    num_songs INTEGER,
    artist_id VARCHAR(100),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INTEGER,
    PRIMARY KEY (song_id))
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_events (
event_id INT IDENTITY(0,1),
artist_name VARCHAR(255),
auth VARCHAR(50),
user_first_name VARCHAR(255),
user_gender  VARCHAR(1),
item_in_session INTEGER,
user_last_name VARCHAR(255),
song_length	DOUBLE PRECISION, 
user_level VARCHAR(50),
location VARCHAR(255),
method VARCHAR(25),
page VARCHAR(35),
registration VARCHAR(50),
session_id BIGINT,
song_title VARCHAR(255),
status INTEGER,  
ts VARCHAR(50),
user_agent TEXT,
user_id VARCHAR(100),
PRIMARY KEY (event_id))
""")

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP REFERENCES time(start_time),
    user_id VARCHAR(50) REFERENCES users(user_id),
    level VARCHAR(50),
    song_id VARCHAR(100) REFERENCES songs(song_id),
    artist_id VARCHAR(100) REFERENCES artists(artist_id),
    session_id BIGINT,
    location VARCHAR(255),
    user_agent TEXT,
    PRIMARY KEY (songplay_id))
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(1),
    level VARCHAR(50),
    PRIMARY KEY (user_id))
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
    song_id VARCHAR(100),
    title VARCHAR(255),
    artist_id VARCHAR(100) NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id))
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(100),
    name VARCHAR(255),
    location VARCHAR(255),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id))
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time (
 start_time TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time))
""")

# STAGING TABLES

staging_events_copy = """
COPY staging_events
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON '{}';
""".format(CONFIG["S3"]["LOG_DATA"], CONFIG["IAM_ROLE"]["ARN"], CONFIG["S3"]["LOG_JSONPATH"])


staging_songs_copy = """
COPY staging_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""".format(CONFIG["S3"]["SONG_DATA"], CONFIG["IAM_ROLE"]["ARN"])

# FINAL TABLES


songplay_table_insert = """
insert into songplays (start_time,
                       user_id,
                       level,
                       song_id,
                       artist_id,
                       session_id,
                       location,
                       user_agent)
select staging_events.ts as start_time,
       staging_events.userid::INTEGER as user_id,
       staging_events.level,
       staging_songs.song_id,
       staging_songs.artist_id,
       staging_events.sessionid as session_id,
       staging_events.location,
       staging_events.useragent as user_agent
  from staging_events
  left join staging_songs
    on staging_events.song = staging_songs.title
   and staging_events.artist = staging_songs.artist_name
  left outer join songplays
    on staging_events.userid = songplays.user_id
   and staging_events.ts = songplays.start_time
 where staging_events.page = 'NextSong'
   and staging_events.userid is not Null
   and staging_events.level is not Null
   and staging_songs.song_id is not Null
   and staging_songs.artist_id is not Null
   and staging_events.sessionid is not Null
   and staging_events.location is not Null
   and staging_events.useragent is not Null
   and songplays.songplay_id is Null
 order by start_time, user_id
;
"""

user_table_insert = """
insert into users
select user_id::INTEGER,
       first_name,
       last_name,
       gender,
       level
  from (select userid as user_id,
               firstname as first_name,
               lastname as last_name,
               gender,
               level
          from staging_events
         where user_id is not Null) as temp
 group by user_id, first_name, last_name, gender, level
 order by user_id;
"""

song_table_insert = """
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
  FROM staging_songs;
"""

artist_table_insert = """
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
  FROM staging_songs;
"""

time_table_insert = """
insert into time
select start_time,
       date_part(hour, date_time) as hour,
       date_part(day, date_time) as day,
       date_part(week, date_time) as week,
       date_part(month, date_time) as month,
       date_part(year, date_time) as year,
       date_part(weekday, date_time) as weekday
  from (select ts as start_time,
               '1970-01-01'::date + ts/1000 * interval '1 second' as date_time
          from staging_events
         group by ts) as temp
 order by start_time;
"""


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
