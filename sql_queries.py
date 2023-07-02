import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR(18) DISTKEY,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(256),
    artist_name VARCHAR(256),
    song_id VARCHAR(18),
    title VARCHAR(256),
    duration FLOAT,
    year INTEGER
)
COMPOUND SORTKEY(artist_id, song_id);
""")

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender VARCHAR,
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT DISTKEY,
    userAgent VARCHAR,
    userId INTEGER
)
COMPOUND SORTKEY(ts, userId);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL,
    session_id INTEGER,
    location VARCHAR,
    user_agent TEXT
)
DISTKEY (user_id)
SORTKEY (start_time);
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY DISTKEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR(5)
)
SORTKEY(user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL DISTKEY,
    year INTEGER,
    duration FLOAT
)
SORTKEY(song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY DISTKEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
)
SORTKEY(artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY DISTKEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
)
SORTKEY(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM '{}'
IAM_ROLE '{}'
JSON '{}';
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
IAM_ROLE '{}'
JSON 'auto';
""").format(config.get('S3', 'SONG_DATA'), 
            config.get('IAM_ROLE', 'ARN'))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    to_timestamp(to_char(e.ts,'9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS') AS start_time,
    e.userId AS user_id,
    NULLIF(e.level, '') AS level,
    s.song_id,
    s.artist_id,
    e.sessionId AS session_id,
    NULLIF(e.location, '') AS location,
    NULLIF(e.userAgent, '') AS user_agent
FROM staging_events e
JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
WHERE e.page = 'NextSong';

""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId AS user_id,
    NULLIF(firstName, '') AS first_name,
    NULLIF(lastName, '') AS last_name,
    NULLIF(gender, '') AS gender,
    NULLIF(level, '') AS level
FROM staging_events
WHERE page = 'NextSong' AND userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
    NULLIF(title, '') AS title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
    NULLIF(artist_name, '') AS name,
    NULLIF(artist_location, '') AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT 
    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS week,
    EXTRACT(month FROM start_time) AS month,
    EXTRACT(year FROM start_time) AS year,
    EXTRACT(weekday FROM start_time) AS weekday
FROM staging_events
WHERE page = 'NextSong';
""")
                     
# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]