# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        PRIMARY KEY(songplay_id),\
        songplay_id SERIAL,\
        start_time TIMESTAMP,\
        user_id INT,\
        level VARCHAR,\
        song_id VARCHAR,\
        artist_id VARCHAR,\
        session_id INT,\
        location TEXT,\
        user_agent VARCHAR);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        PRIMARY KEY(user_id),\
        user_id BIGINT,\
        first_name VARCHAR,\
        last_name VARCHAR,\
        gender CHAR(1),\
        level VARCHAR NOT NULL);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        PRIMARY KEY(song_id),\
        song_id VARCHAR,\
        title VARCHAR NOT NULL,\
        artist_id VARCHAR NOT NULL, \
        year INT, \
        duration FLOAT);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        PRIMARY KEY(artist_id),\
        artist_id VARCHAR,\
        artist_name VARCHAR NOT NULL,\
        artist_location TEXT,\
        artist_latitude FLOAT,\
        artist_longitude FLOAT);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        PRIMARY KEY(start_time),\
        start_time TIMESTAMP,\
        hour INT NOT NULL,\
        day INT NOT NULL,\
        week INT NOT NULL,\
        month INT NOT NULL,\
        year INT NOT NULL,\
        weekday INT NOT NULL);
""")

# INSERT RECORDS  
songplay_table_insert = ("""
    INSERT INTO songplays(
        start_time,\
        user_id,\
        level,\
        song_id,\
        artist_id,\
        session_id,\
        location,\
        user_agent)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(songplay_id)
    DO UPDATE SET start_time=EXCLUDED.start_time, user_id=EXCLUDED.user_id,\
        level=EXCLUDED.level, song_id=EXCLUDED.song_id, artist_id=EXCLUDED.artist_id,\
        session_id=EXCLUDED.session_id, location=EXCLUDED.location, user_agent=EXCLUDED.user_agent
""")

user_table_insert = ("""
    INSERT INTO users(
        user_id,\
        first_name,\
        last_name,\
        gender,\
        level)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT(user_id)
    DO UPDATE SET first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, gender=EXCLUDED.gender, level=EXCLUDED.level
""")

song_table_insert = ("""
    INSERT INTO songs(
        song_id,\
        title,\
        artist_id,\
        year,\
        duration)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT(song_id)
    DO UPDATE SET title=EXCLUDED.title, artist_id=EXCLUDED.artist_id, year=EXCLUDED.year, duration=EXCLUDED.duration
""")

artist_table_insert = ("""
    INSERT INTO artists(
        artist_id,\
        artist_name,\
        artist_location,\
        artist_latitude,\
        artist_longitude)
    VALUES(%s, %s, %s, %s, %s)
    ON CONFLICT(artist_id)
    DO UPDATE SET artist_name=EXCLUDED.artist_name, artist_location=EXCLUDED.artist_location, artist_latitude=EXCLUDED.artist_latitude, artist_longitude=EXCLUDED.artist_longitude
""")


time_table_insert = ("""
    INSERT INTO time(
        start_time,\
        hour,\
        day,\
        week,\
        month,\
        year,\
        weekday)
    VALUES(%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(start_time)
    DO UPDATE SET hour=EXCLUDED.hour, day=EXCLUDED.day, week=EXCLUDED.week, month=EXCLUDED.month, year=EXCLUDED.year, weekday=EXCLUDED.weekday
""")

# FIND SONGS

song_select = ("""
    SELECT s.song_id, a.artist_id 
    FROM songs s
    JOIN artists a 
    ON s.artist_id = a.artist_id 
    WHERE s.title = %s
    AND a.artist_name = %s
    AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]