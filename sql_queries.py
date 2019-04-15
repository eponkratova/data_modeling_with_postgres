# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

user_table_create = ("""
CREATE TABLE users (
                user_id int,
                first_name varchar,
                last_name varchar,
                gender varchar,
                level varchar                
        )
""")

song_table_create = ("""
CREATE TABLE songs (
                song_id varchar,
                title varchar,
                artist_id varchar,
                year int,
                duration float                   
        )
""")

artist_table_create = ("""
CREATE TABLE artists (
                artist_id varchar,
                name varchar,
                location varchar,
                lattitude float,
                longitude float 
        )
""")

time_table_create = ("""
CREATE TABLE time (
                start_time varchar,
                hour int,
                day int,
                week int,
                month int,
                year int,
                weekday int                
        )
""")



songplay_table_create = ("""
CREATE TABLE songplays (
                songplay_id varchar,
                start_time varchar,
                user_id int,
                level varchar,
                song_id varchar, 
                artist_id varchar,
                session_id int,
                location varchar,
                user_agent varchar              
        )
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (
                songplay_id,
                start_time,
                user_id,
                level,
                song_id, 
                artist_id,
                session_id,
                location,
                user_agent)
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s) ;
""")

user_table_insert = ("""
INSERT INTO users (
                user_id,
                first_name,
                last_name,
                gender,
                level)
                VALUES(%s, %s, %s, %s, %s);
""")

song_table_insert = ("""
INSERT INTO songs (
                song_id,
                title,
                artist_id,
                year,
                duration)
                VALUES(%s, %s, %s, %s, %s) 
""")

artist_table_insert = ("""
INSERT INTO artists (
                artist_id,
                name,
                location,
                lattitude,
                longitude)
                VALUES(%s, %s, %s, %s, %s)
""")


time_table_insert = ("""
INSERT INTO time (
                start_time,
                hour,
                day,
                week,
                month,
                year,
                weekday)
                VALUES(%s, %s, %s, %s, %s, %s, %s) ;
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id FROM songs s
LEFT JOIN artists a 
ON a.artist_id = s.artist_id
WHERE title = (%s) AND name = (%s) AND duration=(%s)
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]