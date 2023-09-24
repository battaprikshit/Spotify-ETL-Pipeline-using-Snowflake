create Table album_data (album_id String, album_name string, release_date string,album_total_track integer,album_url string );


create table artist_data (artist_id string, artist_name string, artist_url string );

create table song_data (song_id string, song_name string, song_duration integer, song_url string,song_popularity integer,song_added date,album_id string,artist_id string );


ALTER TABLE album_data 
ADD PRIMARY KEY (album_id);

ALTER TABLE artist_data 
ADD PRIMARY KEY (artist_id);

ALTER TABLE song_data add primary key (song_id);

CREATE STAGE album_stage
URL = "s3://spotify-etl-project-prikshit/transformed_data/album_data/"
CREDENTIALS = (AWS_KEY_ID='' AWS_SECRET_KEY= '');

CREATE STAGE artist_stage
URL = "s3://spotify-etl-project-prikshit/transformed_data/artist_data/"
CREDENTIALS = (AWS_KEY_ID='' AWS_SECRET_KEY= '');

CREATE STAGE songs_stage
URL = "s3://spotify-etl-project-prikshit/transformed_data/songs_data/"
CREDENTIALS = (AWS_KEY_ID='' AWS_SECRET_KEY= '');

CREATE OR REPLACE FILE FORMAT csv_file_format
TYPE ='CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

COPY INTO album_data (ALBUM_ID, ALBUM_NAME,RELEASE_DATE,ALBUM_TOTAL_TRACK,ALBUM_URL) 
from@album_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');


COPY INTO artist_data (artist_ID, artist_NAME,artist_URL) 
from@artist_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');




COPY INTO song_data (SONG_ID,SONG_NAME,SONG_DURATION,SONG_URL,SONG_ADDED,ALBUM_ID,ARTIST_ID) 
from@songs_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');

select count(*) from album_data ;


create or replace pipe album_pipe auto_ingest =TRUE 
as
COPY INTO album_data 
from@album_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');

create or replace pipe artist_pipe auto_ingest =TRUE 
as
COPY INTO artist_data 
from@artist_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');

create or replace pipe song_pipe auto_ingest =TRUE 
as
COPY INTO song_data 
from@songs_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');




desc pipe album_pipe;
SHOW PIPES;
SHOW PIPELINE album_pipe;


