
# Spotify ETL Pipeline using Snowflake

In this project, I've successfully implemented an ETL pipeline for Spotify data, leveraging the power of Snowflake and AWS services. The process begins with Python, which interfaces with the Spotify API to extract the raw data. This data is then efficiently transferred to an AWS S3 bucket through the utilization of Lambda functions. Following this, another Lambda function takes charge, applying the necessary transformations to the data, subsequently storing the transformed dataset back into an S3 bucket.

Now, we're poised to advance to the next pivotal phase - data loading into our robust data warehouse via Snowflake. To streamline this process, we'll employ Snowpipe, a powerful tool that facilitates the seamless flow of data into Snowflake, enhancing efficiency and real-time availability.

Overall, this project embodies a well-orchestrated symphony of technologies, culminating in a dynamic and efficient ETL pipeline.
![alt text](https://github.com/battaprikshit/Spotify-ETL-Pipeline-using-Snowflake/blob/main/spotify_screenshots/Architecture_ETL.jpg)

## Prerequisites
Before you begin, ensure you have met the following requirements:
- You have a Windows/Linux/Mac machine.
- You have Amazon Web Services Account.
- You have Snowflake Account.
- You have Spotify Developer Account
## Business Requirement

The core business requirement for this project is to assemble a comprehensive and diverse compilation of global songs reflecting a wide range of genres, languages, and cultural influences. This entails establishing a seamless integration with the Spotify API through Python, enabling direct access to an extensive music library. The extracted raw music data is to be efficiently stored in a secure AWS S3 bucket, ensuring scalability and reliability. Additionally, the system should accommodate a potentially high volume of requests to meet the client's ambitious vision.
## Steps for the ETL Pipeline
1. Go to Spotify.com and register for a developer account so that you can access the spotify api.
2. Go to jupyter/collab notebook and install the spotify from pip module of python.
```python
!pip install spotipy
import spotipy
import pandas as pd
from spotipy.oauth2 import SpotifyClientCredentials
```
3. Next step is to provide the client id and client secret id of the spotify api so that api can be hit.

```python
client_credentials_manager = SpotifyClientCredentials(client_id="",client_secret="")

from spotipy.client import Spotify
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"

playlist_uri = playlist_link.split("/")[-1]
data = sp.playlist_tracks(playlist_uri)
```
4. Next step is we will get the details of Album, Artists and Songs 

```pyhton
album_list=[]
for row in data["items"] :
  album_id= row["track"]["album"]["id"]
  album_name = row["track"]["album"]["name"]
  album_release_date = row["track"]["album"]["release_date"]
  album_total_tracks = row["track"]["album"]["total_tracks"]
  album_url = row["track"]["album"]["external_urls"]["spotify"]
  album_element ={"album_id": album_id, "album_name":album_name,"release_date":album_release_date,"album_total_tracks":album_total_tracks,"album_url":album_url}
  album_list.append(album_element)

artist_list = []
for row in data["items"] :
  artist_id = row["track"]["album"]["artists"][0]["id"]
  artist_name = row["track"]["album"]["artists"][0]["name"]
  external_url = row["track"]["album"]["artists"][0]["href"]
  artist_elements = {"artist_id":artist_id,"artist_name" :artist_name ,"external_url":external_url}
  artist_list.append(artist_elements)


song_list=[]
for row in data["items"] :
  song_id = row["track"]["id"]
  song_name = row["track"]["name"]
  song_duration = row["track"]["duration_ms"]
  song_url = row["track"]["external_urls"]["spotify"]
  song_popularity = row["track"]["popularity"]
  song_added = row["added_at"]
  album_id= row["track"]["album"]["id"]
  artist_id = row["track"]["album"]["artists"][0]["id"]
  song_element = {"song_id":song_id,"song_name":song_name,"song_duration":song_duration,"song_url":song_url,"song_popularity":song_popularity,"song_added":song_added,"album_id":album_id,
                  "artist_id":artist_id}
  song_list.append(song_element)
```
5. Next step is convert the data to pandas dataframe and do the data cleaning.

```python
album_df = pd.DataFrame.from_dict(album_list)
album_df.drop_duplicates(subset=['album_id'])

artist_df =pd.DataFrame.from_dict(artist_list)
artist_df.drop_duplicates(subset=['artist_id'])

song_df = pd.DataFrame.from_dict(song_list)
song_df.drop_duplicates(subset=['song_id'])

# converting the release_date and song_added to date time format

album_df["release_date"] = pd.to_datetime(album_df["release_date"])

song_df["song_added"] =pd.to_datetime(song_df["song_added"])
```

## AWS Component (Extract and Transformation)

Now we have extracted the data using spotify api now we want to host this on AWS.

1. Login to your AWS Account
2. Create the billing alarm so that you receive the billing alerts.
3. Create the S3 bucket for the ETL Pipline.My bucket is as follows.
 ---------------image s3 bucket-----

4. Create two folders, one for raw data and another for transformed data. 
5. Create another subforders processed and to_processed in raw data. Processed includes the data after processing and to_processed will have the raw data that direcly comes from spotify api.
--------image

6. Create another three subfolders in transformed_data which will have the album, artists and songs data that is transformed.
----- image

7. Create the Lambda function to axtract the data using spotify api.Put the client id and client secret id in the environverment variable.This lambda function will extarct the raw data and store into raw data folder to_processed.Deploy and run the function. 

The code for api data extarct function is provided in seperate foder.

8. Create a new role and attach to the lambda function.The role should have permissions for s3 full access.

9. Now we have the raw data in s3 bucket and our next step is to do the transformation.
10. Create another lambda function so that transformation can be performed. The code for another lambda function is attached in the repository.

11. Now the data should be transformed and put to their respective folder ie Album,Song and Artist.

12. Next step is to copy the file which we have transformed and then put that file to the folder processed because the raw file that has been processed so that we do not process the same data again.Then delete the file which is present in to_processed folder.

13. Now we have the transformed data.

14. Next step is to apply the triggers
15. Attach the Amazon Cloudwatch daily trigger to the first lambda function which extract data from api so that it can run daily or as per the business needs.

16. Next add the trigger to the lambda function which do the transformation. I have added All Object created event to the Lambda function so that if any object is created in the to_processed bucket then it triggers the lambda fucntion and data is transformed.

17. Now the new files are getting aded to the transformed_data folder.

18. Now our next step is to load this data to Snowflake.






## Snowflake Component (Data Loading)
Till now we have extarcted and transformed the data. Out next step is to load that data to datawarehouse.

1. Create 3 tables in Snowflake Database - album_data,artist_data,song_data
```sql
create Table album_data (album_id String, album_name string, release_date string,album_total_track integer,album_url string );


create table artist_data (artist_id string, artist_name string, artist_url string );

create table song_data (song_id string, song_name string, song_duration integer, song_url string,song_popularity integer,song_added date,album_id string,artist_id string );


ALTER TABLE album_data 
ADD PRIMARY KEY (album_id);

ALTER TABLE artist_data 
ADD PRIMARY KEY (artist_id);

ALTER TABLE song_data add primary key (song_id);
```
2. Create a stage to connect the AWS account to Snowflake
```sql
CREATE STAGE album_stage
URL = "s3://spotify-etl-project-prikshit/transformed_data/album_data/"
CREDENTIALS = (AWS_KEY_ID="" AWS_SECRET_KEY= "");

CREATE STAGE artist_stage
URL = "s3://spotify-etl-project-prikshit/transformed_data/artist_data/"
CREDENTIALS = (AWS_KEY_ID=  AWS_SECRET_KEY= "");

CREATE STAGE songs_stage
URL = "s3://spotify-etl-project-prikshit/transformed_data/songs_data/"
CREDENTIALS = (AWS_KEY_ID='' AWS_SECRET_KEY= "");

```

3. Now next step is to create the file format so that the data is correctly read.

```sql
CREATE OR REPLACE FILE FORMAT csv_file_format
TYPE ='CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;
```
4. Test the copy command to load the data to the tables
```sql
COPY INTO album_data (ALBUM_ID, ALBUM_NAME,RELEASE_DATE,ALBUM_TOTAL_TRACK,ALBUM_URL) 
from@album_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');

COPY INTO artist_data (artist_ID, artist_NAME,artist_URL) 
from@artist_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');


COPY INTO song_data (SONG_ID,SONG_NAME,SONG_DURATION,SONG_URL,SONG_ADDED,ALBUM_ID,ARTIST_ID) 
from@songs_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');
```
5. Now we have to create a Snowpipe so that when the new data is added to the transformed folder then the data is automatically loaded to the Snowflake. For that we ahve to create 3 seperate Snowpipes.

```sql
create or replace pipe album_pipe auto_ingest =TRUE as
COPY INTO from@album_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');

create or replace pipe artist_pipe auto_ingest =TRUE 
as
COPY INTO artist_data from@artist_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');

create or replace pipe song_pipe auto_ingest =TRUE 
as
COPY INTO song_data from@songs_stage/
FILE_FORMAT = (FORMAT_NAME= 'csv_file_format');
```

6. Now we have to create event notification. go to S3 bucket and go to events.

7. Event type as All objects created so that snowpipe can be triggered.

8. Click on SQS queue and enter the SQS queue ARN. Copy the Notification channel from snowpipe and paste it to SQS queue ARN while creating the event notification.

9. Create this event notification for all 3 snowpipe.

10. Test the pipeline.


## Summary

With the successful completion of the final ETL process for Spotify data, we have now seamlessly integrated and stored the refined dataset within Snowflake, a robust and scalable data warehousing solution. This accomplishment opens up a multitude of opportunities for leveraging the data's insights. Data scientists and analysts can now harness this enriched dataset to develop predictive models, gaining invaluable insights into musical trends and user preferences. Additionally, the data can be harnessed through powerful visualization tools like Power BI, empowering stakeholders to create dynamic and interactive dashboards that offer a comprehensive view of the music landscape. This achievement marks a significant milestone, paving the way for a deeper understanding and strategic utilization of the vast musical repository we've meticulously curated.
## Contributing
Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are greatly appreciated. If you have a suggestion that would make this better, please fork the repo and create a pull request. Don't forget to give the project a star! Thanks again!

1. Fork this repository.
2. Create a branch: `git checkout -b <branch_name>`.
3. Make your changes and commit them: `git commit -m '<commit_message>'`
4. Push to the original branch: `git push origin <project_name>/<location>`
5. Create the pull request.

Alternatively see the GitHub documentation on [creating a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request). 
## License
Distributed under the MIT License. See LICENSE.txt for more information.
