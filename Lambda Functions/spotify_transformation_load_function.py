import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO


def album(data):
    album_list=[]
    for row in data["items"] :
      album_id= row["track"]["album"]["id"]
      album_name = row["track"]["album"]["name"]
      album_release_date = row["track"]["album"]["release_date"]
      album_total_tracks = row["track"]["album"]["total_tracks"]
      album_url = row["track"]["album"]["external_urls"]["spotify"]
      album_element ={"album_id": album_id, "album_name":album_name,"release_date":album_release_date,"album_total_tracks":album_total_tracks,"album_url":album_url}
      album_list.append(album_element)
    return album_list
    
def artist(data):
    artist_list = []
    for row in data["items"] :
      artist_id = row["track"]["album"]["artists"][0]["id"]
      artist_name = row["track"]["album"]["artists"][0]["name"]
      external_url = row["track"]["album"]["artists"][0]["href"]
      artist_elements = {"artist_id":artist_id,"artist_name" :artist_name ,"external_url":external_url}
      artist_list.append(artist_elements)
    return artist_list
    
def songs(data):
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
    return song_list
    
    
    

def lambda_handler(event, context):
    # TODO implement
    s3 =boto3.client("s3")
    Bucket= "spotify-etl-project-prikshit"
    Key = "raw_data/to_processed/"
    spotify_data=[]
    spotify_keys=[]
  
    # x = s3.list_objects(Bucket=Bucket,Prefix=Key)["Contents"][0]
    
    # print(x["Key"])
    for file in (s3.list_objects(Bucket=Bucket,Prefix=Key)["Contents"][0]):
        file_key = file["Key"]
        if file_key.split(".")[-1] == "json":
            response=s3.get_object(Bucket= Bucket, Key = file_key)
            content = response["Body"]
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
            # print(spotify_data)
            
    for data in spotify_data:
        album_list = album(data)
        artist_list = artist(data)
        song_list = songs(data)
        
        album_df = pd.DataFrame.from_dict(album_list)
        album_df.drop_duplicates(subset=['album_id'])
        album_df["release_date"] = pd.to_datetime(album_df["release_date"])
        
        artist_df =pd.DataFrame.from_dict(artist_list)
        artist_df.drop_duplicates(subset=['artist_id'])
        
        song_df = pd.DataFrame.from_dict(song_list)
        song_df.drop_duplicates(subset=['song_id'])
        song_df["song_added"] =pd.to_datetime(song_df["song_added"])
        
        song_key = "transformed_data/songs_data/song_transformed_" + str(datetime.now()) + ".csv"
        song_buffer=StringIO()
        song_df.to_csv(song_buffer,index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key= song_key, Body = song_content)
        
        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now()) + ".csv"
        album_buffer=StringIO()
        album_df.to_csv(album_buffer,index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key= album_key, Body = album_content)
        
        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now()) + ".csv"
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer,index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key= artist_key, Body = artist_content)
    
    
    s3_resource = boto3.resource("s3")
    for key in spotify_keys:
        copy_source= {"Bucket": Bucket, "Key":key}
        s3_resource.meta.client.copy(copy_source,Bucket,"raw_data/processed/" +key.split("/")[-1])
        s3_resource.Object(Bucket,key).delete()
        # print(album_list)
    # print(s3.list_objects(Bucket=Bucket,Prefix=Key)["Contents"][0])