import json
import os
import spotipy
import boto3
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime

def lambda_handler(event, context):
    # TODO implement
    client_id = os.environ.get("client_id")
    client_secret = os.environ.get("client_secret")
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    # playlists=sp.user.playlists("spotify")
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF"
    playlist_uri = playlist_link.split("/")[-1]
    data = sp.playlist_tracks(playlist_uri)
    print(data)
    filename = "spotify_raw" + str(datetime.now()) + ".json"
    client =boto3.client("s3")
    client.put_object( Bucket= "spotify-etl-project-prikshit",
                        Key= "raw_data/to_processed/"+filename,
                        Body=json.dumps(data))