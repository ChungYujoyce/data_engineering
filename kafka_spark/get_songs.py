import numpy as np
import pandas as pd
import ujson
import seaborn as sns
import ssl

import spotipy
import spotipy.util

from spotipy.oauth2 import SpotifyOAuth
import cred 

scope = 'user-library-read'

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=cred.client_id, client_secret= cred.client_secret, redirect_uri=cred.redirect_url, scope=scope))

saved_tracks_resp = sp.current_user_saved_tracks(limit=50)


def save_fields(track_response):
    return {
        'id': str(track_response['track']['id']),
        'name': str(track_response['track']['name']),
        'artists': [artist['name'] for artist in track_response['track']['artists']],
        'duration_ms': track_response['track']['duration_ms'],
        'popularity': track_response['track']['popularity'],
    }

tracks = [save_fields(track) for track in saved_tracks_resp['items']]
while saved_tracks_resp['next']:
    saved_tracks_resp = sp.next(saved_tracks_resp)
    tracks.extend([save_fields(track) for track in saved_tracks_resp['items']])

tracks_df = pd.DataFrame(tracks)

def transform(tracks_df):
    tracks_df['artists'] = tracks_df['artists'].apply(lambda artists: artists[0])
    tracks_df['duration_ms'] = tracks_df['duration_ms'].apply(lambda duration: duration/1000)
    tracks_df = tracks_df.rename(columns = {'duration_ms':'duration_s'})
    audio_features = {}
    col_name = {'acousticness', 'speechiness', 'key', 'liveness', 'danceability', 
                'instrumentalness', 'energy','tempo', 'time_signature', 'loudness', 'valence'}
    for idd in tracks_df['id'].tolist():
        audio_features[idd] = sp.audio_features(idd)[0]

    for col in col_name:
        tracks_df[col] = tracks_df['id'].apply(lambda idd: audio_features[idd][col])

    return tracks_df

tracks_df = transform(tracks_df)
print(tracks_df.head(n = 10))
tracks_df.to_csv('./my_songs.csv')


