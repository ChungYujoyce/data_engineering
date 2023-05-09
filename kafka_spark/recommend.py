from tqdm import tqdm
import numpy as np, pandas as pd

class spotify_recommender():
    def __init__(self, rec_data):
        self.rec_data = rec_data
    
    def spotify_recommendations(self, song_name, amount = 1):
        distances = []
        print("recccc")
        print(self.rec_data)
        print(song_name)
        song = self.rec_data[(self.rec_data.name.str.lower() == song_name.lower())].head(1).values[0]
        res_data = self.rec_data[self.rec_data.name.str.lower() != song_name.lower()]

        for r_song in tqdm(res_data.values):
            dist = 0
            for col in np.arange(len(res_data.columns)):
                #indeces of non-numerical columns neednt be considered.
                #calculating the manhettan distances for each numerical feature
                # song: from our fav songs, r_song: from streaming data
                if not col in [0, 1]:
                    dist = dist + np.absolute(float(song[col]) - float(r_song[col]))
            distances.append(dist)
        
        res_data['distance'] = distances
        res_data = res_data.sort_values('distance')
        columns = ['name', 'artists', 'acousticness', 'liveness', 'instrumentalness', 'energy', 'danceability', 'valence']
        
        return res_data[columns][:amount]

