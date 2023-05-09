from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans
import pandas as pd
import random

def feature_engineer(df):
    df = df.drop('duration_s')

    assembler = VectorAssembler(inputCols=[
        'danceability', 'energy', 'loudness',
        'speechiness', 'acousticness', 'instrumentalness',
        'liveness', 'valence', 'tempo'], outputCol='features')
    
    assembled_data = assembler.setHandleInvalid("skip").transform(df)
    scale = StandardScaler(inputCol='features', outputCol='standardized')
    data_scale = scale.fit(assembled_data)
    df = data_scale.transform(assembled_data)
    # set the amount of clusters
    KMeans_algo = KMeans(featuresCol='standardized', k=5)
    KMeans_fit=KMeans_algo.fit(df)
    
    output_df =KMeans_fit.transform(df)

    datad = output_df.select('name', 'artists', 'danceability',
        'energy', 'loudness', 'speechiness', 'acousticness',
        'instrumentalness', 'liveness', 'valence', 'tempo', 'prediction')

    datf = datad.toPandas()

    song_data = pd.read_csv("./my_songs.csv", sep=",", header=0, index_col=None)
    song_data = song_data[song_data['popularity'] > 85]
    song_data = song_data.drop(['key', 'time_signature', 'duration_s'], axis='columns')
    rand_n = random.randint(0,len(song_data)-1)
    add_df = song_data.head(rand_n)[-1:]

    return datf, add_df


    