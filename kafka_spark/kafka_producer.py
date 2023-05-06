import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import numpy as np

KAFKA_TOPIC_NAME_CONS = "songTopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == '__main__':
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                        value_serializer=lambda x: dumps(x).encode('utf-8'))
    songs_df = pd.read_csv('./my_songs.csv')
    songs_df = songs_df[songs_df['popularity'] > 75]
    song_list = songs_df.to_dict(orient="records")
    
    message_list = []
    value_name = ['name', 'artists', 'popularity', 'duration_s', 'acousticness',
                  'danceability', 'energy', 'loudness', 'speechiness',  
                  'instrumentalness', 'liveness', 'valence' ,'tempo', 'time_signature']
                          
    message = None
    for song in song_list:
        message_list_value = []
        for val in value_name:
            message_list_value.append(song[val])

        message = ','.join(str(v) for v in message_list_value)
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)

    print("Kafka Producer Application Completed. ")





