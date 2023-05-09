from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Normalizer, StandardScaler
from pyspark.sql.types import *
import os
import pandas as pd
from feature_eng import feature_engineer
from recommend import spotify_recommender
# export SPARK_LOCAL_IP="127.0.0.1"
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'

KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "songTopic"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputsongTopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

spark = SparkSession.builder \
        .appName("Spotify Streaming Recommendation System") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# # read from the topic
songs_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load() \
        .selectExpr("CAST(value AS STRING)")
        


songs_schema_string = "name STRING, artists STRING, popularity INT, duration_s DOUBLE," \
                           + "acousticness DOUBLE, danceability DOUBLE," \
                           + "energy DOUBLE, loudness DOUBLE, speechiness DOUBLE," \
                           + "instrumentalness DOUBLE, liveness DOUBLE, " \
                           + "valence DOUBLE, tempo DOUBLE, time_signature DOUBLE"

# parse the Avro-encoded payload
songs_df2 = songs_df \
                .select(from_json(col("value"), songs_schema_string).alias("song"))

songs_df3 = songs_df2.select("song.*")
songs_df3.createOrReplaceTempView("song_find")

# # write data to memory for spark sql
song_find_text = spark.sql("SELECT * FROM song_find")

# songs_df1.createOrReplaceTempView("song_find")
# song_find_text = spark.sql("SELECT * FROM song_find")
song_write_stream = song_find_text \
                        .writeStream \
                        .trigger(processingTime = '5 seconds') \
                        .outputMode("append") \
                        .option("truncate", "false") \
                        .format("memory") \
                        .queryName("testedTable") \
                        .start()  \
                        .awaitTermination(30)

df = spark.sql("SELECT * FROM testedTable")
df_tmp = df

datf, add_df = feature_engineer(df)
df_sp = spark.createDataFrame(add_df)
df = df.union(df_sp)

recommender = spotify_recommender(datf)

x = add_df['name'].tolist()[0]

rec_song = recommender.spotify_recommendations(x, 10)

v = add_df[['name', 'artists',  'acousticness', 'liveness', 'instrumentalness', 'energy', 
       'danceability', 'valence']]

rec_song = pd.concat([rec_song, v])
rec_song.to_csv('./rec_song.csv')

df_rec = spark.createDataFrame(rec_song)
df_rec.show()
