from confluent_kafka import Consumer
from google.cloud import bigquery
import ast
from json import loads

from google.oauth2 import service_account

KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "songTopic"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputsongTopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'


#Create BQ credentials object
credentials = service_account.Credentials.from_service_account_file('/Users/joycehuang/Downloads/kafkabq-386118-0a3d1cdf53b4.json')
bq_client = bigquery.Client(credentials=credentials)
# add your own info 'PROJECT-ID.DATASET.TABLE-NAME'
table_id = 'kafkabq-386118.mysongs123.songinfo'

c = Consumer({'bootstrap.servers':KAFKA_BOOTSTRAP_SERVERS_CONS,
              'group.id':KAFKA_CONSUMER_GROUP_NAME_CONS,
              'auto.offset.reset':'earliest'})

#Subscribe to topic
c.subscribe(['songTopic'])

def main():
    try:
        while True:
            msg = c.poll(timeout=1.0)  #Retrieve records one-by-one that have been efficiently pre-fetched by the consumer behind the scenes
            if msg is None:
                continue
            if msg.error():
                print('Error: {}'.format(msg.error()))
                continue
            else:
                data=msg.value().decode('utf-8')
                data = data[1:-1]
                keys = ['name', 'artists', 'popularity', 'duration_s', 'acousticness',
                        'danceability', 'energy', 'loudness', 'speechiness',  
                        'instrumentalness', 'liveness', 'valence' ,'tempo', 'time_signature']
                
                values = data.split(',')
                result = dict(zip(keys, values))
                
                ##### Stream data into BigQuery table #######
                rows_to_insert = [result]
                
                errors = bq_client.insert_rows_json(table_id,rows_to_insert) #Make API request

                if errors==[]:
                    print("New rows added.")
                else:
                    print("Encountered erros while inserting rows: {}".format(errors))
    finally:
        c.close() # Close down consumer to commit final offsets.

if __name__ == "__main__":
    main()