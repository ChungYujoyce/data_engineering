from kafka import KafkaConsumer
from json import loads
import time
import pandas as pd

KAFKA_CONSUMER_GROUP_NAME_CONS = "test_consumer_group"
KAFKA_TOPIC_NAME_CONS = "songTopic"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputsongTopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Consumer Application Started ... ")

    consumer = KafkaConsumer(
                KAFKA_TOPIC_NAME_CONS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id=KAFKA_CONSUMER_GROUP_NAME_CONS,
                value_deserializer=lambda x: loads(x.decode('utf-8')))
    
    def get_message():
        print("Reading Messages from Kafka Topic... ")
        counter = 0
        message_list = []
        for message in consumer:
            message_list.append([message.value, [counter]])
            counter += 1
            if counter == 10:
                yield message_list
                counter = 0
                time.sleep(5)

    count = 0
    for message_list in get_message():
        print("begin streaming batch...")
        print(message_list[count:count+10])
        count += 10
        print("end streaming batch...")




    