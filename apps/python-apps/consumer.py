from kafka import KafkaConsumer
import json


def consume_data(consumer):
    for message in consumer:
        data = message.value
        print(f"Consumed data from topic:", data)


if __name__ == '__main__':
    consumer = KafkaConsumer(
        'my-topic-3-many-partitions',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8'),
        group_id='my-group3'
    )
    consume_data(consumer)


