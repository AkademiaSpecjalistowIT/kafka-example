from kafka import KafkaConsumer
import json
from datetime import datetime

def consume_data(consumer):
    for message in consumer:
        data = json.loads(message.value)

        # Extract the producer timestamp
        producer_timestamp = data.get("timestamp", None)

        # Get the current time (when the message was consumed)
        consumer_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # Calculate the difference between producer and consumer timestamps
        producer_time_obj = datetime.strptime(producer_timestamp, '%Y-%m-%d %H:%M:%S.%f')
        consumer_time_obj = datetime.strptime(consumer_timestamp, '%Y-%m-%d %H:%M:%S.%f')

        time_difference = consumer_time_obj - producer_time_obj

        print(f"Consumed message: {data['message']}")
        print(f"Produced at: {producer_timestamp}, Consumed at: {consumer_timestamp}")
        print(f"Time taken from producer to consumer: {time_difference}")

if __name__ == '__main__':
    consumer = KafkaConsumer(
        'remote-topic',
        bootstrap_servers='3.71.3.69:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: x.decode('utf-8'),
    )
    consume_data(consumer)
