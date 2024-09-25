from kafka import KafkaProducer
import json
from datetime import datetime
import time

# Kafka configuration
KAFKA_BROKER = '3.71.3.69:9092'
KAFKA_TOPIC = 'remote-topic'

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Callback for acknowledging receipt
def on_send_success(record_metadata):
    print(f"Message produced to {record_metadata.topic} partition [{record_metadata.partition}] at offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Failed to deliver message: {excp}")

def send_message(value):
    # Produce a message to the Kafka topic
    producer.send(KAFKA_TOPIC, value=value).add_callback(on_send_success).add_errback(on_send_error)

if __name__ == '__main__':
    print("Sending messages to Kafka...")
    for i in range(10):
        # Get the current time in a human-readable format (day, second, millisecond)
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        message = {
            "id": i,
            "message": f"Hello Kafka {i}",
            "timestamp": current_time  # Add timestamp as a human-readable string
        }
        send_message(message)
        time.sleep(5)

    # Flush the messages to ensure delivery
    producer.flush()
