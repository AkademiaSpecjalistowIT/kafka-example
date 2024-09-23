from confluent_kafka import Producer
import json
import time

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'my-topic'

# Kafka producer setup
producer_conf = {
    'bootstrap.servers': KAFKA_BROKER
}

producer = Producer(producer_conf)

# Callback for acknowledging receipt
def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Message produced: {msg.value().decode('utf-8')}")

def send_message(value):
    # Produce a message to the Kafka topic
    producer.produce(KAFKA_TOPIC, key=None, value=json.dumps(value), callback=acked)
    producer.poll(1)

if __name__ == '__main__':
    print("Sending messages to Kafka...")
    for i in range(1):
        message = {
            "id": i,
            "message": f"Hello Kafka {i}",
            "timestamp": time.time()
        }
        send_message(message)
        time.sleep(1)

    # Flush the messages to ensure delivery
    producer.flush()
