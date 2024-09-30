from kafka import KafkaProducer
from fastavro.schema import load_schema
from fastavro import schemaless_writer
import io
import requests
from datetime import datetime
import time

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'orders'
SCHEMA_REGISTRY_URL = 'http://localhost:8081/apis/registry/v2/groups/default/subjects/orders/versions/latest'

# Pobierz schemat Avro z Apicurio Schema Registry
def get_latest_schema():
    response = requests.get(SCHEMA_REGISTRY_URL)
    schema_json = response.json()['schema']
    schema = load_schema(io.StringIO(schema_json))
    return schema

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: v  # Ręczna serializacja za pomocą Avro
)

def serialize_avro_message(schema, value):
    """ Serializuj wiadomość Avro. """
    bytes_writer = io.BytesIO()
    schemaless_writer(bytes_writer, schema, value)
    return bytes_writer.getvalue()

# Callback for acknowledging receipt
def on_send_success(record_metadata):
    print(f"Message produced to {record_metadata.topic} partition [{record_metadata.partition}] at offset {record_metadata.offset}")

def on_send_error(excp):
    print(f"Failed to deliver message: {excp}")

def send_message(schema, value):
    # Serializuj wiadomość w Avro
    serialized_value = serialize_avro_message(schema, value)
    producer.send(KAFKA_TOPIC, value=serialized_value).add_callback(on_send_success).add_errback(on_send_error)

if __name__ == '__main__':
    schema = get_latest_schema()
    print("Sending messages to Kafka...")

    for i in range(10):
        # Get the current time in a human-readable format (day, second, millisecond)
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        message = {
            "id": i,
            "message": f"Hello Kafka {i}",
            "timestamp": current_time  # Add timestamp as a human-readable string
        }
        send_message(schema, message)
        time.sleep(5)

    # Flush the messages to ensure delivery
    producer.flush()
