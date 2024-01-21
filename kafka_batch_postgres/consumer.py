from concurrent.futures import ThreadPoolExecutor
import os
import json
from dotenv import load_dotenv
from datetime import datetime
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer


load_dotenv()
# Define Kafka configuration
kafka_id = os.environ.get("confluent_kafka_id")
kafka_secret_key = os.environ.get("confluent_kafka_secret_key")
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': kafka_id,
    'sasl.password': kafka_secret_key,
    'group.id': 'group11',
    'auto.offset.reset': 'earliest'
}

def fetch_kafka_schema(subject_name: str):
    try:
        schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
    except Exception as e:
        return None
        pass
        # raise Exception(f"Kafka schema for subject {subject_name} doesn't exist")
    return schema_str

def create_consumer(kafka_config: dict):
    # Define the DeserializingConsumer
    consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})
    return consumer

# Create a Schema Registry client
url = 'https://psrc-kjwmg.ap-southeast-2.aws.confluent.cloud'
schema_id = os.environ.get("confluence_schema_id")
schema_secret = os.environ.get("confluence_schema_secret")
schema_registry_client = SchemaRegistryClient({
  'url': url,
  'basic.auth.user.info': '{}:{}'.format(schema_id, schema_secret)
})

# Fetch the latest Avro schema for the value
topic = 'orders_topic'
subject_name = f"{topic}-value"
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)


def consume_messages(thread):
    consumer = create_consumer(kafka_config)
    # Subscribe to the topic
    consumer.subscribe([f"{topic}"])

    with open(f'consumer_{thread}.json', 'w') as f:
        #Continually read messages from Kafka
        try:
            while True:
                msg = consumer.poll(1.0) # How many seconds to wait for message

                if msg is None:
                    continue
                if msg.error():
                    print('Consumer error: {}'.format(msg.error()))
                    continue
                msg.value()["last_update"] = datetime.strftime(msg.value().get("last_update"), "%Y-%m-%d %H:%M:%S") \
                    if isinstance(msg.value().get("last_update"), datetime) else msg.value().get("last_update")
                json_str = json.dumps(msg.value(), ensure_ascii=False)
                f.write(json_str + "\n")
                f.flush()
                print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))

        except KeyboardInterrupt:
            pass
        finally:
            f.close()
            consumer.close()

num_threads = 5

with ThreadPoolExecutor(max_workers=num_threads) as executor:
    futures = [executor.submit(consume_messages, thread_num) for thread_num in range(num_threads)]
    for future in futures:
        future.result()
