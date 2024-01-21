from datetime import datetime
import os
from uuid import uuid4, UUID
from dotenv import load_dotenv, find_dotenv
import json
import numpy as np

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd

# Define Kafka configuration
load_dotenv()
kafka_id = os.environ.get("confluent_kafka_id")
kafka_secret_key = os.environ.get("confluent_kafka_secret_key")
kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': kafka_id,
    'sasl.password': kafka_secret_key
}

# Create a Schema Registry client
url = 'https://psrc-kjwmg.ap-southeast-2.aws.confluent.cloud'
schema_id = os.environ.get("confluent_schema_id")
schema_secret = os.environ.get("confluent_schema_secret")
schema_registry_client = SchemaRegistryClient({
  'url': url,
  'basic.auth.user.info': '{}:{}'.format(schema_id, schema_secret)
})
# Fetch the latest Avro schema for the value
topic = 'bookings-topic'
subject_name = f"{topic}-value"

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def fetch_kafka_schema(subject_name: str):
    try:
        schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
    except Exception as e:
        return None
        pass
        # raise Exception(f"Kafka schema for subject {subject_name} doesn't exist")
    return schema_str

def date_encoder(data):
    if isinstance(data, datetime):
        return data.isoformat()


# schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
schema_str = fetch_kafka_schema(subject_name)
# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_serializer = StringSerializer('utf_8')
if schema_str:
    # avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    json_serializer = JSONSerializer(schema_str, schema_registry_client)
    # Define the SerializingProducer
    producer = SerializingProducer({
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanisms': kafka_config['sasl.mechanisms'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password'],
        'key.serializer': key_serializer,  # Key will be serialized as a string
        'value.serializer': json_serializer  # Value will be serialized as Avro
    })       
    with open('mock_booking_data.json', 'r') as f:
        for row in f:
            # Create a dictionary from the row values
            value = json.loads(row)
            # Produce to Kafka
            producer.produce(topic=f"{topic}", key=value["booking_id"], value=value, on_delivery=delivery_report)
    producer.flush()
    print("Data successfully published to Kafka")