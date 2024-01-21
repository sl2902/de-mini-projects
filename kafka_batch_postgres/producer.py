from datetime import datetime
import os
from uuid import uuid4, UUID
from dotenv import load_dotenv, find_dotenv
import json
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd


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

def create_db_connection(
        # use default db to connect to
        # then create a db of your choice
        db: str="postgres", 
        user: str="postgres", 
        pwd: str="postgres", 
        port: str="5432",
):
    try:
        conn = psycopg2.connect(dbname=db, user=user, password=pwd, port=port)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    except Exception as e:
        raise Exception(f"Connection to postgres db failed {e}")
    return conn

def fetch_data_db(cursor, last_read_ts: str, table_name: str="products"):
    # datetime.strptime(last_read_ts, "%Y-%m-%d %H:%M:%S")
    qry = f""" select * from {table_name}
                where last_update > '{last_read_ts}'
        """
    cursor.execute(qry)
    return cursor.fetchall()

def max_sql_date(cursor, table_name: str="products"):
    # datetime.strptime(last_read_ts, "%Y-%m-%d %H:%M:%S")
    qry = f""" select max(last_update::timestamp) from {table_name}"""
    cursor.execute(qry)
    return cursor.fetchall()

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
schema_id = os.environ.get("confluence_schema_id")
schema_secret = os.environ.get("confluence_schema_secret")
schema_registry_client = SchemaRegistryClient({
  'url': url,
  'basic.auth.user.info': '{}:{}'.format(schema_id, schema_secret)
})

# Fetch the latest Avro schema for the value
topic = 'orders_topic'
subject_name = f"{topic}-value"
# schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
schema_str = fetch_kafka_schema(subject_name)

# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_serializer = StringSerializer('utf_8')
if schema_str:
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)
    # Define the SerializingProducer
    producer = SerializingProducer({
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanisms': kafka_config['sasl.mechanisms'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password'],
        'key.serializer': key_serializer,  # Key will be serialized as a string
        'value.serializer': avro_serializer  # Value will be serialized as Avro
    })
    # json_serializer = JSONSerializer(schema_str, schema_registry_client)

try:
    with open('config.json', 'r') as f:
        data = json.load(f)
        last_read_ts = data.get("last_read_timestamp", "1900-01-01 00:00:00")
except FileNotFoundError as e:
    pass

conn = create_db_connection(db="kafka_assignment")
cursor = conn.cursor()
table_name = "products"
result_set = fetch_data_db(cursor, last_read_ts)
cols = [col[0] for col in cursor.description]

# Iterate over DataFrame rows and produce to Kafka
if result_set: 
    for row in result_set:
        # Create a dictionary from the row values
        value = dict(zip(cols, row))
        value["last_update"] = datetime.strftime(value["last_update"], "%Y-%m-%d %H:%M:%S") \
            if isinstance(value["last_update"], datetime) else value["last_update"]
        # Produce to Kafka
        producer.produce(topic=f"{topic}", key=str(value["id"]), value=value, on_delivery=delivery_report)
    producer.flush()
print("Data successfully published to Kafka")

result_set = max_sql_date(cursor)
data["last_read_timestamp"] = datetime.strftime(result_set[0][0], "%Y-%m-%d %H:%M:%S") if isinstance(result_set[0][0], datetime) else result_set[0][0]
try:
    with open('config.json', 'w', encoding='utf-8') as f:
        json_str = json.dumps(data, ensure_ascii=False)
        f.write(json_str + "\n")
except FileNotFoundError as e:
    pass