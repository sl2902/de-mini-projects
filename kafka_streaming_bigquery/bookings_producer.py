from datetime import datetime
import os
from uuid import uuid4, UUID
from dotenv import load_dotenv, find_dotenv
import json
import numpy as np

from confluent_kafka import SerializingProducer
import pandas as pd

# Define Kafka configuration
load_dotenv()

topic = 'bookings-topic'

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


def date_encoder(data):
    if isinstance(data, datetime):
        return data.isoformat()



# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': "localhost:9092",
})       
with open('data/mock_booking_data.json', 'r') as f:
    for row in f:
        # Create a dictionary from the row values
        value = json.loads(row)
        # Produce to Kafka
        producer.produce(topic=f"{topic}", key=value["booking_id"], value=json.dumps(value), on_delivery=delivery_report)
producer.flush()
print("Data successfully published to Kafka")
