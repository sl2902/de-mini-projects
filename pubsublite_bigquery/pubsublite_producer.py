from google.cloud.pubsublite.cloudpubsub import PublisherClient
from google.cloud.pubsublite.types import (
    CloudRegion,
    CloudZone,
    MessageMetadata,
    TopicPath,
)
import json
import argparse



parser = argparse.ArgumentParser()

parser.add_argument("--topic", required=True, help="Enter topic name")
parser.add_argument("--filename", required=True, help="Enter filename")
args = parser.parse_args()

project_number = "hive-project-404608"
cloud_region = "us-central1"
zone_id = "c"
topic_id = args.topic
regional = True

if regional:
    location = CloudRegion(cloud_region)
else:
    location = CloudZone(CloudRegion(cloud_region), zone_id)

topic_path = TopicPath(project_number, location, topic_id)

# PublisherClient() must be used in a `with` block or have __enter__() called before use.
with PublisherClient() as publisher_client:
    with open(args.filename, "r") as f:
        for row in f:
            data = json.dumps(json.loads(row))
            api_future = publisher_client.publish(topic_path, data.encode("utf-8"))
            # result() blocks. To resolve API futures asynchronously, use add_done_callback().
            message_id = api_future.result()
            message_metadata = MessageMetadata.decode(message_id)
            print(
                f"Published a message to {topic_path} with partition {message_metadata.partition.value} and offset {message_metadata.cursor.offset}."
            )