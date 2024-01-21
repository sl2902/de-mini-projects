from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import *
from dotenv import load_dotenv
import json
import os
from confluent_kafka.schema_registry import SchemaRegistryClient

load_dotenv()
packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
spark = SparkSession \
        .builder\
        .master("local[*]")\
        .appName("Flighs")\
        .config("spark.sql.shuffle.partitions", "2")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.jars.packages", packages)\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
# Define Kafka configuration
kafka_id = os.environ.get("confluent_kafka_id")
kafka_secret_key = os.environ.get("confluent_kafka_secret_key")
topic = "bookings-topic"

url = "https://psrc-kjwmg.ap-southeast-2.aws.confluent.cloud"
schema_id = os.environ.get("confluent_schema_id")
schema_secret = os.environ.get("confluent_schema_secret")
schema_registry_client = SchemaRegistryClient({
    "url": url,
    "basic.auth.user.info": "{}:{}".format(schema_id, schema_secret)
})

subject_name =f"{topic}-value"
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
kafka_options = {
        'kafka.bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
        'kafka.sasl.mechanism': 'PLAIN',
        'kafka.security.protocol': 'SASL_SSL',
        "kafka.sasl.jaas.config": f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_id}' password='{kafka_secret_key}';",
        "kafka.ssl.endpoint.identification.algorithm": "https",
        "subscribe": topic,
        "enable.partition.eof": "true",
        "startingOffsets": "earliest"
        # "auto.offset.reset": "earliest"
}

json_schema = StructType([
    StructField("booking_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("flight_id", StringType(), True),
    StructField("booking_time", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("price", FloatType(), True)
])

df = spark.readStream.format("kafka")\
        .options(**kafka_options)\
        .load()

df = df.selectExpr("cast(value as string) as value")
df = df\
        .withColumn("value", from_json(df["value"], json_schema)).select("value.*")

query = df.writeStream\
        .outputMode("append")\
        .format("console")\
        .option("truncate", "false")\
        .start()
query.awaitTermination()

        