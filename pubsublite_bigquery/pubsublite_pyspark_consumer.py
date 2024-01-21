from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# from pyspark.sql.streaming import GroupStateTimeout
from pyspark.sql.types import *
from dotenv import load_dotenv
import json
import os
from google.cloud import bigquery
from google.api_core.exceptions import AlreadyExists, Conflict


project_number = "hive-project-404608"
location = "us-central1"
booking_subscription_id = "bookings-topic"
payment_subscription_id = "payments-topic"

load_dotenv()
temp_bucket = "gs://reservation_booking"
bq_dataset = "reservation"
table_name = "booking_payments_pubsub"
packages = ",".join([ 
                     "com.google.cloud.spark:spark-3.3-bigquery:0.35.1",
                     "com.google.cloud:pubsublite-spark-sql-streaming:1.0.0",
                     "com.google.cloud:google-cloud-pubsublite:1.9.0",
                     "com.github.ben-manes.caffeine:caffeine:2.9.0",
                     "org.scala-lang.modules:scala-java8-compat_2.12:1.0.0",
                  #  "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.9"
                     ])
jars = "gs://spark-lib/bigquery/spark-3.3-bigquery-0.35.1.jar"
spark = SparkSession \
        .builder\
        .appName("Flights")\
        .master("local[*]")\
        .enableHiveSupport()\
        .config("spark.sql.shuffle.partitions", "2")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.jars.packages", packages)\
        .getOrCreate()

# bigquery
client = bigquery.Client()
try:
    dataset_id = "{}.{}".format(client.project, bq_dataset)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
except (Conflict, AlreadyExists):
    print(f"Dataset {bq_dataset} already exists.")

# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
conf.set("google.cloud.auth.service.account.json.keyfile", "/Users/home/Documents/grow_data/hive-project-404608-480340eab6cd.json")


booking_df = (
    spark.readStream.format("pubsublite")
    .option(
        "pubsublite.subscription",
        f"projects/{project_number}/locations/{location}/subscriptions/{booking_subscription_id}",
    )
    .load()
)

payment_df = (
    spark.readStream.format("pubsublite")
    .option(
        "pubsublite.subscription",
        f"projects/{project_number}/locations/{location}/subscriptions/{payment_subscription_id}",
    )
    .load()
)
# booking_df.printSchema()
json_booking_schema = StructType([
    StructField("booking_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("flight_id", StringType()),
    StructField("booking_time", StringType()),
    StructField("origin", StringType()),
    StructField("destination", StringType()),
    StructField("price", StringType())
])

json_payment_schema = StructType([
    StructField("payment_id", StringType()),
    StructField("booking_id", StringType()),
    StructField("payment_time", TimestampType()),
    StructField("payment_method", StringType()),
    StructField("payment_status", StringType())
])

# booking_df = spark.read.json(booking_df.select("data").cast(StringType()).rdd.map(lambda row: row.data))
# booking_df = booking_df.select("*")
# converts the streaming to non-streaming
# booking_df = booking_df.rdd.map(
#     lambda x: \
#     (x.data["booking_id"], x.data["customer_id"], x.data["flight_id"], x.data["booking_time"], x.data["origin"], x.data["destination"], x.data["price"])\
#     .toDF(["booking_id", "customer_id", "flight_id", "booking_time", "origin", "destination", "price"])
# )
booking_df.printSchema()
booking_df = (
    booking_df.select("attributes")
            .withColumn("booking_id", col("attributes.booking_id").getItem(0).cast(StringType()))
            .withColumn("customer_id", col("attributes.customer_id").getItem(0).cast(StringType()))
            .withColumn("flight_id", col("attributes.flight_id").getItem(0).cast(StringType()))
            .withColumn("booking_time", col("attributes.event_timestamp").getItem(0).cast(StringType()).cast(TimestampType()).alias("booking_time"))
            .withColumn("origin", col("attributes.origin").getItem(0).cast(StringType()))
            .withColumn("destination", col("attributes.destination").getItem(0).cast(StringType()))
            .withColumn("price", col("attributes.price").getItem(0).cast(StringType()).cast(FloatType()))
            .select("booking_id", "customer_id", "flight_id", "booking_time", "origin", "destination", "price")
            .filter(col("booking_id").isNotNull())
)
# booking_df = booking_df\
#         .withColumn("data", from_json(booking_df["data"].cast(StringType()), json_booking_schema))\
#         .select("data.*")\
#         .select(
#             "booking_id", 
#             "customer_id", 
#             "flight_id", 
#             col("booking_time").cast("timestamp").alias("booking_time"),
#             "origin",
#             "destination",
#             "price"
#         )\
#         .filter(col("booking_id").isNotNull())

payment_df = (
    payment_df.select("attributes")
            .withColumn("payment_id", col("attributes.payment_id").getItem(0).cast(StringType()))
            .withColumn("payment_booking_id", col("attributes.booking_id").getItem(0).cast(StringType()))
            .withColumn("payment_time", col("attributes.event_timestamp").getItem(0).cast(StringType()).cast(TimestampType()))
            .withColumn("payment_method", col("attributes.payment_method").getItem(0).cast(StringType()))
            .withColumn("payment_status", col("attributes.payment_status").getItem(0).cast(StringType()))
            .select("payment_id", "payment_booking_id", "payment_time", "payment_method", "payment_status")
            .filter(col("payment_id").isNotNull())
)

# payment_df = payment_df\
#                 .withColumn("data", from_json(payment_df["data"].cast("string"), schema=json_payment_schema))\
#                 .select("data.*")\
#                 .select(
#                     col("payment_id"),
#                     col("booking_id"),
#                     col("payment_time").cast("timestamp").alias("payment_time"),
#                     "payment_method",
#                     "payment_status"
#                 )

booking_df = booking_df.withWatermark("booking_time", "1 hours").dropDuplicates(["booking_id", "booking_time"])
payment_df = payment_df.withWatermark("payment_time", "1 hours").dropDuplicates(["payment_booking_id", "payment_time"])

joined = booking_df.join(
    payment_df,
    expr("""
         booking_id = payment_booking_id and
         payment_time >= booking_time and
         payment_time <= booking_time + interval 1 hour
         """),
        "inner")\
        .drop("payment_booking_id")

def write_bigquery(df, batch_id):
    df.write.format("bigquery")\
            .option("temporaryGcsBucket", temp_bucket)\
            .option("table", f"{bq_dataset}.{table_name}")\
            .mode("append")\
            .save()

query = joined.writeStream\
        .outputMode("append")\
        .foreachBatch(write_bigquery)\
        .start()
# query.awaitTermination(120)


query = (
    booking_df.writeStream.format("console")
    .outputMode("append")
    .trigger(processingTime="1 second")
    .option("truncate", "false")
    .start()
)

query = (
    payment_df.writeStream.format("console")
    .outputMode("append")
    .trigger(processingTime="1 second")
    .option("truncate", "false")
    .start()
)

query = (
    joined.writeStream.format("console")
    .outputMode("append")
    .trigger(processingTime="1 second")
    .option("truncate", "false")
    .start()
)
query.awaitTermination()
# # Wait 120 seconds (must be >= 60 seconds) to start receiving messages.
# query.awaitTermination()
query.stop()