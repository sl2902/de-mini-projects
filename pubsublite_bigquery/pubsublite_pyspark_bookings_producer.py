from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# from pyspark.sql.streaming import GroupStateTimeout
from pyspark.sql.types import *
from dotenv import load_dotenv
import json
import os
import uuid
from google.cloud import bigquery
from google.api_core.exceptions import AlreadyExists, Conflict

project_number = "hive-project-404608"
location = "us-central1"
topic_id = "bookings"

packages = ",".join([ 
                     "com.google.cloud:pubsublite-spark-sql-streaming:1.0.0",
                     "com.google.cloud:google-cloud-pubsublite:1.9.0",
                     "com.github.ben-manes.caffeine:caffeine:2.9.0",
                     "org.scala-lang.modules:scala-java8-compat_2.12:1.0.0"
                     ])
spark = SparkSession \
        .builder\
        .appName("Flights")\
        .master("local[*]")\
        .enableHiveSupport()\
        .config("spark.sql.shuffle.partitions", "2")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .config("spark.jars.packages", packages)\
        .getOrCreate()

# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled","true")

df = spark.read.json("mock_subset_booking_data.json")
# sdf = df.toPandas()
# pdf = sdf.to_dict(orient="records")
# values = [row.values() for row in pdf]
# df.withColumn("value", lit(values))
df = df.withColumn("booking_time", col("booking_time").cast("timestamp"))
df = df.withColumn("value",
              create_map(lit("booking_id"), col("booking_id"),
                         lit("customer_id"), col("customer_id"),
                         lit("flight_id"), col("flight_id"),
                         lit("booking_time"), col("booking_time"),
                         lit("origin"), col("origin"),
                         lit("destination"), col("destination"),
                         lit("price"), col("price")
                        )
            )
df.printSchema()

df = (
        df.withColumn("key", col("booking_id").cast(BinaryType()))
            .withColumn("data", col("value").cast(StringType()).cast(BinaryType()))
            .withColumnRenamed("booking_time", "event_timestamp")
            .withColumn(
                "attributes",
                create_map(
                    lit("booking_id"),
                    array(col("booking_id").cast(BinaryType())),
                    lit("customer_id"),
                    array(col("customer_id").cast(BinaryType())),
                    lit("flight_id"),
                    array(col("flight_id").cast(BinaryType())),
                    lit("event_timestamp"),
                    array(col("event_timestamp").cast(StringType()).cast(BinaryType())),
                    lit("origin"),
                    array(col("origin").cast(BinaryType())),
                    lit("destination"),
                    array(col("destination").cast(BinaryType())),
                    lit("price"),
                    array(col("price").cast(StringType()).cast(BinaryType()))
                )
            )
            .drop("value", "booking_id", "customer_id", "flight_id", "origin", "destination", "price")
)
json_schema = df.schema
# have to create a streaming DF
df.write.mode("overwrite").json("./bookings")
sdf = spark.readStream \
        .schema(json_schema) \
        .json("./bookings")

# query = sdf.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# q.awaitTermination(60)
# q.stop()

query = (
    sdf.writeStream.format("pubsublite")
    .option(
        "pubsublite.topic",
        f"projects/{project_number}/locations/{location}/topics/{topic_id}",
    )
    # Required. Use a unique checkpoint location for each job.
    .option("checkpointLocation", "/tmp/app" + uuid.uuid4().hex)
    .outputMode("append")
    .option("truncate", False)
    # .trigger(processingTime="1 second")
    .start()
)

# Wait 60 seconds to terminate the query.
query.awaitTermination()
query.stop()