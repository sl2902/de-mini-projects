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
topic_id = "payments"

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

df = spark.read.json("mock_subset_payment_data.json")
# sdf = df.toPandas()
# pdf = sdf.to_dict(orient="records")
# values = [row.values() for row in pdf]
# df.withColumn("value", lit(values))
df = df.withColumn("payment_time", col("payment_time").cast("timestamp"))
df = df.withColumn("value",
              create_map(lit("payment_id"), col("payment_id"),
                         lit("booking_id"), col("booking_id"),
                         lit("payment_time"), col("payment_time"),
                         lit("payment_method"), col("payment_method"),
                         lit("payment_status"), col("payment_status")
                        )
            )
df.printSchema()

df = (
        df.withColumn("key", col("payment_id").cast(BinaryType()))
            .withColumn("data", col("value").cast(StringType()).cast(BinaryType()))
            .withColumnRenamed("payment_time", "event_timestamp")
            .withColumn(
                "attributes",
                create_map(
                    lit("payment_id"),
                    array(col("payment_id").cast(BinaryType())),
                    lit("booking_id"),
                    array(col("booking_id").cast(BinaryType())),
                    lit("event_timestamp"),
                    array(col("event_timestamp").cast(StringType()).cast(BinaryType())),
                    lit("payment_method"),
                    array(col("payment_method").cast(BinaryType())),
                    lit("payment_status"),
                    array(col("payment_status").cast(BinaryType()))
                )
            )
            .drop("value", "booking_id", "payment_id", "payment_time", "payment_method", "payment_status")
)
json_schema = df.schema
# have to create a streaming DF
df.write.mode("overwrite").json("./payments")
sdf = spark.readStream \
        .schema(json_schema) \
        .json("./payments")

# query = sdf.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination(60)
# q.uerystop()

query = (
    sdf.writeStream.format("pubsublite")
    .option(
        "pubsublite.topic",
        f"projects/{project_number}/locations/{location}/topics/{topic_id}",
    )
    # Required. Use a unique checkpoint location for each job.
    .option("checkpointLocation", "/tmp/app" + uuid.uuid4().hex)
    .outputMode("append")
    # .trigger(processingTime="1 second")
    .start()
)

# Wait 60 seconds to terminate the query.
query.awaitTermination()
query.stop()