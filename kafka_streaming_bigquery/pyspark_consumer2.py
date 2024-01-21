from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# from pyspark.sql.streaming import GroupStateTimeout
from pyspark.sql.types import *
from dotenv import load_dotenv
import json
import os
from google.cloud import bigquery

"""
set options to enable gs://
    .option("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .option("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .option("google.cloud.auth.service.account.enable", "true")
    .option("google.cloud.auth.service.account.json.keyfile", "<path-to-json-keyfile.json>")
"""
# what finally worked
# created service account file
# added hadoop connector jar file to /usr/local/Cellar/apache-spark/3.3.0/libexec/jars/

# https://github.com/GoogleCloudDataproc/spark-bigquery-connector
load_dotenv()
temp_bucket = "gs://reservation_booking"
bq_dataset = "reservation"
table_name = "booking_payments"
packages = ",".join(["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0", 
                     "com.google.cloud.spark:spark-3.3-bigquery:0.35.1",
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
datasets = list(client.list_datasets()) 

dataset_flag = True
if datasets:
    for dataset in datasets:
        if dataset == "bq_dataset":
            print(f"BQ dataset {bq_dataset} exists")
            dataset_flag = False
            break

if not dataset_flag:
    dataset_id = "{}.{}".format(client.project, bq_dataset)
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
conf.set("google.cloud.auth.service.account.json.keyfile", "/Users/home/Documents/grow_data/hive-project-404608-480340eab6cd.json")
conf.set("google.cloud.auth.application-default.credentials-file", "/Users/home/Documents/grow_data/hive-project-404608-480340eab6cd.json")
conf.set("spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS", "/Users/home/Documents/grow_data/hive-project-404608-480340eab6cd.json")


spark.sparkContext.setLogLevel("ERROR")
topic = "bookings-topic"
payment_topic = "payments-topic"

kafka_options = {
        "kafka.bootstrap.servers": "localhost:9092",
        "subscribe": topic,
        "enable.partition.eof": "true",
        "startingOffsets": "earliest"
        # "auto.offset.reset": "earliest"
}

kafka_payment_options = {
        "kafka.bootstrap.servers": "localhost:9092",
        "subscribe": payment_topic,
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

json_payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("booking_id", StringType(), True),
    StructField("payment_time", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True)
])

booking_df = spark.readStream.format("kafka")\
        .options(**kafka_options)\
        .load()

booking_df = booking_df.selectExpr("cast(value as string) as value")
booking_df = booking_df\
        .withColumn("value", from_json(booking_df["value"], json_schema))\
        .select("value.*")\
        .select(
            "booking_id", 
            "customer_id", 
            "flight_id", 
            col("booking_time").cast("timestamp").alias("booking_time"),
            "origin",
            "destination",
            "price"
        )
payment_df = spark.readStream.format("kafka")\
                .options(**kafka_payment_options)\
                .load()

payment_df = payment_df.selectExpr("cast(value as string) as value")\
                .withColumn("value", from_json(col("value"), json_payment_schema))\
                .select("value.*")\
                .select(
                    "payment_id",
                    col("booking_id").alias("payment_booking_id"),
                    col("payment_time").cast("timestamp").alias("payment_time"),
                    "payment_method",
                    "payment_status"
                )
booking_df = booking_df.withWatermark("booking_time", "1 hours")
payment_df = payment_df.withWatermark("payment_time", "1 hours")

booking_df.createOrReplaceTempView("bookings")
payment_df.createOrReplaceTempView("payments")
result = spark.sql("""
          select
            a.*,
             b.*
          from
            bookings a join payments b on a.booking_id = b.payment_booking_id
          and payment_time >= booking_time
          """)

joined = booking_df.join(
    payment_df,
    expr("""
         booking_id = payment_booking_id and
         payment_time >= booking_time and
         payment_time <= booking_time + interval 1 hour
         """),
        "inner")\
    .drop("payment_booking_id")

# def process_events(key, value, state):
#     print(value)
#     if state.exists:
#         pass
#     else:
#         state.update(value)

# result = df.groupBy("booking_id").mapGroupsWithState(process_events, GroupStateTimeout.NoTimeout)

def write_bigquery(df, batch_id):
    df.write.format("bigquery")\
            .option("temporaryGcsBucket", temp_bucket)\
            .option("table", f"{bq_dataset}.{table_name}")\
            .mode("append")\
            .save()

# df1 = spark.read.json("mock_booking_data.json")
# df2 = spark.read.json("mock_payment_data.json")
# result = df1.join(df2, df1.booking_id == df2.booking_id, "inner").drop(df2.booking_id)
# result.write.format("csv").option("header", "true").save("static_result")

# query = joined.writeStream\
#         .outputMode("append")\
#         .format("csv")\
#         .option("checkpointLocation", "/tmp/pyspark_streaming_bookings")\
#         .option("path", "merges_result")\
#         .start()
query = joined.writeStream\
        .outputMode("append")\
        .foreachBatch(write_bigquery)\
        .start()
query.awaitTermination()

# query = joined.writeStream\
#         .outputMode("append")\
#         .format("console")\
#         .option("truncate", "false")\
#         .start()
# query.awaitTermination()
