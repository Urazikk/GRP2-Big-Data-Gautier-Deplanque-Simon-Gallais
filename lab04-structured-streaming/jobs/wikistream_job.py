"""Standalone version of the tumbling window aggregation, run with spark-submit.

Edits per minute (bots / humans, bytes added and removed) are appended to parquet files in
/home/jovyan/data. The checkpoint keeps the Kafka offsets and the window state, so if the job is
stopped and started again it continues where it stopped (no event read twice, no event lost).

docker exec pyspark_notebook bash -c 'spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:$(python -c "import pyspark; print(pyspark.__version__)") \
  /home/jovyan/jobs/wikistream_job.py'
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as sum_, when, coalesce, lit, timestamp_seconds
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, BooleanType
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
DATA_DIR = os.environ.get("DATA_DIR", "/home/jovyan/data")
OUTPUT = f"{DATA_DIR}/edits_per_minute"
CHECKPOINT = f"{DATA_DIR}/checkpoints/edits_per_minute"

schema = StructType([
    StructField("user", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("bot", BooleanType(), True),
    StructField("length", StructType([
        StructField("old", IntegerType(), True),
        StructField("new", IntegerType(), True)
    ]), True)
])

spark = SparkSession.builder \
    .appName("WikistreamsEditsPerMinute") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

edits = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "wikistreams") \
    .option("startingOffsets", "earliest") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", timestamp_seconds(col("timestamp"))) \
    .withColumn("size_delta", coalesce(col("length.new"), lit(0)) - coalesce(col("length.old"), lit(0)))

# Tumbling 1 minute windows. The file sink only supports the append mode: a window is written once,
# when the watermark has passed its end (so about 2 minutes after the end of the minute).
per_minute = edits \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(window(col("event_time"), "1 minute"), col("bot")) \
    .agg(
        count("*").alias("edits"),
        sum_(when(col("size_delta") > 0, col("size_delta")).otherwise(0)).alias("bytes_added"),
        sum_(when(col("size_delta") < 0, -col("size_delta")).otherwise(0)).alias("bytes_removed")
    ) \
    .select(col("window.start").alias("window_start"), col("window.end").alias("window_end"),
            "bot", "edits", "bytes_added", "bytes_removed")

query = per_minute.writeStream \
    .format("parquet") \
    .option("path", OUTPUT) \
    .option("checkpointLocation", CHECKPOINT) \
    .outputMode("append") \
    .trigger(processingTime="1 minute") \
    .start()

print(f"Writing to {OUTPUT} (Ctrl+C to stop)")
query.awaitTermination()
