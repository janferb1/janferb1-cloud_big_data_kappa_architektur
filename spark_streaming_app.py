"""
spark_streaming_app.py
======================
IoT Kappa Architecture – Spark Structured Streaming Job

Pipeline:
  Kafka (iot-sensor-topic)
    → parse CSV fields
    → KMeans clustering (k=5) per micro-batch
    → write Parquet to MinIO (s3a://iot-output/sensor-clusters/)

Environment variables (set via docker-compose):
  KAFKA_BROKER   – e.g. broker:9092
  KAFKA_TOPIC    – e.g. iot-sensor-topic
  MINIO_ENDPOINT – e.g. http://minio:9000
  MINIO_ACCESS   – minioadmin
  MINIO_SECRET   – minioadmin
  MINIO_BUCKET   – iot-output
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.clustering import KMeans

# ── Configuration ──────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",   "broker:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC",    "iot-sensor-topic")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",   "minioadmin")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET",   "iot-output")
CHECKPOINT_DIR = f"s3a://{MINIO_BUCKET}/checkpoints"
OUTPUT_DIR     = f"s3a://{MINIO_BUCKET}/sensor-clusters"

# ── Spark Session ──────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("IoT-Kappa-Streaming")
    # S3A / MinIO configuration
    .config("spark.hadoop.fs.s3a.endpoint",               MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access",      "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("[Spark] Session started.")

# ── Read from Kafka ────────────────────────────────────────────
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# ── Parse CSV rows ─────────────────────────────────────────────
# Schema: timestamp,sensor_id,sensor_type,location,value,unit,status
value_df = raw_df.selectExpr("CAST(value AS STRING) as raw")

parsed_df = (
    value_df
    .withColumn("fields",      split(col("raw"), ","))
    .withColumn("timestamp",   col("fields").getItem(0))
    .withColumn("sensor_id",   col("fields").getItem(1))
    .withColumn("sensor_type", col("fields").getItem(2))
    .withColumn("location",    col("fields").getItem(3))
    .withColumn("value",       col("fields").getItem(4).cast(DoubleType()))
    .withColumn("unit",        col("fields").getItem(5))
    .withColumn("status",      col("fields").getItem(6))
    .drop("raw", "fields")
    .filter(col("value").isNotNull())
)

# ── Foreach-Batch: KMeans clustering per micro-batch ───────────
def process_batch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        print(f"[Spark] Batch {batch_id}: empty, skipping.")
        return

    print(f"[Spark] Batch {batch_id}: {count} rows received.")

    # Encode categorical features
    indexer_type = StringIndexer(
        inputCol="sensor_type", outputCol="sensor_type_idx", handleInvalid="keep"
    )
    indexer_loc = StringIndexer(
        inputCol="location", outputCol="location_idx", handleInvalid="keep"
    )

    df_indexed = indexer_type.fit(batch_df).transform(batch_df)
    df_indexed = indexer_loc.fit(df_indexed).transform(df_indexed)

    # Assemble feature vector
    assembler = VectorAssembler(
        inputCols=["value", "sensor_type_idx", "location_idx"],
        outputCol="features",
        handleInvalid="skip"
    )
    df_features = assembler.transform(df_indexed)

    # KMeans (k=5 clusters)
    k = min(5, count)   # safety: k cannot exceed number of rows
    kmeans = KMeans(k=k, seed=42, featuresCol="features", predictionCol="cluster")
    model  = kmeans.fit(df_features)
    df_result = model.transform(df_features)

    # Write results as Parquet, partitioned by sensor_type
    (
        df_result
        .drop("features")
        .write
        .mode("append")
        .partitionBy("sensor_type")
        .parquet(OUTPUT_DIR)
    )
    print(f"[Spark] Batch {batch_id}: written to {OUTPUT_DIR}")

# ── Start Streaming Query ──────────────────────────────────────
query = (
    parsed_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="30 seconds")
    .start()
)

print("[Spark] Streaming query started. Waiting for data ...")
query.awaitTermination()
