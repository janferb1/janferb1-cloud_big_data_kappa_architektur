"""
spark_streaming_app.py – IoT Kappa Architecture: Speed Layer
=============================================================
Spark Structured Streaming Anwendung, die:
  1. IoT-Sensor-Messages vom Kafka-Topic konsumiert
  2. CSV-Zeilen parst und strukturiert
  3. KMeans-Clustering (Unsupervised Learning) pro Micro-Batch anwendet
  4. Ergebnisse als Parquet-Dateien in MinIO schreibt (Serving Layer)

Kappa-Architektur Rolle: Stream Processing Layer → Serving Layer

Voraussetzungen (spark-submit --packages):
  - org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
  - org.apache.hadoop:hadoop-aws:3.3.4
  - com.amazonaws:aws-java-sdk-bundle:1.12.262
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, col, to_timestamp, when
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

# ── Konfiguration via Umgebungsvariablen ──────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",   "localhost:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC",    "iot-sensor-topic")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",   "minioadmin")
MINIO_BUCKET   = os.getenv("MINIO_BUCKET",   "iot-output")
OUTPUT_PATH    = f"s3a://{MINIO_BUCKET}/sensor-clusters/"
CHECKPOINT_DIR = f"s3a://{MINIO_BUCKET}/checkpoints/"
NUM_CLUSTERS   = int(os.getenv("NUM_CLUSTERS", "5"))

# ── Spark Session mit S3A/MinIO-Konfiguration ────────────────────────────────
spark = (SparkSession.builder
    .appName("IoT-Kappa-Streaming")
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
    .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.shuffle.partitions",          "4")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ── Schritt 1: Kafka-Stream einlesen ─────────────────────────────────────────
# Jede Kafka-Message enthält eine CSV-Zeile als String
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "latest")
    .load())

# ── Schritt 2: CSV-Zeilen parsen ──────────────────────────────────────────────
# Erwartetes Format: timestamp,sensor_id,sensor_type,location,value,unit,status
parsed_stream = (raw_stream
    .selectExpr("CAST(value AS STRING) as raw")   # Kafka-Bytes → String
    .select(split(col("raw"), ",").alias("f"))    # Komma-Split
    .select(
        to_timestamp(col("f")[0]).alias("timestamp"),
        col("f")[1].alias("sensor_id"),
        col("f")[2].alias("sensor_type"),
        col("f")[3].alias("location"),
        col("f")[4].cast(DoubleType()).alias("value"),
        col("f")[5].alias("unit"),
        col("f")[6].alias("status"),
    )
    .filter(col("value").isNotNull())  # Fehlerhafte Zeilen verwerfen
)


# ── Schritt 3: KMeans-Clustering pro Micro-Batch ──────────────────────────────
def apply_clustering_and_write(batch_df: DataFrame, batch_id: int) -> None:
    """
    Wird für jeden Micro-Batch aufgerufen.
    Führt KMeans-Clustering auf den Sensordaten durch und
    schreibt das Ergebnis partitioniert nach sensor_type nach MinIO.
    """
    count = batch_df.count()
    if count == 0:
        print(f"[Batch {batch_id}] Leer – übersprungen.")
        return

    print(f"[Batch {batch_id}] Verarbeite {count} Datensätze...")

    # sensor_type (String) → numerischen Index für Feature-Vektor umwandeln
    indexer = StringIndexer(
        inputCol="sensor_type",
        outputCol="sensor_type_idx",
        handleInvalid="keep"
    )

    # Feature-Vektor aus Messwert + Sensor-Typ-Index erstellen
    assembler = VectorAssembler(
        inputCols=["value", "sensor_type_idx"],
        outputCol="features"
    )

    # KMeans: Anzahl Cluster = min(NUM_CLUSTERS, Batch-Größe)
    kmeans = KMeans(
        k=min(NUM_CLUSTERS, count),
        seed=42,
        featuresCol="features",
        predictionCol="cluster"
    )

    # Pipeline: Indexer → Assembler → KMeans
    pipeline = Pipeline(stages=[indexer, assembler, kmeans])
    result = pipeline.fit(batch_df).transform(batch_df)

    # Interne Zwischenspalten entfernen
    result = result.drop("features", "sensor_type_idx")

    # Cluster-ID mit lesbarem Label versehen
    result = result.withColumn("cluster_label",
        when(col("cluster") == 0, "normal_operation")
       .when(col("cluster") == 1, "elevated_readings")
       .when(col("cluster") == 2, "critical_zone")
       .when(col("cluster") == 3, "low_activity")
       .otherwise("anomaly")
    )

    # Schritt 4: Ergebnis als Parquet nach MinIO schreiben (Serving Layer)
    # Partitionierung nach sensor_type für effiziente Abfragen
    (result.write
        .mode("append")
        .partitionBy("sensor_type")
        .parquet(OUTPUT_PATH))

    print(f"[Batch {batch_id}] Erfolgreich nach {OUTPUT_PATH} geschrieben.")


# ── Schritt 4: Streaming-Query starten ───────────────────────────────────────
query = (parsed_stream.writeStream
    .foreachBatch(apply_clustering_and_write)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="30 seconds")   # Micro-Batch alle 30 Sekunden
    .start())

print("=" * 60)
print("IoT Spark Streaming App gestartet.")
print(f"  Kafka Topic : {KAFKA_TOPIC}")
print(f"  MinIO Output: {OUTPUT_PATH}")
print("Warte auf Daten...")
print("=" * 60)

query.awaitTermination()
