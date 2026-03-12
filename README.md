# IoT Kappa Architecture – Factory Sensor Pipeline

> Cloud Computing & Big Data – Abgabe  
> Autor: janferb1  
> Lizenz Code: [Apache 2.0](LICENSE)  
> Lizenz Dokumentation: [Creative Commons CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

---

## Übersicht

Dieses Projekt implementiert eine **Kappa-Architektur** zur Echtzeit-Verarbeitung von IoT-Sensordaten einer Fabrik. 20 Sensoren (Temperatur, Luftfeuchtigkeit, Druck, Vibration, CO₂) aus 6 Standorten streamen kontinuierlich Daten durch die Pipeline.

```
CSV (Sensordaten)
      │
      ▼
Streaming Server (Kafka Producer)
      │
      ▼
Apache Kafka (iot-sensor-topic)
      │
      ▼
Apache Spark Structured Streaming
+ KMeans Clustering (MLlib)
      │
      ▼
MinIO Object Storage
(Parquet-Dateien unter s3a://iot-output/sensor-clusters/)
```

---

## Architektur

| Komponente | Technologie | Rolle |
|---|---|---|
| Producer | Python + kafka-python | Liest CSV, sendet Zeilen an Kafka |
| Message Broker | Apache Kafka (KRaft) | Puffert und verteilt Nachrichten |
| Stream Processor | Apache Spark 3.5.0 + PySpark | Konsumiert Kafka, KMeans-Clustering |
| Object Storage | MinIO | Speichert Ergebnisse als Parquet |

---

## Voraussetzungen

- Docker & Docker Compose
- Git

---

## Quickstart

```bash
# 1. Repository klonen
git clone https://github.com/janferb1/janferb1-cloud_big_data_kappa_architektur.git
cd janferb1-cloud_big_data_kappa_architektur

# 2. Pipeline starten
docker compose up --build
```

---

## Dateistruktur

```
.
├── docker-compose.yaml          # Alle Services: Kafka, Spark, MinIO
├── kafka/
│   ├── Dockerfile               # Python 3.11 Image für den Producer
│   ├── requirements.txt         # kafka-python
│   ├── streaming_server.py      # Kafka Producer – liest CSV, sendet an Kafka
│   └── iot_sensor_data.csv      # Sensordaten (20 Sensoren, 6 Standorte)
└── spark/
    ├── Dockerfile               # Spark Image mit numpy, pandas, pyarrow
    └── spark_streaming_app.py   # Spark Structured Streaming + KMeans (MLlib)
```

---

## Services

### broker – Apache Kafka
- Image: `apache/kafka:latest` (KRaft-Modus, kein Zookeeper)
- Topic: `iot-sensor-topic`
- Port: `9092`

### streaming-server – Kafka Producer
- Liest `iot_sensor_data.csv` Zeile für Zeile
- Sendet jede Zeile als Nachricht an Kafka
- Wiederholt die CSV in einer Endlosschleife (Replay)

### spark – PySpark Structured Streaming
- Konsumiert Nachrichten von Kafka
- Parst CSV-Felder: `timestamp, sensor_id, sensor_type, location, value, unit`
- Führt **KMeans-Clustering (k=5)** mit Spark MLlib durch
- Schreibt Ergebnisse als **Parquet** nach MinIO

### minio – Object Storage
- S3-kompatibler Objektspeicher
- Bucket: `iot-output`
- Web-UI: `http://localhost:9001` (Login: `minioadmin` / `minioadmin`)

---

## Ergebnisse prüfen

Nachdem die Pipeline läuft, können die Parquet-Dateien in der MinIO Web-UI eingesehen werden:

1. Browser öffnen: `http://localhost:9001`
2. Login: `minioadmin` / `minioadmin`
3. Bucket `iot-output` → Ordner `sensor-clusters`

Erfolgreiche Verarbeitung in den Spark-Logs:
```
[Spark] Streaming query started. Waiting for data ...
[Spark] Batch 1: 299 rows received.
[Spark] Batch 1: written to s3a://iot-output/sensor-clusters
```

---

## Pipeline stoppen

```bash
docker compose down
```

---

## Referenzen

- Vorlesungsrepository: [github.com/sturc/cloud_computing_big_data](https://github.com/sturc/cloud_computing_big_data)
- Apache Kafka Dokumentation: [kafka.apache.org](https://kafka.apache.org)
- Apache Spark Dokumentation: [spark.apache.org](https://spark.apache.org)
- MinIO Dokumentation: [min.io/docs](https://min.io/docs/minio/container/index.html)
