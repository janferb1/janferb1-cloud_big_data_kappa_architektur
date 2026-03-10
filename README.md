<!--
SPDX-License-Identifier: CC-BY-4.0
Documentation © 2024 – Licensed under Creative Commons Attribution 4.0
-->

# IoT Sensor Monitoring – Kappa Architecture

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Docs License](https://img.shields.io/badge/Docs-CC%20BY%204.0-green.svg)](LICENSE-DOCS)

## Anwendungsidee

Industrielle IoT-Sensoren in einer Fabrikumgebung erzeugen kontinuierlich Messdaten
(Temperatur, Luftfeuchtigkeit, CO₂, Druck, Vibration). Diese Rohdaten sollen in
Echtzeit verarbeitet, automatisch in Betriebsklassen eingruppiert (Clustering) und
für spätere Auswertungen persistent gespeichert werden.

Die Kappa-Architektur eignet sich ideal, da **alle** Verarbeitung als Stream erfolgt:
kein separater Batch-Pfad nötig. Historische Daten können jederzeit durch "Replay"
des Master-Datasets neu verarbeitet werden.

---

## Architektur

```
┌──────────────────────────────────────────────────────────────────┐
│                      Kappa Architecture                          │
│                                                                  │
│  iot_sensor_data.csv                                             │
│        │                                                         │
│        ▼                                                         │
│  ┌─────────────┐    ┌──────────────────────┐                    │
│  │  Streaming  │───▶│   Data Ingestion      │                   │
│  │   Server    │    │   Layer (Kafka)        │                   │
│  │  (Producer) │    │   Topic: iot-sensor   │                   │
│  └─────────────┘    └──────────┬───────────┘                    │
│         ▲                      │                                 │
│         │  Master Dataset      │                                 │
│         │  Replay              ▼                                 │
│  ┌──────┴──────┐    ┌──────────────────────┐                    │
│  │  CSV-Datei  │    │  Stream Processing   │                    │
│  │  (Master    │    │  Layer (Spark)        │                    │
│  │  Dataset)   │    │  + KMeans Clustering  │                   │
│  └─────────────┘    └──────────┬───────────┘                    │
│                                │                                 │
│                                ▼                                 │
│                     ┌──────────────────────┐                    │
│                     │   Serving Layer       │                   │
│                     │   MinIO (Parquet)     │                    │
│                     │   Bucket: iot-output  │                   │
│                     └──────────────────────┘                    │
└──────────────────────────────────────────────────────────────────┘
```

| Komponente | Technologie | Rolle |
|---|---|---|
| Input Stream | `streaming_server.py` | Liest CSV, sendet Kafka-Messages |
| Ingestion Layer | Apache Kafka | Message Broker, Topic-Verwaltung |
| Master Dataset | `iot_sensor_data.csv` | Historische Rohdaten (Replay-Quelle) |
| Speed Layer | Apache Spark Structured Streaming | Stream-Verarbeitung + KMeans |
| Serving Layer | MinIO (S3-kompatibel) | Persistenter Parquet-Speicher |

---

## Projektstruktur

```
.
├── data/
│   └── iot_sensor_data.csv        # Synthetischer IoT-Datensatz (5.000 Einträge)
├── kafka/
│   ├── Dockerfile                 # Container-Image für Streaming Server
│   ├── streaming_server.py        # Kafka Producer: liest CSV → sendet Messages
│   ├── kafka_consumer.py          # Test-Consumer zur Verifikation
│   └── requirements.txt           # Python-Abhängigkeiten
├── spark/
│   └── spark_streaming_app.py     # Spark Streaming + KMeans → MinIO
├── k8s/
│   ├── rbac.yaml                  # Namespace + ServiceAccount (Spark)
│   ├── kafka.yaml                 # Kafka Deployment
│   ├── kafka-svc.yaml             # Kafka ClusterIP Service
│   ├── minio.yaml                 # MinIO Deployment + NodePort Service
│   ├── streaming-server.yaml      # Streaming Server Deployment
│   └── spark-job.yaml             # Spark Submit als K8s Job
├── docker-compose.yaml            # Hauptdeployment: alle Komponenten
├── LICENSE                        # Apache License 2.0 (Code)
└── LICENSE-DOCS                   # Creative Commons BY 4.0 (Dokumentation)
```

---

## IoT-Datensatz

**Datei:** `data/iot_sensor_data.csv` | **Einträge:** 5.000 | **Intervall:** 10 Sekunden

```
timestamp,sensor_id,sensor_type,location,value,unit,status
2024-01-01T00:00:00,sensor_009,temperature,warehouse_north,55.12,°C,OK
2024-01-01T00:00:10,sensor_001,co2,factory_floor_a,1126.14,ppm,WARNING
```

| Feld | Beschreibung | Werte |
|---|---|---|
| `timestamp` | Messzeitpunkt (ISO 8601) | 2024-01-01T00:00:00 |
| `sensor_id` | Eindeutige Sensor-ID | sensor_001 – sensor_020 |
| `sensor_type` | Messart | temperature, humidity, pressure, vibration, co2 |
| `location` | Standort | factory_floor_a/b, warehouse_north/south, server_room_1, control_room |
| `value` | Messwert | Float |
| `unit` | Einheit | °C, %, hPa, mm/s, ppm |
| `status` | Bewertung | OK / WARNING / ERROR |

---

## Schnellstart – Docker Compose (empfohlen)

```bash
# 1. Repository klonen
git clone https://github.com/<dein-user>/cloud_computing_big_data_abgabe.git
cd cloud_computing_big_data_abgabe

# 2. Alle Komponenten starten (Kafka, Streaming Server, Spark, MinIO)
docker compose up --build

# 3. MinIO Web-Console aufrufen
#    URL:      http://localhost:9001
#    Login:    minioadmin / minioadmin
#    Bucket:   iot-output/sensor-clusters/

# 4. Zum Stoppen
docker compose down
```

**Erwarteter Ablauf nach Start:**
1. Kafka-Broker startet und wird healthy
2. Streaming Server beginnt CSV-Zeilen an `iot-sensor-topic` zu senden
3. Spark konsumiert Topic, clustert Daten alle 30 Sekunden
4. Parquet-Dateien erscheinen in MinIO unter `iot-output/sensor-clusters/`

---

## KMeans-Clustering

Die Spark-Anwendung gruppiert IoT-Messdaten in **5 Cluster** basierend auf
`value` (Messwert) und `sensor_type` (kodiert als numerischer Index):

| Cluster-ID | Label | Bedeutung |
|---|---|---|
| 0 | `normal_operation` | Normaler Betrieb |
| 1 | `elevated_readings` | Erhöhte Messwerte |
| 2 | `critical_zone` | Kritischer Bereich (Handlungsbedarf) |
| 3 | `low_activity` | Geringe Sensoraktivität |
| 4 | `anomaly` | Anomalie / Ausreißer |

**Output-Schema (Parquet):**
```
timestamp      | sensor_id  | sensor_type | location        | value | unit | status  | cluster | cluster_label
2024-01-01 ... | sensor_009 | temperature | warehouse_north | 55.12 | °C   | OK      | 0       | normal_operation
```

---

## Schnellstart – Minikube (optional)

```bash
# Voraussetzung: minikube, kubectl installiert
minikube start --memory=6144 --cpus=4

# Alle K8s-Ressourcen deployen
kubectl apply -f k8s/rbac.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/kafka-svc.yaml
kubectl apply -f k8s/minio.yaml
kubectl apply -f k8s/spark-job.yaml

# Streaming Server (lokales Image bauen)
eval $(minikube docker-env)
docker build -t streaming-server:latest ./kafka/
kubectl apply -f k8s/streaming-server.yaml

# MinIO Console öffnen
minikube service minio-service -n kappa-iot
```

---

## Abhängigkeiten

| Technologie | Version |
|---|---|
| Apache Kafka | latest (apache/kafka) |
| Apache Spark | 3.5 (bitnami/spark) |
| MinIO | latest |
| Python | 3.11 |
| kafka-python | 2.0.2 |

---

## Lizenz

- **Quellcode:** [Apache License 2.0](LICENSE)
- **Dokumentation:** [Creative Commons Attribution 4.0](LICENSE-DOCS)
