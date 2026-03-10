"""
streaming_server.py – IoT Sensor Streaming Server (Kafka Producer)
===================================================================
Liest die IoT-Sensor-CSV-Datei und sendet jede Zeile als Message
an das konfigurierte Kafka-Topic. Implementiert den "Master Dataset
Replay"-Mechanismus der Kappa-Architektur: Nach dem Ende der Datei
wird von vorne begonnen, um einen kontinuierlichen Datenstrom zu
simulieren.

Kappa-Architektur Rolle: Input Stream → Data Ingestion Layer
"""

import os
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Logging-Konfiguration ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Konfiguration via Umgebungsvariablen ──────────────────────────────────────
FILE_PATH    = os.getenv("FILE_PATH",    "data/iot_sensor_data.csv")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC        = os.getenv("KAFKA_TOPIC",  "iot-sensor-topic")
DELAY        = float(os.getenv("SEND_DELAY", "0.1"))  # Sekunden zwischen Messages


def create_producer(retries: int = 10, wait: int = 5) -> KafkaProducer:
    """Erstellt einen KafkaProducer mit Retry-Logik beim Start."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
            log.info(f"Verbunden mit Kafka-Broker: {KAFKA_BROKER}")
            return producer
        except NoBrokersAvailable:
            log.warning(f"Broker nicht erreichbar (Versuch {attempt}/{retries}). Warte {wait}s...")
            time.sleep(wait)
    raise RuntimeError("Kafka-Broker nicht erreichbar nach mehreren Versuchen.")


def stream_file(producer: KafkaProducer) -> None:
    """Liest CSV-Datei Zeile für Zeile und sendet jede Zeile an Kafka."""
    with open(FILE_PATH, "r") as f:
        f.readline()  # Header-Zeile überspringen
        for line in f:
            line = line.strip()
            if not line:
                continue
            producer.send(TOPIC, value=line.encode("utf-8"))
            log.info(f"Gesendet → {TOPIC}: {line}")
            time.sleep(DELAY)
    producer.flush()


def start_server() -> None:
    """Hauptschleife: Streamt CSV wiederholt an Kafka (Master Dataset Replay)."""
    log.info(f"Starte IoT Streaming Server | Datei: {FILE_PATH} | Topic: {TOPIC}")
    producer = create_producer()
    cycle = 1
    while True:
        log.info(f"--- Durchlauf {cycle} gestartet ---")
        stream_file(producer)
        log.info(f"--- Durchlauf {cycle} abgeschlossen. Starte neu (Replay)... ---")
        cycle += 1


if __name__ == "__main__":
    start_server()
