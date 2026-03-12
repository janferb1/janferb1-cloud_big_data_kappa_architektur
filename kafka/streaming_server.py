import os
import time
from kafka import KafkaProducer

# ---------------------------------------------------------------
# IoT Sensor Streaming Server
# Reads iot_sensor_data.csv line by line and sends each row
# as a Kafka message to the configured topic.
# Loops indefinitely to simulate a continuous data stream.
# ---------------------------------------------------------------

FILE_PATH    = os.getenv("FILE_PATH",    "data/iot_sensor_data.csv")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC",  "iot-sensor-topic")
SEND_DELAY   = float(os.getenv("SEND_DELAY", "0.1"))  # seconds between messages


def start_server():
    print(f"[StreamingServer] Connecting to Kafka at {KAFKA_BROKER} ...")

    # Retry until Kafka is ready
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                retries=5
            )
            print("[StreamingServer] Connected to Kafka.")
        except Exception as e:
            print(f"[StreamingServer] Kafka not ready yet: {e} – retrying in 3s")
            time.sleep(3)

    loop = 0
    while True:
        loop += 1
        print(f"[StreamingServer] Starting loop #{loop} over {FILE_PATH}")
        try:
            with open(FILE_PATH, "r") as f:
                f.readline()  # skip CSV header
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    producer.send(KAFKA_TOPIC, value=line.encode("utf-8"))
                    print(f"[StreamingServer] Sent → {line[:80]}")
                    time.sleep(SEND_DELAY)
            producer.flush()
        except FileNotFoundError:
            print(f"[StreamingServer] ERROR: {FILE_PATH} not found!")
            break


if __name__ == "__main__":
    start_server()
