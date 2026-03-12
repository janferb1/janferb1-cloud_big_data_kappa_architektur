# Author: janferb1
# Streaming Server – sends IoT sensor CSV rows to Kafka

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY streaming_server.py .
# CSV data is copied into the image so the container is self-contained
COPY iot_sensor_data.csv data/iot_sensor_data.csv

CMD ["python", "streaming_server.py"]
