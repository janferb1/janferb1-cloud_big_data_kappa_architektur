# Dockerfile für den IoT Streaming Server (Kafka Producer)
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY streaming_server.py .
CMD ["python", "streaming_server.py"]
