
# Author: janferb1
# Based on: https://github.com/sturc/cloud_computing_big_data
#
# Custom Spark image with all required Python packages pre-installed.
 
FROM apache/spark:3.5.0
 
USER 0
 
# Upgrade pip first so it can find modern package versions
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install --upgrade pip && \
    pip3 install --no-cache-dir \
        numpy==1.24.4 \
        pandas==2.0.3 \
        pyarrow==12.0.1 \
        kafka-python==2.0.2 \
        python-dateutil==2.8.2 \
        pytz==2024.1
 
# Copy Spark streaming application into the image
COPY spark_streaming_app.py /opt/spark/work-dir/spark_streaming_app.py
 
ARG spark_uid=185
USER ${spark_uid}
