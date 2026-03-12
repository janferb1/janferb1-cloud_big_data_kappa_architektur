# Author: janferb1
# Based on: https://github.com/sturc/cloud_computing_big_data
#
# Custom Spark image with all required Python packages
# pre-installed. Built on top of the official Apache Spark image.

FROM apache/spark:3.5.0

USER 0

# Install pip and Python packages (numpy, pyspark, kafka, etc.)
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install --no-cache-dir \
        numpy==1.26.4 \
        pandas==2.2.3 \
        pyarrow==14.0.1 \
        kafka-python==2.0.2 \
        python-dateutil==2.8.2 \
        pytz==2024.2

# Copy our Spark streaming application into the image
COPY spark_streaming_app.py /opt/spark/work-dir/spark_streaming_app.py

ARG spark_uid=185
USER ${spark_uid}
