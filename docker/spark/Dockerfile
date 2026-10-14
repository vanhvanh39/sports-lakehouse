# Spark 3.5.3 + Delta Lake 3.2.1 + Kafka + Avro + S3A (MinIO).
# Các thư viện JAR được tải sẵn khi build để cụm chạy không cần Internet.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends curl python3-pip procps; rm -rf /var/lib/apt/lists/*; \
    cd /opt/spark/jars; \
    for j in \
      io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
      io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      org/apache/spark/spark-avro_2.12/3.5.3/spark-avro_2.12-3.5.3.jar \
      org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    ; do curl -fsSL --retry 5 -o "$(basename "$j")" "$MAVEN/$j"; done; \
    ls -la /opt/spark/jars | grep -E 'delta|kafka|avro|aws'

COPY docker/spark/requirements.txt /tmp/requirements.txt
# --no-deps: không cài pyspark từ PyPI (đã có sẵn trong /opt/spark/python)
RUN pip3 install --no-cache-dir --no-deps -r /tmp/requirements.txt \
    && pip3 install --no-cache-dir elastic-transport==8.15.1 urllib3 certifi charset-normalizer idna zipp

ENV APP_ROOT=/opt/app \
    PYTHONPATH=/opt/app/src \
    DATA_DIR=/opt/app/data \
    SCHEMA_DIR=/opt/app/schemas \
    CHECKPOINT_ROOT=/opt/checkpoints \
    PYSPARK_PYTHON=python3 \
    SPARK_NO_DAEMONIZE=true

COPY schemas /opt/app/schemas
COPY src /opt/app/src
COPY docker/spark/run-pipeline.sh /opt/app/run-pipeline.sh
RUN mkdir -p /opt/checkpoints /opt/app/data /opt/spark/work && sed -i "s/\r$//" /opt/app/run-pipeline.sh && chmod +x /opt/app/run-pipeline.sh

WORKDIR /opt/app
