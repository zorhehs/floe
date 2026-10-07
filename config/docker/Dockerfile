# One base image (Spark 3.5 + Iceberg + Kafka connector) with three targets so
# the layers are shared on disk:
#   spark   - Structured Streaming CDC ingestion job
#   tools   - loader, workload generator, benchmark, validation (Python)
#   airflow - Airflow scheduler/web UI that runs Spark maintenance jobs

ARG SPARK_IMAGE=apache/spark:3.5.6-scala2.12-java17-python3-ubuntu

# ---------------------------------------------------------------------------
FROM ${SPARK_IMAGE} AS spark
USER root

# Iceberg + Kafka jars are pre-downloaded and checksum-verified by
# scripts/fetch-jars.sh (robust against flaky networks).
COPY --chmod=644 config/docker/jars/*.jar /opt/spark/jars/
COPY config/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY config/spark/log4j2.properties config/spark/metrics.properties /opt/spark/conf/

RUN mkdir -p /opt/jobs /opt/checkpoints /opt/spark/work-dir \
 && chown -R spark:spark /opt/checkpoints /opt/spark/work-dir

ENV PATH="/opt/spark/bin:${PATH}" \
    PYTHONUNBUFFERED=1
WORKDIR /opt/spark/work-dir
USER spark

# ---------------------------------------------------------------------------
FROM spark AS tools
USER root
# Every shared dependency is resolved against Airflow's constraint file (vendored
# for reproducible offline-friendly builds) so the airflow target adds Airflow
# on top without any version conflicts.
COPY config/docker/requirements-tools.txt config/docker/constraints-airflow-2.10.5-py3.10.txt /tmp/
RUN pip3 install --no-cache-dir --retries 20 --timeout 60 -r /tmp/requirements-tools.txt \
      --constraint /tmp/constraints-airflow-2.10.5-py3.10.txt
COPY --chown=spark:spark src /opt/src
ENV PYTHONPATH=/opt/src
WORKDIR /opt/src
USER spark

# ---------------------------------------------------------------------------
FROM tools AS airflow
USER root
ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_MM=3.10
ENV AIRFLOW_HOME=/opt/airflow
RUN pip3 install --no-cache-dir --retries 20 --timeout 60 "apache-airflow==${AIRFLOW_VERSION}" \
      --constraint /tmp/constraints-airflow-2.10.5-py3.10.txt \
 && mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/state \
 && chown -R spark:spark /opt/airflow
COPY --chmod=755 config/docker/airflow-entrypoint.sh /opt/airflow-entrypoint.sh
WORKDIR /opt/airflow
USER spark
ENTRYPOINT ["/opt/airflow-entrypoint.sh"]
