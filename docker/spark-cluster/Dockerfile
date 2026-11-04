# Spark 4.2.0 standalone cluster: master, worker, and the long-lived driver
# service the scored Spark adapter talks to (src/adapters/spark_cluster.py).
#
# One image for all three roles, so the driver and the executors run the same
# Python and the same PySpark as the pinned in-process adapter.
FROM python:3.11-slim@sha256:9e1912aab0a30bbd9488eb79063f68f42a68ab0946cbe98fecf197fe5b085506

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==4.2.0 pandas==2.2.3 pyarrow

ENV PYSPARK_PYTHON=python \
    PYTHONUNBUFFERED=1 \
    SPARK_NO_DAEMONIZE=1

COPY driver_service.py /opt/driver_service.py
