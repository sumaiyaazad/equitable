# Flink session cluster for the scored Flink cells and D5 (scripts/run/run_d5_recovery.py).
#
# Same Flink release as the pinned PyFlink (2.2.1), so moving from the in-process
# MiniCluster to a JobManager/TaskManager deployment changes the deployment and
# not the engine version. The Kafka SQL connector is the release built for
# Flink 2.2 and is checked against the SHA-256 of the Maven Central artifact
# (its published SHA-1 is 7bf2ded1144cb8aece05adcaccbc81b2ac3ef339).
FROM flink:2.2.1-scala_2.12-java17@sha256:13b1328bbb86263b41e740f06790f49c8512a26a5c0eba36a8f91b1983d33a6c

# ADD --checksum makes the build fail on any byte difference.
ADD --checksum=sha256:5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616 \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
    /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar
USER root
RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka-5.0.0-2.2.jar \
 && mkdir -p /checkpoints && chown flink:flink /checkpoints

# A named volume copies this directory's ownership on first creation, so the
# JobManager and TaskManager (both uid flink) can write retained checkpoints.
VOLUME /checkpoints
