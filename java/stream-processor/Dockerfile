FROM flink:1.20-java17

RUN mkdir -p /tmp/flink-checkpoints /tmp/flink-savepoints && \
    chown -R flink:flink /tmp/flink-checkpoints /tmp/flink-savepoints

COPY target/stream-processor-0.1.0-SNAPSHOT.jar /opt/flink/usrlib/stream-processor.jar
