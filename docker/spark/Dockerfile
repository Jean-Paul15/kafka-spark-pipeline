# Image Spark commune (master, worker, jobs de streaming) avec client Redis.
FROM apache/spark:3.5.8

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir redis==5.0.1

COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
