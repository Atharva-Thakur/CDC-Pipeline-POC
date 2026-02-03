FROM quay.io/debezium/postgres:15

USER root
RUN apt-get update && apt-get install -y postgresql-15-wal2json && rm -rf /var/lib/apt/lists/*
USER postgres
