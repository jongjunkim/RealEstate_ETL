FROM apache/airflow:2.10.2-python3.10

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    nano vim curl netcat-openbsd iputils-ping && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/airflow
