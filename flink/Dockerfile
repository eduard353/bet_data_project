FROM flink:1.17.2-scala_2.12-java11

USER root

# Ставим Python
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Делаем симлинк python -> python3
RUN ln -s /usr/bin/python3 /usr/bin/python

# Устанавливаем PyFlink + драйвер PostgreSQL
RUN pip3 install apache-flink==1.17.2 psycopg2-binary

WORKDIR /opt/flink/jobs

COPY jobs/ ./jobs/
# Подключаем Kafka коннектор 
COPY artifacts/flink-sql-connector-kafka-1.17.2.jar /opt/flink/lib/

# Подключаем PostgreSQL драйвер 
COPY artifacts/postgresql-42.7.3.jar /opt/flink/lib/

CMD ["bash", "-c", "echo 'Flink container ready. Submit jobs manually via Flink UI or CLI.' && tail -f /dev/null"]
