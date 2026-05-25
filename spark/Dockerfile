FROM apache/spark:3.5.1-python3

USER root

RUN pip install --no-cache-dir psycopg2-binary pandas

RUN mkdir -p /opt/spark/jars-extra \
    && curl -fsSL -o /opt/spark/jars-extra/postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
    && curl -fsSL -o /opt/spark/jars-extra/clickhouse-jdbc-0.6.5-all.jar \
        https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.5/clickhouse-jdbc-0.6.5-all.jar

ENV SPARK_JARS=/opt/spark/jars-extra/postgresql-42.7.3.jar,/opt/spark/jars-extra/clickhouse-jdbc-0.6.5-all.jar
ENV PYTHONPATH=/opt/spark/work-dir

WORKDIR /opt/spark/work-dir
COPY . .

USER spark

CMD ["tail", "-f", "/dev/null"]
