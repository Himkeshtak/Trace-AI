FROM apache/airflow:2.10.5-python3.12

RUN pip install --no-cache-dir apache-airflow-providers-postgres==5.14.0 redis==5.2.1

COPY --chmod=0755 unpause-dag.sh /usr/local/bin/traceai-unpause-dag
