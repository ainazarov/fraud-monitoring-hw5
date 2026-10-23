ARG BASE_IMAGE=apache/airflow:2.10.5-python3.11
FROM ${BASE_IMAGE}
USER root
RUN mkdir -p /opt/project/storage /opt/project/data && chown -R airflow:0 /opt/project && chmod -R g+rwX /opt/project
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.5" -r /tmp/requirements.txt && pip check
WORKDIR /opt/project
COPY --chown=airflow:0 . /opt/project
ENV PYTHONPATH=/opt/project STORAGE_DIR=/opt/project/storage PYTHONUNBUFFERED=1
