# Use the official Airflow image as the base
FROM apache/airflow:2.7.0

# Install the Docker provider for Airflow
RUN pip install apache-airflow-providers-docker

USER airflow