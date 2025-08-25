# Dockerfile
FROM apache/airflow:2.9.3

# Use the airflow user (already created in base image)
USER airflow

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install Airflow-related extras under Airflow constraints
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt --constraint "${CONSTRAINT_URL}"

# Install dbt separately with versions that are compatible with protobuf<5
# (so we don't fight Airflow’s constraints)
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dbt.txt
