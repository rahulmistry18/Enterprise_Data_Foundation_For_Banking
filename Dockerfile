# In file: enterprise_data_foundation_for_banking/Dockerfile

# Base official Airflow image
FROM apache/airflow:2.8.0-python3.11

# Install necessary system packages for database connectors and Spark
USER root
RUN apt-get update -qq && apt-get install -y --no-install-recommends \
        # PostgreSQL Client needed for Airflow Postgres Provider and bulk COPY
        postgresql-client \
        # OpenJDK needed to run PySpark
        openjdk-17-jdk  \
    && apt-get clean

USER airflow
# Install Python libraries needed for the DAG and transformations
RUN pip install --no-cache-dir \
    # Airflow Provider to connect to Postgres
    apache-airflow-providers-postgres \
    # PySpark and Pandas for the transformation job
    pyspark \
    pandas \
    # Psycopg2-binary for direct connection/hook within PythonOperators
    psycopg2-binary