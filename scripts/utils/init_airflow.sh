#!/bin/bash
# Initialize Airflow Database and Create Admin User

echo "Running Airflow database migration..."
airflow db migrate

echo "Creating Airflow Admin User..."
airflow users create \
    --username admin \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spidey@dailybugle.com \
    --password admin

echo "Creating Airflow Connections (if needed)..."
# Example connection setup (you can add your specific Postgres connection here)
# Since you use AIRFLOW__CORE__SQL_ALCHEMY_CONN, this may not be strictly necessary,
# but it's good practice for creating other connections.
# airflow connections add 'postgres_banking_dw' --conn-uri 'postgresql://airflow:airflow@postgres:5432/airflow'

echo "Airflow initialization complete."