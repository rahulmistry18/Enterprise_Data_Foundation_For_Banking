-- Create the Airflow user if it does not exist
DO
$do$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = 'airflow') THEN

      CREATE USER airflow WITH PASSWORD 'airflow';
   END IF;
END
$do$;

-- Create the Airflow database if it does not exist and assign ownership
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec