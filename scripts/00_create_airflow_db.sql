-- Create a SEPARATE database for Airflow's own metadata, so it never mixes
-- with our analytics tables (which live in the POSTGRES_DB = air_quality).
-- Runs before 01/init scripts because of the "00_" filename prefix.
SELECT 'CREATE DATABASE airflow_meta'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_meta')\gexec
