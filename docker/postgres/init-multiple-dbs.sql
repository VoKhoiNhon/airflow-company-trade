-- Create the first user and database
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

CREATE USER openmetadata_user WITH PASSWORD 'openmetadata_password';
CREATE DATABASE openmetadata_db OWNER openmetadata_user;

-- Grant all privileges on the databases to their respective users
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO openmetadata_user;

