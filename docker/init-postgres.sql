-- docker/init-postgres.sql
--
-- Initialisation script for the shared PostgreSQL instance.
-- Creates the three logical databases required by the platform and
-- provisions the necessary database users.
--
-- This script runs inside the postgres Docker container on first startup.
-- The default `taxdb` database is created via the POSTGRES_DB env var, but
-- we also create it here conditionally for idempotency (e.g. when mounting
-- this script into an existing volume).

-- Create marquez database if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'marquez') THEN
        CREATE DATABASE marquez;
    END IF;
END
$$;

-- Create mlflow database if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow') THEN
        CREATE DATABASE mlflow;
    END IF;
END
$$;

-- Create taxdb database if it doesn't exist (POSTGRES_DB already creates it,
-- but this block makes the script idempotent for re-runs)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'taxdb') THEN
        CREATE DATABASE taxdb;
    END IF;
END
$$;

-- Create the tax-domain user with password (or update password if exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'taxuser') THEN
        CREATE ROLE taxuser LOGIN PASSWORD 'taxpass';
    ELSE
        ALTER ROLE taxuser LOGIN PASSWORD 'taxpass';
    END IF;
END
$$;

-- Create the marquez role (Marquez default dev config connects as this user)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'marquez') THEN
        CREATE ROLE marquez LOGIN PASSWORD 'marquez' SUPERUSER;
    ELSE
        ALTER ROLE marquez LOGIN PASSWORD 'marquez' SUPERUSER;
    END IF;
END
$$;

-- Grant privileges on the marquez database to marquez user
GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;

-- Grant privileges on the taxdb database to taxuser
GRANT ALL PRIVILEGES ON DATABASE taxdb TO taxuser;
