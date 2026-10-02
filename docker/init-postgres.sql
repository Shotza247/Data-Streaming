-- docker/init-postgres.sql
--
-- PostgreSQL initialisation script for the Tax Analytics Platform.
-- Runs inside the postgres container on first startup as the POSTGRES_USER
-- (taxuser — set via POSTGRES_USER env var).
--
-- Creates:
--   Databases : marquez, mlflow  (taxdb created automatically by POSTGRES_DB env var)
--   Roles     : marquez (owns marquez db), taxuser already exists as superuser
--
-- NOTE: taxuser is configured as POSTGRES_USER so it has SUPERUSER rights,
-- which means this script can create other databases and roles.

-- ── Create marquez database ───────────────────────────────────────────────────
CREATE DATABASE marquez;

-- ── Create mlflow database ────────────────────────────────────────────────────
CREATE DATABASE mlflow;

-- ── Create marquez role for the Marquez API ───────────────────────────────────
-- Marquez connects with username=marquez, password=marquez
CREATE ROLE marquez WITH LOGIN PASSWORD 'marquez' SUPERUSER;

-- ── Grant full access ─────────────────────────────────────────────────────────
GRANT ALL PRIVILEGES ON DATABASE marquez TO marquez;
GRANT ALL PRIVILEGES ON DATABASE taxdb  TO taxuser;
GRANT ALL PRIVILEGES ON DATABASE mlflow TO taxuser;
