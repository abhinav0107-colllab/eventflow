#!/bin/sh
# Runs once, on first initialisation of the PostgreSQL data volume.
#
# Roles:
#   $POSTGRES_USER   - bootstrap superuser and schema owner; used only by the
#                      migrate job.
#   eventflow_app    - least-privilege runtime role used by the API and the
#                      workers: DML on application tables only, no DDL.
set -eu
: "${EVENTFLOW_APP_DB_PASSWORD:?EVENTFLOW_APP_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v app_password="$EVENTFLOW_APP_DB_PASSWORD" <<'SQL'
CREATE ROLE eventflow_app LOGIN PASSWORD :'app_password' NOSUPERUSER NOCREATEDB NOCREATEROLE;
REVOKE ALL ON DATABASE :"DBNAME" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"DBNAME" TO eventflow_app;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO eventflow_app;
-- Tables created by future migrations are usable by the runtime role.
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO eventflow_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO eventflow_app;
SQL
