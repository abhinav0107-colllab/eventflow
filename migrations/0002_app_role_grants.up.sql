-- Least-privilege runtime role. The role itself is created by the database
-- bootstrap (deploy/postgres/init-roles.sh); migrations run as the schema
-- owner and only grant DML. When the role does not exist (e.g. in tests that
-- connect as a superuser) this migration is a no-op.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventflow_app') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE
            ON jobs, job_attempts, outbox_events, workers TO eventflow_app;
        GRANT SELECT, INSERT, UPDATE ON api_keys TO eventflow_app;
        GRANT SELECT ON schema_migrations TO eventflow_app;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO eventflow_app;
    END IF;
END
$$;
