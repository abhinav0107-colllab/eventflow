DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'eventflow_app') THEN
        REVOKE ALL ON jobs, job_attempts, outbox_events, workers, api_keys, schema_migrations FROM eventflow_app;
        REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM eventflow_app;
    END IF;
END
$$;
