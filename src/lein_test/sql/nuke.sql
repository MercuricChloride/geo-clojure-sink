DO $$ DECLARE
    r RECORD;
BEGIN
    -- drop all sequences
  --FOR r IN (SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema='public') LOOP
  --    EXECUTE 'DROP SEQUENCE IF EXISTS ' || r.sequence_name || ' CASCADE';
  --END LOOP;

    -- drop all tables
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname='public') LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || r.tablename || ' CASCADE';
    END LOOP;

  ---- drop all views
  --FOR r IN (SELECT table_name FROM information_schema.views WHERE table_schema='public') LOOP
  --    EXECUTE 'DROP VIEW IF EXISTS ' || r.table_name || ' CASCADE';
  --END LOOP;

  ---- drop all materialized views
  --FOR r IN (SELECT matviewname FROM pg_matviews WHERE schemaname='public') LOOP
  --    EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS ' || r.matviewname || ' CASCADE';
  --END LOOP;

    -- drop all functions
    FOR r IN (SELECT proname FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')) LOOP
        EXECUTE 'DROP FUNCTION IF EXISTS ' || r.proname || ' CASCADE';
    END LOOP;

    -- drop all types
    FOR r IN (SELECT typname FROM pg_type WHERE typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')) LOOP
        EXECUTE 'DROP TYPE IF EXISTS ' || r.typname || ' CASCADE';
    END LOOP;

END $$;
