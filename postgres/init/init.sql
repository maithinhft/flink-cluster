DO $$
BEGIN
  IF NOT EXISTS (
    SELECT FROM pg_roles
    WHERE rolname = 'replicator'
  ) THEN
    CREATE ROLE replicator
      WITH LOGIN
      REPLICATION
      PASSWORD 'replicatorpassword';
  END IF;
END
$$;

GRANT CONNECT ON DATABASE realtime_core TO replicator;

\c realtime_core

CREATE TABLE IF NOT EXISTS rule_definitions (
    id BIGSERIAL PRIMARY KEY,

    rule_id UUID NOT NULL,

    name VARCHAR(255) NOT NULL,

    rule_json JSONB NOT NULL,

    priority INTEGER NOT NULL DEFAULT 0,

    cooldown_seconds BIGINT NOT NULL DEFAULT 0,

    version BIGINT NOT NULL,

    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    user_id VARCHAR(255) NOT NULL DEFAULT 'system',

    CONSTRAINT uq_rule_version
        UNIQUE (rule_id, version)
);

CREATE TABLE IF NOT EXISTS schema_definitions (
    schema_id VARCHAR(255) PRIMARY KEY,
    schema_payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE rule_definitions
REPLICA IDENTITY FULL;

ALTER TABLE schema_definitions
REPLICA IDENTITY FULL;

GRANT USAGE ON SCHEMA public TO replicator;

GRANT SELECT ON
    public.rule_definitions,
    public.schema_definitions
TO replicator;

GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO replicator;


CREATE PUBLICATION realtime_publication
FOR TABLE
    public.rule_definitions,
    public.schema_definitions;
