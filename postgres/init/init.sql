
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


CREATE TABLE IF NOT EXISTS aggregation_definitions (
    id BIGSERIAL PRIMARY KEY,

    aggregation_id UUID NOT NULL,

    name VARCHAR(255) NOT NULL,

    entity_type VARCHAR(100) NOT NULL,

    field_name VARCHAR(255),

    aggregation_type VARCHAR(20) NOT NULL,

    window_size_seconds BIGINT NOT NULL,

    slide_seconds BIGINT NOT NULL,

    version BIGINT NOT NULL,

    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_aggregation_version
        UNIQUE (aggregation_id, version)
);

ALTER TABLE aggregation_definitions
REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS rule_definitions (
    id BIGSERIAL PRIMARY KEY,

    rule_id UUID NOT NULL,

    name VARCHAR(255) NOT NULL,

    rule_json JSONB NOT NULL,

    version BIGINT NOT NULL,

    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_rule_version
        UNIQUE (rule_id, version)
);


ALTER TABLE rule_definitions
REPLICA IDENTITY FULL;

GRANT USAGE ON SCHEMA public TO replicator;

GRANT SELECT ON
    public.aggregation_definitions,
    public.rule_definitions
TO replicator;

GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO replicator;


CREATE PUBLICATION realtime_publication
FOR TABLE
    public.aggregation_definitions,
    public.rule_definitions;

