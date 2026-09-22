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
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


ALTER TABLE rule_definitions
REPLICA IDENTITY FULL;

ALTER TABLE schema_definitions
REPLICA IDENTITY FULL;

-- ============================================================
-- Bảng cha: thông tin 1 Kafka cluster (bootstrap servers + bảo mật) cho Stream
-- ============================================================
CREATE TABLE IF NOT EXISTS kafka_stream_cluster_config (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,          -- định danh logic, khớp KafkaStream.streamId
    cluster_name VARCHAR(255) NOT NULL,       -- tên gợi nhớ, VD "kafka-plain", "kafka-gssapi"
    bootstrap_servers VARCHAR(500) NOT NULL,  -- "broker1:9092,broker2:9092"

    -- ---- Cấu hình bảo mật riêng theo cluster ----
    security_protocol VARCHAR(50) DEFAULT 'PLAINTEXT',  -- PLAINTEXT | SASL_PLAINTEXT | SASL_SSL
    sasl_mechanism VARCHAR(50),               -- GSSAPI (Kerberos) | PLAIN | SCRAM-SHA-256 ...
    sasl_kerberos_service_name VARCHAR(100),  -- thường là "kafka"
    sasl_jaas_config TEXT,                    -- toàn bộ chuỗi JAAS

    enabled BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (stream_id, cluster_name)
);

-- ============================================================
-- Bảng con: danh sách topic thuộc về 1 cluster (1-nhiều) cho Stream
-- ============================================================
CREATE TABLE IF NOT EXISTS kafka_stream_topic_config (
    id SERIAL PRIMARY KEY,
    cluster_config_id INTEGER NOT NULL REFERENCES kafka_stream_cluster_config(id) ON DELETE CASCADE,
    topic_name VARCHAR(255) NOT NULL,
    enabled BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE (cluster_config_id, topic_name)   -- tránh khai báo trùng topic trong cùng 1 cluster
);

-- ============================================================
-- Bảng cha & con cho Batch
-- ============================================================
CREATE TABLE IF NOT EXISTS kafka_batch_cluster_config (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    cluster_name VARCHAR(255) NOT NULL,
    bootstrap_servers VARCHAR(500) NOT NULL,
    security_protocol VARCHAR(50) DEFAULT 'PLAINTEXT',
    sasl_mechanism VARCHAR(50),
    sasl_kerberos_service_name VARCHAR(100),
    sasl_jaas_config TEXT,
    enabled BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (stream_id, cluster_name)
);

CREATE TABLE IF NOT EXISTS kafka_batch_topic_config (
    id SERIAL PRIMARY KEY,
    cluster_config_id INTEGER NOT NULL REFERENCES kafka_batch_cluster_config(id) ON DELETE CASCADE,
    topic_name VARCHAR(255) NOT NULL,
    enabled BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE (cluster_config_id, topic_name)
);

-- Dữ liệu mẫu ban đầu cho kafka_stream_cluster_config (CHỈ cấu hình cho event streams)
INSERT INTO kafka_stream_cluster_config (stream_id, cluster_name, bootstrap_servers, security_protocol, sasl_mechanism, sasl_kerberos_service_name, sasl_jaas_config, enabled)
VALUES 
(
    'stream-events',
    'kafka-gssapi',
    'kafka-gssapi:29094',
    'SASL_PLAINTEXT',
    'GSSAPI',
    'kafka',
    'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true doNotPrompt=true keyTab="/var/lib/secret/client.keytab" principal="client@EXAMPLE.COM";',
    TRUE
),
(
    'stream-events',
    'kafka-plain',
    'kafka-plain:29092',
    'SASL_PLAINTEXT',
    'PLAIN',
    NULL,
    'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";',
    TRUE
)
ON CONFLICT (stream_id, cluster_name) DO NOTHING;

-- Dữ liệu mẫu ban đầu cho kafka_stream_topic_config
-- crm thuộc cụm kafka-gssapi
INSERT INTO kafka_stream_topic_config (cluster_config_id, topic_name, enabled)
SELECT id, 'events_crm', TRUE FROM kafka_stream_cluster_config 
WHERE stream_id = 'stream-events' AND cluster_name = 'kafka-gssapi'
ON CONFLICT (cluster_config_id, topic_name) DO NOTHING;

-- ecommerce và payment thuộc cụm kafka-plain
INSERT INTO kafka_stream_topic_config (cluster_config_id, topic_name, enabled)
SELECT id, 'events_ecommerce', TRUE FROM kafka_stream_cluster_config 
WHERE stream_id = 'stream-events' AND cluster_name = 'kafka-plain'
ON CONFLICT (cluster_config_id, topic_name) DO NOTHING;

INSERT INTO kafka_stream_topic_config (cluster_config_id, topic_name, enabled)
SELECT id, 'events_payment', TRUE FROM kafka_stream_cluster_config 
WHERE stream_id = 'stream-events' AND cluster_name = 'kafka-plain'
ON CONFLICT (cluster_config_id, topic_name) DO NOTHING;

GRANT USAGE ON SCHEMA public TO replicator;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO replicator;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO postgres;
GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA public TO replicator, postgres;

CREATE PUBLICATION realtime_publication
FOR TABLE
    public.rule_definitions,
    public.schema_definitions;
