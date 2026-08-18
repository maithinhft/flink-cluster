CREATE TABLE aggregation_definitions (
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

    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE rule_definitions (
    id BIGSERIAL PRIMARY KEY,

    rule_id UUID NOT NULL,

    name VARCHAR(255) NOT NULL,

    rule_json JSONB NOT NULL,

    version BIGINT NOT NULL,

    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
