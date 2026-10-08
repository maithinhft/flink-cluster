#!/usr/bin/env bash
# =============================================================================
# scripts/create-kafka-topics.sh
# Tạo các topic cần thiết trên 2 cụm Kafka:
# - kafka-plain (SASL_PLAINTEXT / PLAIN)
# - kafka-gssapi (SASL_PLAINTEXT / GSSAPI Kerberos)
# =============================================================================

set -euo pipefail

echo "=================================================="
echo "1. Tạo topics trên cụm KAFKA-PLAIN (Port 9092/29092)"
echo "=================================================="

# Format: "name:partitions[:config1,config2,...]"
# Control topics (rule/schema CDC) use 1 partition for a total order and log compaction so the
# latest definition of every key is retained forever (restart without savepoint replays them).
COMPACT="cleanup.policy=compact,min.compaction.lag.ms=60000"

PLAIN_TOPICS=(
  "rule_definitions:1:${COMPACT}"
  "debezium_heartbeat:1"
  "events_ecommerce:12"
  "events_payment:12"
  "result:12"
  "dlq:6"
)

topic_configs() {
  local cfgs="$1" out=()
  [ -z "$cfgs" ] && return 0
  IFS=',' read -ra parts <<< "$cfgs"
  for c in "${parts[@]}"; do out+=(--config "$c"); done
  printf '%s\n' "${out[@]}"
}

for item in "${PLAIN_TOPICS[@]}"; do
  IFS=':' read -r TOPIC PARTITIONS CONFIGS <<< "$item"
  mapfile -t EXTRA < <(topic_configs "${CONFIGS:-}")
  echo "👉 Tạo topic '$TOPIC' ($PARTITIONS partitions)..."
  docker compose exec kafka-plain /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-plain:29092 \
    --command-config /etc/kafka/secrets/client.properties \
    --topic "$TOPIC" \
    --create \
    --if-not-exists \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    "${EXTRA[@]}"
done

echo ""
echo "=================================================="
echo "2. Tạo topics trên cụm KAFKA-GSSAPI (Port 9094/29094)"
echo "=================================================="

GSSAPI_TOPICS=(
  "schema_registry:1:${COMPACT}"
  "events_crm:12"
  "debezium_heartbeat:1"
)

for item in "${GSSAPI_TOPICS[@]}"; do
  IFS=':' read -r TOPIC PARTITIONS CONFIGS <<< "$item"
  mapfile -t EXTRA < <(topic_configs "${CONFIGS:-}")
  echo "👉 Tạo topic '$TOPIC' ($PARTITIONS partitions)..."
  docker compose exec kafka-gssapi /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-gssapi:29094 \
    --command-config /etc/kafka/secrets/client.properties \
    --topic "$TOPIC" \
    --create \
    --if-not-exists \
    --partitions "$PARTITIONS" \
    --replication-factor 1 \
    "${EXTRA[@]}"
done

echo ""
echo "✅ Hoàn tất khởi tạo topic trên cả 2 cụm Kafka!"