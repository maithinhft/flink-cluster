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

PLAIN_TOPICS=(
  "rule_definitions:6"
  "debezium_heartbeat:1"
  "events_ecommerce:6"
  "events_payment:6"
  "result:6"
  "dlq:6"
)

for item in "${PLAIN_TOPICS[@]}"; do
  TOPIC="${item%%:*}"
  PARTITIONS="${item##*:}"
  echo "👉 Tạo topic '$TOPIC' ($PARTITIONS partitions)..."
  docker compose exec kafka-plain /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-plain:29092 \
    --command-config /etc/kafka/secrets/client.properties \
    --topic "$TOPIC" \
    --create \
    --if-not-exists \
    --partitions "$PARTITIONS" \
    --replication-factor 1
done

echo ""
echo "=================================================="
echo "2. Tạo topics trên cụm KAFKA-GSSAPI (Port 9094/29094)"
echo "=================================================="

GSSAPI_TOPICS=(
  "schema_registry:6"
  "events_crm:6"
  "debezium_heartbeat:1"
)

for item in "${GSSAPI_TOPICS[@]}"; do
  TOPIC="${item%%:*}"
  PARTITIONS="${item##*:}"
  echo "👉 Tạo topic '$TOPIC' ($PARTITIONS partitions)..."
  docker compose exec kafka-gssapi /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-gssapi:29094 \
    --command-config /etc/kafka/secrets/client.properties \
    --topic "$TOPIC" \
    --create \
    --if-not-exists \
    --partitions "$PARTITIONS" \
    --replication-factor 1
done

echo ""
echo "✅ Hoàn tất khởi tạo topic trên cả 2 cụm Kafka!"