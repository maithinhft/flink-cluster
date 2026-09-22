#!/usr/bin/env bash
# ==============================================================================
# Script sinh và gửi dữ liệu sự kiện CRM vào topic events_crm trên cụm kafka-plain
# Sử dụng xác thực SASL_PLAINTEXT (PLAIN: admin / admin-secret)
# ==============================================================================

set -euo pipefail

NUM_EVENTS="${1:-10}"
TOPIC_NAME="events_crm"
CONTAINER_NAME="kafka-plain"
CLIENT_CONFIG="/etc/kafka/secrets/client.properties"

echo "======================================================================"
echo "[INFO] Đang chuẩn bị gửi ${NUM_EVENTS} sự kiện CRM vào topic '${TOPIC_NAME}' trên '${CONTAINER_NAME}'..."
echo "======================================================================"

# 1. Đảm bảo topic events_crm tồn tại trên kafka-plain (sử dụng --command-config với user admin/admin-secret)
echo "[INFO] 1. Kiểm tra / Khởi tạo topic ${TOPIC_NAME} trên ${CONTAINER_NAME}..."
docker exec "${CONTAINER_NAME}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --command-config "${CLIENT_CONFIG}" \
  --create --if-not-exists \
  --topic "${TOPIC_NAME}" \
  --partitions 6 \
  --replication-factor 1

# 2. Sinh dữ liệu JSON giả lập CRM và pipe trực tiếp vào kafka-console-producer kèm --producer.config
echo "[INFO] 2. Bắt đầu sinh và gửi ${NUM_EVENTS} bản ghi JSON (xác thực SASL PLAIN)..."

EVENT_TYPES=("login" "account_created" "profile_update" "subscription_change" "support_ticket_created")

for ((i = 1; i <= NUM_EVENTS; i++)); do
    RAND_TYPE_IDX=$((RANDOM % ${#EVENT_TYPES[@]}))
    EVENT_TYPE="${EVENT_TYPES[$RAND_TYPE_IDX]}"
    ENTITY_ID="user_$((100 + RANDOM % 900))"
    EVENT_ID="crm-evt-$(date +%s%N)-$i"
    NOW_ISO=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    case "$EVENT_TYPE" in
        "login")
            DATA_JSON="{\"channel\":\"web\",\"status\":\"success\",\"ip_address\":\"192.168.1.$((RANDOM % 255))\"}"
            ;;
        "account_created")
            DATA_JSON="{\"account_tier\":\"standard\",\"referral_code\":\"REF$((RANDOM % 1000))\",\"country\":\"VN\"}"
            ;;
        "profile_update")
            DATA_JSON="{\"updated_field\":\"phone_number\",\"status\":\"completed\"}"
            ;;
        "subscription_change")
            DATA_JSON="{\"old_tier\":\"standard\",\"new_tier\":\"premium\",\"billing_cycle\":\"monthly\"}"
            ;;
        "support_ticket_created")
            DATA_JSON="{\"ticket_id\":\"TCK-$((RANDOM % 10000))\",\"priority\":\"medium\",\"category\":\"billing\"}"
            ;;
    esac

    JSON_PAYLOAD="{\"event_id\":\"${EVENT_ID}\",\"event_type\":\"${EVENT_TYPE}\",\"entity_id\":\"${ENTITY_ID}\",\"source_system\":\"crm\",\"schema_version\":\"1.0\",\"event_time\":\"${NOW_ISO}\",\"source_id\":\"crm-${i}\",\"trace_id\":\"trace-${i}\",\"correlation_id\":\"corr-${i}\",\"data\":${DATA_JSON}}"

    echo "${JSON_PAYLOAD}"
done | docker exec -i "${CONTAINER_NAME}" /opt/kafka/bin/kafka-console-producer.sh \
       --bootstrap-server localhost:9092 \
       --producer.config "${CLIENT_CONFIG}" \
       --topic "${TOPIC_NAME}"

echo "======================================================================"
echo "[SUCCESS] Đã gửi thành công ${NUM_EVENTS} sự kiện CRM vào '${TOPIC_NAME}' trên cụm kafka-plain!"
echo "======================================================================"
