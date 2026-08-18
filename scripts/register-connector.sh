#!/usr/bin/env bash
# =============================================================================
# scripts/register-connector.sh
# Đăng ký Debezium PostgreSQL connector vào Kafka Connect
# =============================================================================

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTOR_FILE="${1:-./connectors/postgres-cdc-connector.json}"

echo "🔌 Đang đăng ký connector từ: $CONNECTOR_FILE"
echo "   Kafka Connect endpoint: $CONNECT_URL"
echo ""

# Kiểm tra Kafka Connect sẵn sàng chưa
until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  echo "⏳ Đang chờ Kafka Connect khởi động..."
  sleep 5
done

echo "✅ Kafka Connect đã sẵn sàng"
echo ""

# Kiểm tra connector đã tồn tại chưa
CONNECTOR_NAME=$(python3 -c "import json,sys; print(json.load(open('$CONNECTOR_FILE'))['name'])" 2>/dev/null || \
                 node -e "console.log(require('$CONNECTOR_FILE').name)" 2>/dev/null || \
                 grep '"name"' "$CONNECTOR_FILE" | head -1 | sed 's/.*"name"[[:space:]]*:[[:space:]]*"\(.*\)".*/\1/')

EXISTING=$(curl -sf "$CONNECT_URL/connectors/$CONNECTOR_NAME" 2>/dev/null || echo "")

if [ -n "$EXISTING" ]; then
  echo "⚠️  Connector '$CONNECTOR_NAME' đã tồn tại. Đang cập nhật config..."
  # Lấy phần config từ file (bỏ trường "name")
  CONFIG=$(python3 -c "
import json, sys
data = json.load(open('$CONNECTOR_FILE'))
print(json.dumps({'config': data['config']}))
" 2>/dev/null)
  curl -sf -X PUT \
    -H "Content-Type: application/json" \
    -d "$CONFIG" \
    "$CONNECT_URL/connectors/$CONNECTOR_NAME/config" | python3 -m json.tool || true
else
  echo "📤 Đang đăng ký connector mới: $CONNECTOR_NAME"
  curl -sf -X POST \
    -H "Content-Type: application/json" \
    -d @"$CONNECTOR_FILE" \
    "$CONNECT_URL/connectors" | python3 -m json.tool || true
fi

echo ""
echo "🔍 Trạng thái connector:"
sleep 2
curl -sf "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool || true
