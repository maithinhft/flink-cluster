#!/usr/bin/env bash
# =============================================================================
# scripts/register-all-connectors.sh
# Đăng ký đồng thời 2 connector vào 2 Kafka Connect tương ứng:
# - postgres-rule-cdc  -> kafka-connect-plain  (http://localhost:8083) -> kafka-plain
# - postgres-schema-cdc -> kafka-connect-gssapi (http://localhost:8084) -> kafka-gssapi
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

CONNECT_PLAIN_URL="${CONNECT_PLAIN_URL:-http://localhost:8083}"
CONNECT_GSSAPI_URL="${CONNECT_GSSAPI_URL:-http://localhost:8084}"

echo "=========================================================="
echo "1. Đăng ký Connector Rule CDC vào kafka-connect-plain"
echo "   Endpoint: $CONNECT_PLAIN_URL"
echo "=========================================================="
CONNECT_URL="$CONNECT_PLAIN_URL" "$SCRIPT_DIR/register-connector.sh" "$PROJECT_ROOT/connectors/postgres-rule-connector.json"

echo ""
echo "=========================================================="
echo "2. Đăng ký Connector Schema CDC vào kafka-connect-gssapi"
echo "   Endpoint: $CONNECT_GSSAPI_URL"
echo "=========================================================="
CONNECT_URL="$CONNECT_GSSAPI_URL" "$SCRIPT_DIR/register-connector.sh" "$PROJECT_ROOT/connectors/postgres-schema-connector.json"

echo ""
echo "=========================================================="
echo "✅ Hoàn tất đăng ký connectors!"
echo "=========================================================="

