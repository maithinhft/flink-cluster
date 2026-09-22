#!/usr/bin/env bash
set -e

# 1. Tìm đường dẫn file .env ở thư mục cha của flink-jobs
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$DIR/../.env"

if [ -f "$ENV_FILE" ]; then
    set -a
    source "$ENV_FILE"
    set +a
    echo "[INFO] Loaded environment from $ENV_FILE"
else
    echo "[WARN] .env file not found at $ENV_FILE, falling back to localhost"
fi

SERVER_IP="${SERVER_IP:-localhost}"
FLINK_PORT="${FLINK_PORT:-8081}"

# Tự động ưu tiên localhost nếu đang chạy trực tiếp trên máy chủ chứa JobManager
if curl -s --connect-timeout 2 "http://localhost:${FLINK_PORT}/config" > /dev/null 2>&1; then
    FLINK_URL="http://localhost:${FLINK_PORT}"
    echo "[INFO] Detected local Flink JobManager, using: $FLINK_URL"
else
    FLINK_URL="http://${SERVER_IP}:${FLINK_PORT}"
    echo "[INFO] Target Flink JobManager: $FLINK_URL"
fi

# 2. Build Fat JAR
echo "[INFO] Building Fat JAR with Maven..."
mvn clean package

JAR_PATH="$DIR/target/flink-jobs-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_PATH" ]; then
    echo "[ERROR] Jar file not found at $JAR_PATH"
    exit 1
fi

# 3. Chờ Flink JobManager sẵn sàng trước khi upload
echo "[INFO] Checking if Flink JobManager is ready at $FLINK_URL..."
MAX_RETRIES=20
RETRY_COUNT=0
until curl -s -f --connect-timeout 2 "$FLINK_URL/config" > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "[ERROR] Flink JobManager at $FLINK_URL is not responding after $MAX_RETRIES attempts."
        echo "[TIP] Please check container status with: docker ps -a | grep flink-jobmanager"
        echo "[TIP] Please check JobManager logs with: docker logs --tail 50 flink-jobmanager"
        exit 1
    fi
    echo "[INFO] Waiting for JobManager to start... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 2
done
echo "[INFO] Flink JobManager is ready!"

# 4. Upload JAR lên Flink JobManager qua REST API
echo "[INFO] Uploading JAR to $FLINK_URL/jars/upload..."
UPLOAD_RESPONSE=$(curl -s -X POST -H "Expect:" -F "jarfile=@$JAR_PATH" "$FLINK_URL/jars/upload" || true)
JAR_ID=$(echo "$UPLOAD_RESPONSE" | grep -o '"filename":"[^"]*' | awk -F'/' '{print $NF}' || true)

if [ -z "$JAR_ID" ]; then
    echo "[ERROR] Failed to upload JAR. Server response: $UPLOAD_RESPONSE"
    echo "[TIP] Check JobManager logs with: docker logs --tail 50 flink-jobmanager"
    exit 1
fi

echo "[INFO] Uploaded successfully. JAR ID: $JAR_ID"

# 5. Kích hoạt chạy Job
ENTRY_CLASS="${ENTRY_CLASS:-flink.RealtimeCepJob}"
PARALLELISM="${PARALLELISM:-4}"
echo "[INFO] Submitting job ($ENTRY_CLASS) to Flink..."
DEFAULT_ARGS="--postgres.url jdbc:postgresql://postgres:5432/realtime_core --postgres.user postgres --postgres.password postgres --postgres.table.prefix kafka_stream --stream.metadata.discovery.interval.ms 30000 --schema.cluster gssapi --rule.cluster plain --events.cluster plain --result.cluster plain --dlq.cluster plain --schema.topic schema_registry --parallelism ${PARALLELISM}"
PROGRAM_ARGS="${1:-$DEFAULT_ARGS}"

RUN_RESPONSE=$(curl -s -X POST -H "Content-Type: application/json" \
  -d "{\"entryClass\":\"${ENTRY_CLASS}\",\"parallelism\":${PARALLELISM},\"programArgs\":\"${PROGRAM_ARGS}\"}" \
  "$FLINK_URL/jars/${JAR_ID}/run" || true)

echo "[INFO] Response: $RUN_RESPONSE"
JOB_ID=$(echo "$RUN_RESPONSE" | grep -o '"jobid":"[^"]*' | cut -d'"' -f4 || true)

if [ -n "$JOB_ID" ]; then
    echo "[SUCCESS] Job submitted successfully! Job ID: $JOB_ID"
    echo "[INFO] Monitor at: $FLINK_URL/#/job/$JOB_ID"
else
    echo "[WARN] Could not parse Job ID. Please check response above or Web UI at $FLINK_URL"
fi