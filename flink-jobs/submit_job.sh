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
FLINK_URL="http://${SERVER_IP}:${FLINK_PORT}"

echo "[INFO] Target Flink JobManager: $FLINK_URL"

# 2. Build Fat JAR
echo "[INFO] Building Fat JAR with Maven..."
mvn clean package

JAR_PATH="$DIR/target/flink-jobs-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_PATH" ]; then
    echo "[ERROR] Jar file not found at $JAR_PATH"
    exit 1
fi

# 3. Upload JAR lên Flink JobManager qua REST API
echo "[INFO] Uploading JAR to $FLINK_URL/jars/upload..."
UPLOAD_RESPONSE=$(curl -s -X POST -H "Expect:" -F "jarfile=@$JAR_PATH" "$FLINK_URL/jars/upload")
JAR_ID=$(echo "$UPLOAD_RESPONSE" | grep -o '"filename":"[^"]*' | awk -F'/' '{print $NF}')

if [ -z "$JAR_ID" ]; then
    echo "[ERROR] Failed to upload JAR. Server response: $UPLOAD_RESPONSE"
    exit 1
fi

echo "[INFO] Uploaded successfully. JAR ID: $JAR_ID"

# 4. Kích hoạt chạy Job
echo "[INFO] Submitting job to Flink..."
RUN_RESPONSE=$(curl -s -X POST "$FLINK_URL/jars/${JAR_ID}/run?entry-class=flink.ValidationJob&programArgs=--bootstrap.servers%20kafka:29092%20--schema.topic%20schema_registry")

echo "[INFO] Response: $RUN_RESPONSE"
JOB_ID=$(echo "$RUN_RESPONSE" | grep -o '"jobid":"[^"]*' | cut -d'"' -f4)

if [ -n "$JOB_ID" ]; then
    echo "[SUCCESS] Job submitted successfully! Job ID: $JOB_ID"
    echo "[INFO] Monitor at: $FLINK_URL/#/job/$JOB_ID"
else
    echo "[WARN] Could not parse Job ID. Please check response above or Web UI at $FLINK_URL"
fi