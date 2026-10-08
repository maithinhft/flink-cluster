#!/usr/bin/env bash
# Savepoint helper for the running Flink job (REST API, no docker exec needed).
#   ./savepoint.sh list                      -> running jobs
#   ./savepoint.sh trigger <jobId>           -> take a savepoint, keep the job running
#   ./savepoint.sh stop <jobId>              -> stop-with-savepoint (graceful upgrade)
#   ./savepoint.sh status <jobId> <triggerId>
# Restore afterwards with: SAVEPOINT_PATH=<path printed here> ./submit_job.sh
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -f "$DIR/../.env" ] && { set -a; source "$DIR/../.env"; set +a; }
FLINK_URL="${FLINK_URL:-http://${SERVER_IP:-localhost}:${FLINK_PORT:-8081}}"
TARGET_DIR="${SAVEPOINT_DIR:-file:///opt/flink/checkpoints/savepoints}"

wait_for() {
    local job="$1" trigger="$2" kind="$3"
    for _ in $(seq 1 120); do
        local resp status
        resp=$(curl -s "$FLINK_URL/jobs/$job/$kind/$trigger")
        status=$(echo "$resp" | grep -o '"id":"[A-Z_]*"' | head -1 | cut -d'"' -f4)
        if [ "$status" = "COMPLETED" ]; then
            local location
            location=$(echo "$resp" | grep -o '"location":"[^"]*"' | cut -d'"' -f4 || true)
            if [ -n "$location" ]; then
                echo "[SUCCESS] Savepoint: $location"
            else
                echo "[ERROR] $resp"; exit 1
            fi
            return 0
        fi
        sleep 1
    done
    echo "[ERROR] Timed out waiting for savepoint $trigger"; exit 1
}

case "${1:-}" in
    list)
        curl -s "$FLINK_URL/jobs/overview" | python3 -m json.tool ;;
    trigger)
        JOB="${2:?jobId required}"
        TRIGGER=$(curl -s -X POST -H "Content-Type: application/json" \
            -d "{\"target-directory\":\"$TARGET_DIR\",\"cancel-job\":false,\"formatType\":\"CANONICAL\"}" \
            "$FLINK_URL/jobs/$JOB/savepoints" | grep -o '"request-id":"[^"]*"' | cut -d'"' -f4)
        echo "[INFO] Trigger id: $TRIGGER"
        wait_for "$JOB" "$TRIGGER" savepoints ;;
    stop)
        JOB="${2:?jobId required}"
        TRIGGER=$(curl -s -X POST -H "Content-Type: application/json" \
            -d "{\"targetDirectory\":\"$TARGET_DIR\",\"drain\":false}" \
            "$FLINK_URL/jobs/$JOB/stop" | grep -o '"request-id":"[^"]*"' | cut -d'"' -f4)
        echo "[INFO] Stop trigger id: $TRIGGER"
        wait_for "$JOB" "$TRIGGER" savepoints ;;
    status)
        curl -s "$FLINK_URL/jobs/${2:?jobId}/savepoints/${3:?triggerId}" | python3 -m json.tool ;;
    *)
        grep '^#   ' "$0" | sed 's/^#//'; exit 1 ;;
esac
