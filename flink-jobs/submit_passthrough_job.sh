#!/usr/bin/env bash
# ==============================================================================
# Script nộp Flink Job phụ (Dynamic Kafka Pass-Through Job) lên Flink JobManager
# Job này chỉ nhận event từ DynamicKafkaSource (đa cụm) và đẩy thẳng ra topic result
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export ENTRY_CLASS="flink.DynamicPassThroughJob"
export PARALLELISM="${PARALLELISM:-4}"

DEFAULT_PASSTHROUGH_ARGS="--postgres.url jdbc:postgresql://postgres:5432/realtime_core --postgres.user postgres --postgres.password postgres --postgres.table.prefix kafka_stream --stream.metadata.discovery.interval.ms 30000 --result.cluster plain --result.topic result --events.group.id flink-passthrough-group --parallelism ${PARALLELISM}"

echo "[INFO] Running DynamicPassThroughJob submitter..."
"${SCRIPT_DIR}/submit_job.sh" "${1:-$DEFAULT_PASSTHROUGH_ARGS}"

