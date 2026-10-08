#!/usr/bin/env bash
# =============================================================================
# bench/run_scenario.sh — one benchmark run (spec/IMPLEMENTATION_PLAN.md §8).
#
# Prerequisites: cluster up (./up.script.sh), schemas published, Prometheus on :9090.
#
# Example matrix (S2/S3/S5):
#   for idx in full trigger df2; do LABEL=S3-$idx INDEX_MODE=$idx RULES=100000 RATE=10000 ./bench/run_scenario.sh; done
#   for z in 0 0.8 1.2; do LABEL=S5-zipf$z ZIPF=$z ./bench/run_scenario.sh; done
#
# Variables (defaults in brackets):
#   LABEL [run]  RULES [10000, 0 = keep current rules]  RESET_RULES [1]  RATE [10000 ev/s]
#   WARMUP_S [120]  DURATION_S [300]  ZIPF [0]  ENTITIES [100000]  LATE_RATE [0.0]  DIRTY_RATE [0.05]
#   INDEX_MODE [df2: full|trigger|df2]  PARALLELISM [4]  EXTRA_ARGS (extra Flink job args)
# Output: bench/results/metrics.csv (+ latency.csv written by LatencyProbeApp)
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[ -f "$ROOT/.env" ] && { set -a; source "$ROOT/.env"; set +a; }

LABEL="${LABEL:-run}"
RULES="${RULES:-10000}"
RESET_RULES="${RESET_RULES:-1}"
RATE="${RATE:-10000}"
WARMUP_S="${WARMUP_S:-120}"
DURATION_S="${DURATION_S:-300}"
ZIPF="${ZIPF:-0}"
ENTITIES="${ENTITIES:-100000}"
LATE_RATE="${LATE_RATE:-0.0}"
DIRTY_RATE="${DIRTY_RATE:-0.05}"
export INDEX_MODE="${INDEX_MODE:-df2}"
export PARALLELISM="${PARALLELISM:-4}"
SERVER_IP="${SERVER_IP:-localhost}"
FLINK_URL="http://${SERVER_IP}:${FLINK_PORT:-8081}"
PROM_URL="http://${SERVER_IP}:${PROMETHEUS_PORT:-9090}"
OUT_DIR="$ROOT/bench/results"
mkdir -p "$OUT_DIR"

log() { echo "[$(date +%H:%M:%S)] $*"; }
gen() { (cd "$ROOT/data-generator" && mvn -q exec:java -Dexec.mainClass="$1" -Dexec.args="$2"); }

prom() {
    curl -s --get "$PROM_URL/api/v1/query" --data-urlencode "query=$1" \
        | python3 -c 'import json,sys
r=json.load(sys.stdin).get("data",{}).get("result",[])
print(r[0]["value"][1] if r else "nan")'
}

log "Scenario $LABEL: rules=$RULES rate=$RATE/s zipf=$ZIPF entities=$ENTITIES index=$INDEX_MODE p=$PARALLELISM"

(cd "$ROOT/data-generator" && mvn -q -DskipTests package)

if [ "$RULES" -gt 0 ]; then
    if [ "$RESET_RULES" = "1" ]; then
        log "Resetting rule_definitions"
        docker compose -f "$ROOT/docker-compose.yml" exec -T postgres \
            psql -U "${POSTGRES_USER:-postgres}" -d "${POSTGRES_DB:-realtime_core}" -c "TRUNCATE rule_definitions;"
    fi
    log "Loading $RULES rules"
    gen generator.rules.RuleGeneratorApp "--num-rules $RULES --batch-size 5000 --seed 42"
fi

log "Submitting job"
SUBMIT_OUT=$("$ROOT/flink-jobs/submit_job.sh" | tee /dev/stderr)
JOB_ID=$(echo "$SUBMIT_OUT" | grep -o 'Job ID: [0-9a-f]*' | awk '{print $3}')
[ -n "$JOB_ID" ] || { echo "Could not determine job id"; exit 1; }

cleanup() {
    [ -n "${GEN_PID:-}" ] && kill "$GEN_PID" 2>/dev/null || true
    curl -s -X PATCH "$FLINK_URL/jobs/$JOB_ID?mode=cancel" >/dev/null || true
}
trap cleanup EXIT

log "Starting generator (continuous, $RATE ev/s)"
(cd "$ROOT/data-generator" && mvn -q exec:java -Dexec.mainClass=generator.events.EventGeneratorApp \
    -Dexec.args="--continuous --rate $RATE --workers 4 --num-entities $ENTITIES --zipf-s $ZIPF --late-event-rate $LATE_RATE --dirty-rate $DIRTY_RATE --seed 7") \
    > "$OUT_DIR/$LABEL.generator.log" 2>&1 &
GEN_PID=$!

log "Warm-up ${WARMUP_S}s"
sleep "$WARMUP_S"

log "Measuring ${DURATION_S}s"
gen generator.bench.LatencyProbeApp "--duration-s $DURATION_S --csv $OUT_DIR/latency.csv --label $LABEL" \
    | tee "$OUT_DIR/$LABEL.latency.log"

OP='flink_taskmanager_job_task_operator_'
IN_RATE=$(prom "sum(rate(${OP}events_processed[2m]))")
REDUCTION_ALL=$(prom "1 - sum(rate(${OP}candidates_after_index[2m])) / clamp_min(sum(rate(${OP}rules_considered[2m])),1)")
REDUCTION_TRIGGER=$(prom "1 - sum(rate(${OP}candidates_after_index[2m])) / clamp_min(sum(rate(${OP}candidates_after_trigger[2m])),1)")
CAND_PER_EVENT=$(prom "sum(rate(${OP}candidates_after_index[2m])) / clamp_min(sum(rate(${OP}events_processed[2m])),1)")
EVAL_RATE=$(prom "sum(rate(${OP}rules_evaluated[2m]))")
PROC_P99=$(prom "max(${OP}engine_process_us{quantile=\"0.99\"})")
CKPT_MS=$(prom "max(flink_jobmanager_job_lastCheckpointDuration)")
CKPT_SIZE=$(prom "max(flink_jobmanager_job_lastCheckpointSize)")
CKPT_FULL=$(prom "max(flink_jobmanager_job_lastCheckpointFullSize)")
STATE_LIVE=$(prom "sum(${OP}rocksdb_estimate_live_data_size)")
BACKPRESSURE=$(prom "max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)")
LAG=$(prom "max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)")

CSV="$OUT_DIR/metrics.csv"
[ -f "$CSV" ] || echo "label,rules,rate_target,zipf,entities,index_mode,parallelism,events_per_s,candidates_per_event,reduction_vs_all,reduction_vs_trigger,rules_evaluated_per_s,engine_p99_us,checkpoint_ms,checkpoint_bytes,checkpoint_full_bytes,rocksdb_live_bytes,max_backpressure_ms_per_s,max_consumer_lag" > "$CSV"
echo "$LABEL,$RULES,$RATE,$ZIPF,$ENTITIES,$INDEX_MODE,$PARALLELISM,$IN_RATE,$CAND_PER_EVENT,$REDUCTION_ALL,$REDUCTION_TRIGGER,$EVAL_RATE,$PROC_P99,$CKPT_MS,$CKPT_SIZE,$CKPT_FULL,$STATE_LIVE,$BACKPRESSURE,$LAG" >> "$CSV"
log "Appended results to $CSV"
tail -1 "$CSV"
