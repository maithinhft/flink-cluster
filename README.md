# Flink Real-time Rule Engine — CDP Segmentation Core

Core xử lý luồng có trạng thái trên Apache Flink 1.20: tổng hợp theo cửa sổ thời gian bằng ring buffer
(288 bucket × 5 phút, SUM/COUNT/AVG/MIN/MAX, tumbling/sliding), nạp lại luật và công thức tổng hợp khi đang chạy
(CDC → Broadcast State), chỉ mục nghịch đảo theo document frequency để loại ứng viên, cây điều kiện
AND/OR/NOT + biểu thức tuyến tính, chịu lỗi bằng checkpoint tăng dần / unaligned / savepoint.

Tài liệu chi tiết: `spec/IMPLEMENTATION_PLAN.md` (kế hoạch + quyết định thiết kế), `spec/ARCHITECTURE.md` (kiến trúc).

## Thành phần

| Thư mục | Nội dung |
|---|---|
| `flink-jobs/` | Job `flink.RealtimeCepJob` (Java 17). Engine trong `flink.engine`, `flink.compile`, `flink.registry`, `flink.index`, `flink.state` |
| `data-generator/` | Sinh event/rule/schema, `ResultSinkApp`, `LatencyProbeApp`, `BatchVerifierApp`, demo `SegmentStudioApp` (Java 21) |
| `postgres/init/init.sql` | Bảng `rule_definitions`, `schema_definitions`, `alert_results`, metadata đa cụm Kafka |
| `connectors/` | Debezium CDC cho rule (cụm PLAIN) và schema (cụm GSSAPI) |
| `scripts/` | Tạo topic, đăng ký connector |
| `bench/run_scenario.sh` | Một lượt đo tải → `bench/results/*.csv` |
| `monitoring/` | Prometheus + Grafana (dashboard có panel engine: giảm ứng viên, state size, độ trễ hiệu lực rule) |

## 1. Khởi động cụm

```bash
cp .env.example .env            # chỉnh SERVER_IP, mật khẩu...
./up.script.sh                  # docker compose up, xuất keytab, tạo topic, đăng ký CDC, publish schema
```

`up.script.sh` gọi `scripts/create-kafka-topics.sh`. Topic điều khiển `rule_definitions` và `schema_registry`
có **1 partition + log compaction** (thứ tự toàn cục, không mất định nghĩa khi hết retention); topic sự kiện có
12 partition. Nếu cụm cũ đã tạo các topic này với cấu hình khác, cần xóa và tạo lại (`./reset_kafka_topic.sh`).

Client chạy ngoài máy chủ cần ánh xạ `kafka-gssapi` trong `/etc/hosts` (xem `spec/CHANGES_OUTSIDE_DATA_GENERATOR.md`).

## 2. Build, test và submit job

```bash
cd flink-jobs
mvn test                        # unit + operator harness + property test chỉ mục
./submit_job.sh                 # build fat jar, chờ JobManager, submit với tham số mặc định
```

Biến môi trường của `submit_job.sh`:

| Biến | Ý nghĩa |
|---|---|
| `INDEX_MODE` | `df2` (mặc định, chỉ mục 2 trục DF), `trigger`, `full` (duyệt toàn bộ — dùng để so sánh) |
| `EFFECTIVE_MODE` | `event-time` (mặc định: định nghĩa chỉ tổng hợp event có `event_time >= updated_at`) hoặc `immediate` |
| `PARALLELISM` | mặc định 4 |
| `SAVEPOINT_PATH` | khôi phục từ savepoint/checkpoint đã giữ lại |
| `EXTRA_ARGS` | tham số job bổ sung, ví dụ `--output.include.payload true --result.exactly.once true` |

Tham số job quan trọng khác: `--watermark.out.of.orderness.ms` (60000), `--watermark.idleness.ms` (60000),
`--late.tolerance.ms` (60000), `--state.full.load.threshold` (24), `--projection.enabled` (true),
`--bootstrap.enabled` (true), `--coldstart.buffer.ms` (30000), `--events.starting.offsets committed|earliest|latest`.

### Savepoint / nâng cấp không mất trạng thái

```bash
./flink-jobs/savepoint.sh list
./flink-jobs/savepoint.sh stop <jobId>            # stop-with-savepoint, in ra đường dẫn
SAVEPOINT_PATH=<đường dẫn> ./flink-jobs/submit_job.sh
```
Mọi operator có `uid` cố định và `maxParallelism = 128`, nên có thể sửa code hoặc đổi parallelism rồi khôi phục.

## 3. Sinh dữ liệu (`data-generator/`)

```bash
cd data-generator && mvn -q package -DskipTests
# Schema (nếu chưa chạy up.script.sh)
mvn -q exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/crm"
mvn -q exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/ecommerce"
mvn -q exec:java -Dexec.mainClass=generator.schema.SchemaPublisherApp -Dexec.args="--path ./schema/payment"
# Rule
mvn -q exec:java -Dexec.mainClass=generator.rules.RuleGeneratorApp -Dexec.args="--num-rules 10000 --seed 42"
# Event
mvn -q exec:java -Dexec.mainClass=generator.events.EventGeneratorApp -Dexec.args="--num-events 100000 --rate 5000"
```

Tùy chọn `RuleGeneratorApp`: `--and-root-ratio 0.7`, `--linear-ratio 0.1`, `--rule-metric-mode inline|reference`
(reference: một phần tổng hợp tham chiếu `metric_id` của schema), `--seed`.

Tùy chọn `EventGeneratorApp`: `--rate` (event/s, 0 = không giới hạn), `--zipf-s` (độ lệch Zipf, ví dụ 0.8, 1.2),
`--data-skew` (mô hình 1% hot entity), `--late-event-rate`, `--max-out-of-order-ms` (20000),
`--late-min-ms`/`--late-max-ms` (7–30 phút, vượt ngưỡng đóng bucket để thử luồng trễ), `--dirty-rate`,
`--num-entities`, `--workers`, `--continuous`, `--seed`, `--cluster dual|plain|gssapi|none`.

Đọc nhanh một topic:
```bash
mvn -q exec:java -Dexec.classpathScope=test -Dexec.mainClass=generator.common.KafkaConsumerApp -Dexec.args="--topic result --max 5"
```

## 4. Kết quả, ứng dụng minh họa và kiểm chứng

```bash
cd data-generator
# Ghi alert từ topic result vào PostgreSQL alert_results (theo lô, retry, upsert idempotent theo alert_id)
mvn -q exec:java -Dexec.mainClass=generator.sink.ResultSinkApp -Dexec.args="--batch-size 1000"

# Ứng dụng minh họa: khai báo tiêu chí, bắn event mẫu, xem alert realtime → http://localhost:8090
mvn -q exec:java -Dexec.mainClass=generator.demo.SegmentStudioApp -Dexec.args="--port 8090"

# Đối chiếu realtime với tính lại bằng batch (độc lập với code Flink)
mvn -q exec:java -Dexec.mainClass=generator.verify.BatchVerifierApp -Dexec.args="--report verify/report.csv"
```

Kịch bản demo (≈3 phút): lưu tiêu chí VIP mặc định (SUM total_amount 24h > 10.000.000 VÀ electronics/premium) →
bấm *Kịch bản VIP* → alert xuất hiện sau vài giây → sửa ngưỡng và lưu (version mới) → hành vi đổi mà không restart job.

`BatchVerifierApp` giả định tập rule không đổi trong lúc sinh event cần kiểm chứng. Chạy *chế độ A* với
`--late-event-rate 0` (kỳ vọng khớp 100%) và *chế độ B* có dữ liệu trễ (sai lệch được phân loại
`late-boundary` / `ordering` / `cooldown-cascade`; nhóm `unexplained` phải bằng 0, nếu không lệnh thoát với mã 2).
Tham số: `--actual kafka|postgres`, `--effective-mode`, `--no-watermark`, `--boundary-margin-ms`.

## 5. Đo hiệu năng

```bash
LABEL=S3-df2 INDEX_MODE=df2 RULES=100000 RATE=10000 ./bench/run_scenario.sh
LABEL=S3-full INDEX_MODE=full RULES=100000 RATE=10000 ./bench/run_scenario.sh
LABEL=S5-zipf1.2 ZIPF=1.2 ./bench/run_scenario.sh
```
Mỗi lượt: nạp rule → submit job → generator theo `--rate` → warm-up → `LatencyProbeApp` (p50/p95/p99) → lấy số liệu
Prometheus (event/s, ứng viên/event, mức giảm ứng viên, checkpoint, RocksDB) → cancel job → ghi
`bench/results/metrics.csv` và `bench/results/latency.csv`. Ma trận kịch bản S1–S10 ở `spec/IMPLEMENTATION_PLAN.md §8`.

## 6. Monitoring

| Dịch vụ | URL | Đăng nhập |
|---|---|---|
| Grafana | `http://localhost:3000` | `admin` / `admin` |
| Prometheus | `http://localhost:9090` | — |
| Flink Web UI | `http://localhost:8081` | — |
| Kafka UI | `http://localhost:8080` | — |

Dashboard **Flink Streaming Performance**: throughput, e2e latency (`e2e_latency_ms`), event-time lag, consumer lag,
checkpoint, busy/backpressure, JVM; nhóm **Segment Engine**: mức giảm ứng viên (so với toàn bộ rule và với lọc
trigger), rule đánh giá/khớp/bị cooldown, số rule/spec đang hoạt động, độ trễ hiệu lực định nghĩa (commit CDC →
áp dụng), thời gian xử lý mỗi event, kích thước state.

Metric chính của operator `Segment Engine`: `events_processed`, `events_late`, `rules_considered`,
`candidates_after_trigger`, `candidates_after_index`, `rules_evaluated`, `rules_matched`,
`alerts_suppressed_by_cooldown`, `rules_active`, `specs_active`, `specs_retired`, `index_posting_keys`,
`e2e_latency_ms`, `engine_process_us`, `rule_effective_latency_ms`, `event_time_lag_ms`.
