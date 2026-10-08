package generator.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import generator.common.ClientConfig;
import generator.events.EventConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Batch sink: Kafka topic {@code result} -> PostgreSQL table {@code alert_results}.
 *
 * <ul>
 *   <li>Batches of up to {@code --batch-size} records or {@code --max-wait-ms}.</li>
 *   <li>Idempotent: {@code INSERT ... ON CONFLICT (alert_id) DO NOTHING}; alert_id is deterministic in the Flink job,
 *       so redelivered alerts (at-least-once) never create duplicates.</li>
 *   <li>Offsets are committed only after the DB transaction commits; DB failures are retried with exponential
 *       backoff and a fresh connection, so nothing is lost while PostgreSQL is down.</li>
 * </ul>
 * Usage: {@code mvn exec:java -Dexec.mainClass=generator.sink.ResultSinkApp -Dexec.args="--batch-size 1000"}
 */
public class ResultSinkApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    static final String UPSERT = "INSERT INTO alert_results (alert_id, entity_id, rule_id, rule_name, rule_version, "
            + "event_id, event_type, event_time, triggered_at, window_complete, evaluated_values, payload) "
            + "VALUES (?::uuid, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb) ON CONFLICT (alert_id) DO NOTHING";

    public static void main(String[] args) throws Exception {
        String topic = ClientConfig.arg(args, "--result-topic", "result");
        String group = ClientConfig.arg(args, "--group", "result-sink");
        int batchSize = Integer.parseInt(ClientConfig.arg(args, "--batch-size", "1000"));
        long maxWaitMs = Long.parseLong(ClientConfig.arg(args, "--max-wait-ms", "200"));
        String dbUrl = ClientConfig.arg(args, "--db-url", ClientConfig.dbUrl() + "?reWriteBatchedInserts=true");

        EventConfig cfg = ClientConfig.eventConfig(args);
        Properties props = ClientConfig.consumer(cfg, false, group);
        props.put("max.poll.records", String.valueOf(batchSize));
        props.put("fetch.max.wait.ms", String.valueOf(maxWaitMs));

        System.out.printf("Result sink: %s -> %s (batch=%d, maxWait=%dms)%n", topic, dbUrl, batchSize, maxWaitMs);
        long total = 0;
        long skipped = 0;
        Connection conn = null;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(maxWaitMs));
                if (records.isEmpty()) {
                    continue;
                }
                List<JsonNode> batch = new ArrayList<>(records.count());
                for (ConsumerRecord<String, String> r : records) {
                    try {
                        JsonNode n = MAPPER.readTree(r.value());
                        if (n != null && n.hasNonNull("alert_id")) {
                            batch.add(n);
                        } else {
                            skipped++;
                        }
                    } catch (Exception e) {
                        skipped++;
                    }
                }
                long backoff = 500;
                while (true) {
                    try {
                        if (conn == null || conn.isClosed()) {
                            conn = ClientConfig.db(dbUrl);
                            conn.setAutoCommit(false);
                        }
                        int inserted = write(conn, batch);
                        conn.commit();
                        consumer.commitSync();
                        total += batch.size();
                        System.out.printf("[sink] batch=%d inserted=%d total=%,d skipped=%d%n",
                                batch.size(), inserted, total, skipped);
                        break;
                    } catch (SQLException e) {
                        System.err.printf("[sink] DB error (%s), retrying in %d ms%n", e.getMessage(), backoff);
                        try {
                            if (conn != null) {
                                conn.close();
                            }
                        } catch (SQLException ignored) {
                        }
                        conn = null;
                        Thread.sleep(backoff);
                        backoff = Math.min(backoff * 2, 30_000);
                    }
                }
            }
        }
    }

    static int write(Connection conn, List<JsonNode> batch) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(UPSERT)) {
            for (JsonNode n : batch) {
                JsonNode trigger = n.path("trigger_event");
                ps.setString(1, n.get("alert_id").asText());
                ps.setString(2, n.path("entity_id").asText());
                ps.setString(3, n.path("rule_id").asText());
                ps.setString(4, n.path("rule_name").asText(null));
                ps.setLong(5, n.path("rule_version").asLong());
                ps.setString(6, trigger.path("event_id").asText(null));
                ps.setString(7, trigger.path("event_type").asText(null));
                ps.setTimestamp(8, new Timestamp(n.path("event_time_ms").asLong()));
                ps.setTimestamp(9, new Timestamp(n.path("triggered_at_ms").asLong()));
                ps.setBoolean(10, n.path("window_complete").asBoolean(true));
                ps.setString(11, n.path("evaluated_values").toString());
                ps.setString(12, n.toString());
                ps.addBatch();
            }
            int inserted = 0;
            for (int c : ps.executeBatch()) {
                if (c > 0) {
                    inserted += c;
                }
            }
            return inserted;
        }
    }
}
