package generator.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import generator.common.ClientConfig;
import generator.events.EventConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;

/**
 * Correctness check: realtime output vs independent batch recomputation ({@link BatchSemantics}).
 *
 * <pre>
 * mvn exec:java -Dexec.mainClass=generator.verify.BatchVerifierApp -Dexec.args="--report verify/report.csv"
 *   --actual kafka|postgres     where to read realtime alerts from (default kafka topic "result")
 *   --effective-mode event-time|immediate   must match the job's --definition.effective.mode
 *   --no-watermark              disable late-data simulation (use when the run had --late-event-rate 0)
 *   --boundary-margin-ms 5000   tolerance used to classify watermark-boundary differences
 * </pre>
 * Assumes the rule set did not change while the verified events were produced (set up rules first).
 */
public class BatchVerifierApp {

    private static final ObjectMapper M = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        EventConfig cfg = ClientConfig.eventConfig(args);
        String report = ClientConfig.arg(args, "--report", "verify/report.csv");
        String actualSource = ClientConfig.arg(args, "--actual", "kafka");
        long margin = Long.parseLong(ClientConfig.arg(args, "--boundary-margin-ms", "5000"));
        BatchSemantics.Options opt = new BatchSemantics.Options();
        opt.eventTimeEffective = !"immediate".equalsIgnoreCase(ClientConfig.arg(args, "--effective-mode", "event-time"));
        opt.simulateWatermark = !ClientConfig.flag(args, "--no-watermark");
        opt.lateToleranceMs = Long.parseLong(ClientConfig.arg(args, "--late-tolerance-ms", "60000"));
        opt.outOfOrderMs = Long.parseLong(ClientConfig.arg(args, "--out-of-orderness-ms", "60000"));

        List<BatchSemantics.InEvent> events = new ArrayList<>();
        readTopic(ClientConfig.consumer(cfg, false, null), cfg.ecommerceTopic, r -> events.add(toIn(r)));
        readTopic(ClientConfig.consumer(cfg, false, null), cfg.paymentTopic, r -> events.add(toIn(r)));
        readTopic(ClientConfig.consumer(cfg, !"plain".equalsIgnoreCase(cfg.cluster) && !"none".equalsIgnoreCase(cfg.cluster), null),
                cfg.crmTopic, r -> events.add(toIn(r)));
        System.out.printf("Read %,d events%n", events.size());

        Map<String, String> schemas = new HashMap<>();
        List<BatchSemantics.RuleRow> rules = new ArrayList<>();
        Map<String, JsonNode> actual = new HashMap<>();
        try (Connection c = ClientConfig.db(ClientConfig.arg(args, "--db-url", null)); Statement st = c.createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT schema_id, schema_payload::text FROM schema_definitions")) {
                while (rs.next()) schemas.put(rs.getString(1), rs.getString(2));
            }
            try (ResultSet rs = st.executeQuery("SELECT DISTINCT ON (rule_id) rule_id::text, version, name, rule_json::text, "
                    + "cooldown_seconds, enabled, updated_at FROM rule_definitions ORDER BY rule_id, version DESC")) {
                while (rs.next()) {
                    if (rs.getBoolean("enabled")) {
                        rules.add(new BatchSemantics.RuleRow(rs.getString(1), rs.getLong(2), rs.getString(3), rs.getString(4),
                                rs.getLong(5), rs.getTimestamp(7).getTime()));
                    }
                }
            }
            if ("postgres".equalsIgnoreCase(actualSource)) {
                try (ResultSet rs = st.executeQuery("SELECT payload::text FROM alert_results")) {
                    while (rs.next()) addActual(actual, rs.getString(1));
                }
            }
        }
        if (!"postgres".equalsIgnoreCase(actualSource)) {
            readTopic(ClientConfig.consumer(cfg, false, null), ClientConfig.arg(args, "--result-topic", "result"),
                    r -> addActual(actual, r.value()));
        }
        System.out.printf("Rules=%d schemas=%d realtime alerts=%,d%n", rules.size(), schemas.size(), actual.size());

        BatchSemantics.Result expected = new BatchSemantics(opt).run(events, schemas, rules);
        System.out.printf("Batch: valid=%,d dirty=%,d late=%,d expected alerts=%,d rule errors=%d%n",
                expected.valid, expected.dirty, expected.late, expected.alerts.size(), expected.ruleErrors.size());

        Set<String> ruleIds = new TreeSet<>();
        for (BatchSemantics.RuleRow r : rules) ruleIds.add(r.ruleId);
        Set<BatchSemantics.AlertKey> real = new TreeSet<>();
        for (JsonNode a : actual.values()) {
            String eventId = a.path("trigger_event").path("event_id").asText();
            if (ruleIds.contains(a.path("rule_id").asText()) && expected.events.containsKey(eventId)) {
                real.add(new BatchSemantics.AlertKey(a.path("entity_id").asText(), a.path("rule_id").asText(), eventId));
            }
        }

        Map<String, Integer> classes = new TreeMap<>();
        List<String[]> diffs = new ArrayList<>();
        int matched = 0;
        for (BatchSemantics.AlertKey k : expected.alerts) {
            if (real.contains(k)) matched++;
            else diffs.add(new String[]{"missing_in_realtime", k.entityId(), k.ruleId(), k.eventId(), classify(k, expected, events, margin, diffs)});
        }
        for (BatchSemantics.AlertKey k : real) {
            if (!expected.alerts.contains(k)) {
                diffs.add(new String[]{"unexpected_in_realtime", k.entityId(), k.ruleId(), k.eventId(), classify(k, expected, events, margin, diffs)});
            }
        }
        for (String[] d : diffs) classes.merge(d[4], 1, Integer::sum);

        double precision = real.isEmpty() ? 1.0 : matched / (double) real.size();
        double recall = expected.alerts.isEmpty() ? 1.0 : matched / (double) expected.alerts.size();
        System.out.println("==================================================");
        System.out.printf("matched=%,d  missing=%,d  unexpected=%,d%n", matched,
                expected.alerts.size() - matched, real.size() - matched);
        System.out.printf("precision=%.4f  recall=%.4f%n", precision, recall);
        System.out.println("difference classes: " + classes);
        if (Paths.get(report).getParent() != null) Files.createDirectories(Paths.get(report).getParent());
        try (FileWriter w = new FileWriter(report)) {
            w.write("kind,entity_id,rule_id,event_id,class\n");
            for (String[] d : diffs) w.write(String.join(",", d) + "\n");
        }
        System.out.println("Diff report written to " + report);
        if (classes.getOrDefault("unexplained", 0) > 0) System.exit(2);
    }

    /**
     * late-boundary: some event of the entity inside the rule's span arrived within margin of its bucket close;
     * ordering: another event of the entity in the span arrived through a different topic within 1s (cross-topic
     * arrival order is not deterministic); cooldown-cascade: an earlier difference on the same entity/rule within
     * the cooldown can shift every later decision.
     */
    static String classify(BatchSemantics.AlertKey k, BatchSemantics.Result r, List<BatchSemantics.InEvent> all,
                           long margin, List<String[]> previous) {
        BatchSemantics.EventInfo trig = r.events.get(k.eventId());
        if (trig == null) return "unexplained";
        long span = r.ruleMaxSpanMs.getOrDefault(k.ruleId(), BatchSemantics.B);
        long from = trig.eventTimeMs - span - BatchSemantics.B;
        for (BatchSemantics.EventInfo e : r.events.values()) {
            if (!k.entityId().equals(e.entityId) || e.dirty || e.eventTimeMs < from || e.eventTimeMs > trig.eventTimeMs + BatchSemantics.B) continue;
            if (e.closeTime != 0 && e.watermark != Long.MIN_VALUE && Math.abs(e.watermark - e.closeTime) <= margin) return "late-boundary";
        }
        for (BatchSemantics.EventInfo e : r.events.values()) {
            if (!k.entityId().equals(e.entityId) || e == trig || e.dirty || e.late) continue;
            if (!e.topic.equals(trig.topic) && Math.abs(e.kafkaTs - trig.kafkaTs) <= 1000 && e.eventTimeMs >= from) return "ordering";
        }
        long cooldown = r.ruleCooldownMs.getOrDefault(k.ruleId(), 0L);
        if (cooldown > 0) {
            for (String[] d : previous) {
                BatchSemantics.EventInfo p = r.events.get(d[3]);
                if (p != null && d[1].equals(k.entityId()) && d[2].equals(k.ruleId())
                        && p.eventTimeMs <= trig.eventTimeMs && trig.eventTimeMs - p.eventTimeMs < cooldown + span) {
                    return "cooldown-cascade";
                }
            }
        }
        return "unexplained";
    }

    private static void addActual(Map<String, JsonNode> actual, String json) {
        try {
            JsonNode n = M.readTree(json);
            if (n.hasNonNull("alert_id")) actual.put(n.get("alert_id").asText(), n);
        } catch (Exception ignored) {
        }
    }

    private static BatchSemantics.InEvent toIn(ConsumerRecord<String, String> r) {
        return new BatchSemantics.InEvent(r.topic(), r.partition(), r.offset(), r.timestamp(), r.value());
    }

    static void readTopic(Properties props, String topic, Consumer<ConsumerRecord<String, String>> sink) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> parts = new ArrayList<>();
            for (PartitionInfo pi : consumer.partitionsFor(topic, Duration.ofSeconds(30))) {
                parts.add(new TopicPartition(topic, pi.partition()));
            }
            consumer.assign(parts);
            consumer.seekToBeginning(parts);
            Map<TopicPartition, Long> end = consumer.endOffsets(parts);
            while (true) {
                boolean done = true;
                for (TopicPartition tp : parts) {
                    if (consumer.position(tp) < end.get(tp)) {
                        done = false;
                        break;
                    }
                }
                if (done) break;
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                    if (r.value() != null) sink.accept(r);
                }
            }
        }
    }
}
