package flink.control;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import flink.compile.Values;
import flink.model.ControlMessage;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Debezium-unwrapped CDC records of {@code rule_definitions} / {@code schema_definitions}
 * (ExtractNewRecordState with {@code delete.handling.mode=rewrite}) into {@link ControlMessage}s.
 * Heartbeats and malformed records are dropped.
 */
public class ControlMessageParser implements FlatMapFunction<String, ControlMessage> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(ControlMessageParser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public void flatMap(String value, Collector<ControlMessage> out) {
        ControlMessage m = parse(value);
        if (m != null) {
            out.collect(m);
        }
    }

    public static ControlMessage parse(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            JsonNode n = MAPPER.readTree(value);
            if (n == null || !n.isObject()) {
                return null;
            }
            String table = n.path("__table").asText("");
            boolean deleted = "true".equalsIgnoreCase(n.path("__deleted").asText("false"))
                    || "d".equalsIgnoreCase(n.path("__op").asText(""));
            long sourceTs = n.hasNonNull("__source_ts_ms") ? n.get("__source_ts_ms").asLong() : 0L;
            if ("rule_definitions".equals(table) || (table.isEmpty() && n.hasNonNull("rule_id"))) {
                return parseRule(n, deleted, sourceTs);
            }
            if ("schema_definitions".equals(table) || (table.isEmpty() && n.hasNonNull("schema_id"))) {
                return parseSchema(n, deleted, sourceTs);
            }
            return null;
        } catch (Exception e) {
            LOG.warn("Dropping unparseable control record: {}", e.getMessage());
            return null;
        }
    }

    private static ControlMessage parseRule(JsonNode n, boolean deleted, long sourceTs) {
        if (!n.hasNonNull("rule_id")) {
            return null;
        }
        String id = n.get("rule_id").asText();
        long version = n.hasNonNull("version") ? n.get("version").asLong() : 1L;
        long eff = timestamp(n.get("updated_at"));
        if (eff == Values.NO_TIMESTAMP) {
            eff = timestamp(n.get("created_at"));
        }
        if (eff == Values.NO_TIMESTAMP) {
            eff = Long.MIN_VALUE;
        }
        return new ControlMessage(deleted ? ControlMessage.Kind.RULE_DELETE : ControlMessage.Kind.RULE_UPSERT,
                id, version, Long.toString(version), eff, sourceTs, n.toString());
    }

    private static ControlMessage parseSchema(JsonNode n, boolean deleted, long sourceTs) throws Exception {
        if (!n.hasNonNull("schema_id")) {
            return null;
        }
        String id = n.get("schema_id").asText();
        JsonNode payload = n.get("schema_payload");
        if (payload != null && payload.isTextual()) {
            payload = MAPPER.readTree(payload.asText());
        }
        String versionLabel = payload != null && payload.hasNonNull("version") ? payload.get("version").asText() : null;
        if (!deleted && (payload == null || !payload.isObject() || versionLabel == null)) {
            return null;
        }
        String body = payload == null ? "{}" : payload.toString();
        return new ControlMessage(deleted ? ControlMessage.Kind.SCHEMA_DELETE : ControlMessage.Kind.SCHEMA_UPSERT,
                id, 0L, versionLabel, Long.MIN_VALUE, sourceTs, body);
    }

    /**
     * Debezium encodes TIMESTAMP as epoch microseconds; bootstrap rows use epoch milliseconds; ISO strings
     * are also accepted.
     */
    public static long timestamp(JsonNode n) {
        if (n == null || n.isNull() || n.isMissingNode()) {
            return Values.NO_TIMESTAMP;
        }
        if (n.isNumber()) {
            long v = n.asLong();
            if (Math.abs(v) >= 100_000_000_000_000L) {
                return v / 1000L;
            }
            if (Math.abs(v) >= 100_000_000_000L) {
                return v;
            }
            return v * 1000L;
        }
        String s = n.asText().trim();
        if (s.length() > 10 && s.charAt(10) == ' ') {
            s = s.substring(0, 10) + "T" + s.substring(11);
            if (!s.endsWith("Z") && !s.matches(".*[+-]\\d\\d:?\\d\\d$")) {
                s = s + "Z";
            }
        }
        return Values.parseTimestamp(s);
    }

    public static ObjectNode ruleRow(String ruleId, String name, String ruleJson, long cooldownSeconds,
                                     long version, boolean enabled, long createdAtMs, long updatedAtMs) {
        ObjectNode row = MAPPER.createObjectNode();
        row.put("rule_id", ruleId);
        row.put("name", name);
        row.put("rule_json", ruleJson);
        row.put("cooldown_seconds", cooldownSeconds);
        row.put("version", version);
        row.put("enabled", enabled);
        row.put("created_at", createdAtMs);
        row.put("updated_at", updatedAtMs);
        row.put("__table", "rule_definitions");
        return row;
    }
}
