package flink.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import flink.compile.Values;
import flink.model.ControlMessage;
import flink.model.ValidatedEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validation-side catalog: compiled schemas (multi-version with a grace period for deprecated versions)
 * and the set of fields referenced by rules and metrics, used to project events before the shuffle.
 */
public final class SchemaCatalog {

    public static final Set<String> CORE_FIELDS = Set.of("event_id", "entity_id", "event_type", "source_system");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    enum FieldType { ANY, STRING, INTEGER, NUMBER, BOOLEAN }

    static final class FieldRule {
        final String name;
        final boolean required;
        final FieldType type;
        final double min;
        final double max;
        final Set<String> allowed;
        final boolean isoTimestamp;

        FieldRule(String name, boolean required, FieldType type, double min, double max, Set<String> allowed,
                  boolean isoTimestamp) {
            this.name = name;
            this.required = required;
            this.type = type;
            this.min = min;
            this.max = max;
            this.allowed = allowed;
            this.isoTimestamp = isoTimestamp;
        }
    }

    public static final class CompiledSchema {
        final String key;
        final List<FieldRule> fields;
        final Set<String> metricFields;

        CompiledSchema(String key, List<FieldRule> fields, Set<String> metricFields) {
            this.key = key;
            this.fields = fields;
            this.metricFields = metricFields;
        }
    }

    private final Map<String, CompiledSchema> byKey = new HashMap<>();
    private final Map<String, String> payloadByKey = new HashMap<>();
    private final Map<String, String> latestVersion = new HashMap<>();
    private final Map<String, Long> deprecatedAt = new HashMap<>();
    private final Map<String, Set<String>> ruleFields = new HashMap<>();
    private final Set<String> referenced = new HashSet<>();
    private final long graceMs;

    public SchemaCatalog(long graceMs) {
        this.graceMs = graceMs;
        referenced.addAll(CORE_FIELDS);
    }

    public static String schemaKey(String sourceSystem, String eventType, String version) {
        return sourceSystem + "_" + eventType + "_" + version;
    }

    public static int compareVersions(String v1, String v2) {
        if (v1 == null || v2 == null) {
            return v1 == null ? (v2 == null ? 0 : -1) : 1;
        }
        try {
            String[] a = v1.split("\\.");
            String[] b = v2.split("\\.");
            for (int i = 0; i < Math.max(a.length, b.length); i++) {
                int x = i < a.length ? Integer.parseInt(a[i].trim()) : 0;
                int y = i < b.length ? Integer.parseInt(b[i].trim()) : 0;
                if (x != y) {
                    return Integer.compare(x, y);
                }
            }
            return 0;
        } catch (NumberFormatException e) {
            return v1.compareTo(v2);
        }
    }

    public boolean has(String key) {
        return byKey.containsKey(key);
    }

    public int size() {
        return byKey.size();
    }

    public Set<String> referencedFields() {
        return referenced;
    }

    public Map<String, String> payloads() {
        return payloadByKey;
    }

    /**
     * Applies a schema or rule control message. Returns true if the catalog changed.
     * {@code clockMs} drives the deprecation grace period (watermark when available).
     */
    public boolean apply(ControlMessage m, long clockMs) {
        if (m.isRule()) {
            return applyRule(m);
        }
        String schemaId = m.id;
        if (m.kind == ControlMessage.Kind.SCHEMA_DELETE) {
            String v = m.versionLabel != null ? m.versionLabel : latestVersion.get(schemaId);
            if (v != null) {
                remove(schemaId + "_" + v);
            }
            if (v != null && v.equals(latestVersion.get(schemaId))) {
                latestVersion.remove(schemaId);
            }
            rebuildReferenced();
            return true;
        }
        CompiledSchema compiled;
        try {
            compiled = compile(schemaId + "_" + m.versionLabel, MAPPER.readTree(m.payload));
        } catch (Exception e) {
            return false;
        }
        String key = compiled.key;
        String current = latestVersion.get(schemaId);
        byKey.put(key, compiled);
        payloadByKey.put(key, m.payload);
        if (current == null) {
            latestVersion.put(schemaId, m.versionLabel);
            deprecatedAt.remove(key);
        } else {
            int cmp = compareVersions(m.versionLabel, current);
            if (cmp > 0) {
                deprecatedAt.put(schemaId + "_" + current, clockMs);
                latestVersion.put(schemaId, m.versionLabel);
                deprecatedAt.remove(key);
            } else if (cmp < 0) {
                deprecatedAt.put(key, clockMs);
            }
        }
        expire(clockMs);
        rebuildReferenced();
        return true;
    }

    public void expire(long clockMs) {
        Iterator<Map.Entry<String, Long>> it = deprecatedAt.entrySet().iterator();
        List<String> drop = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<String, Long> e = it.next();
            if (clockMs - e.getValue() > graceMs) {
                drop.add(e.getKey());
                it.remove();
            }
        }
        for (String k : drop) {
            remove(k);
        }
        if (!drop.isEmpty()) {
            rebuildReferenced();
        }
    }

    private void remove(String key) {
        byKey.remove(key);
        payloadByKey.remove(key);
        deprecatedAt.remove(key);
    }

    private boolean applyRule(ControlMessage m) {
        if (m.kind == ControlMessage.Kind.RULE_DELETE) {
            boolean changed = ruleFields.remove(m.id) != null;
            if (changed) {
                rebuildReferenced();
            }
            return changed;
        }
        Set<String> fields = new HashSet<>();
        try {
            JsonNode row = MAPPER.readTree(m.payload);
            JsonNode doc = row.has("rule_json") ? row.get("rule_json") : row;
            if (doc.isTextual()) {
                doc = MAPPER.readTree(doc.asText());
            }
            collectFields(doc, fields);
        } catch (Exception e) {
            return false;
        }
        ruleFields.put(m.id, fields);
        rebuildReferenced();
        return true;
    }

    public void putRuleFields(String ruleId, Set<String> fields) {
        ruleFields.put(ruleId, fields);
        rebuildReferenced();
    }

    public Set<String> ruleFields(String ruleId) {
        return ruleFields.get(ruleId);
    }

    static void collectFields(JsonNode n, Set<String> out) {
        if (n == null) {
            return;
        }
        if (n.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = n.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                String k = e.getKey();
                JsonNode v = e.getValue();
                if (("field".equals(k) || "field_name".equals(k) || "source_field".equals(k)) && v.isTextual()) {
                    out.add(v.asText());
                } else {
                    collectFields(v, out);
                }
            }
        } else if (n.isArray()) {
            for (JsonNode c : n) {
                collectFields(c, out);
            }
        }
    }

    private void rebuildReferenced() {
        referenced.clear();
        referenced.addAll(CORE_FIELDS);
        for (Set<String> s : ruleFields.values()) {
            referenced.addAll(s);
        }
        for (CompiledSchema s : byKey.values()) {
            referenced.addAll(s.metricFields);
        }
    }

    static CompiledSchema compile(String key, JsonNode schema) {
        List<FieldRule> rules = new ArrayList<>();
        JsonNode fields = schema.get("fields");
        if (fields != null && fields.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> it = fields.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                JsonNode def = e.getValue();
                FieldType type = parseType(def.path("type").asText(""));
                boolean required = def.path("required").asBoolean(false);
                double min = def.has("min") ? def.get("min").asDouble() : Double.NaN;
                double max = def.has("max") ? def.get("max").asDouble() : Double.NaN;
                Set<String> allowed = null;
                if (def.has("allowed_values") && def.get("allowed_values").isArray()) {
                    allowed = new HashSet<>();
                    for (JsonNode a : def.get("allowed_values")) {
                        allowed.add(a.asText());
                    }
                }
                boolean iso = "ISO-8601".equalsIgnoreCase(def.path("format").asText(""));
                rules.add(new FieldRule(e.getKey(), required, type, min, max, allowed, iso));
            }
        }
        Set<String> metricFields = new HashSet<>();
        JsonNode metrics = schema.get("metrics");
        if (metrics != null && metrics.isArray()) {
            collectFields(metrics, metricFields);
        }
        return new CompiledSchema(key, rules, metricFields);
    }

    private static FieldType parseType(String t) {
        switch (t.trim().toLowerCase()) {
            case "string":
                return FieldType.STRING;
            case "integer":
            case "int":
            case "long":
                return FieldType.INTEGER;
            case "number":
            case "double":
            case "float":
            case "decimal":
                return FieldType.NUMBER;
            case "boolean":
            case "bool":
                return FieldType.BOOLEAN;
            default:
                return FieldType.ANY;
        }
    }

    /**
     * Returns null if the event is valid against the schema registered under {@code key}, otherwise the
     * error reason. Returns {@code "Schema not found for key: ..."} when no schema is registered.
     */
    public String validate(String key, JsonNode event) {
        CompiledSchema schema = byKey.get(key);
        if (schema == null) {
            return "Schema not found for key: " + key;
        }
        List<String> missing = null;
        List<String> badType = null;
        List<String> badConstraint = null;
        for (FieldRule r : schema.fields) {
            JsonNode v = event.get(r.name);
            boolean present = v != null && !v.isNull();
            if (!present) {
                if (r.required) {
                    missing = add(missing, r.name);
                }
                continue;
            }
            switch (r.type) {
                case STRING:
                    if (!v.isTextual()) {
                        badType = add(badType, r.name + " (expected string)");
                        continue;
                    }
                    break;
                case INTEGER:
                    if (!v.isIntegralNumber()) {
                        badType = add(badType, r.name + " (expected integer)");
                        continue;
                    }
                    break;
                case NUMBER:
                    if (!v.isNumber()) {
                        badType = add(badType, r.name + " (expected number)");
                        continue;
                    }
                    break;
                case BOOLEAN:
                    if (!v.isBoolean()) {
                        badType = add(badType, r.name + " (expected boolean)");
                        continue;
                    }
                    break;
                default:
                    break;
            }
            if (v.isNumber()) {
                if (!Double.isNaN(r.min) && v.asDouble() < r.min) {
                    badConstraint = add(badConstraint, r.name + " (< min " + Values.canonNumber(r.min) + ")");
                }
                if (!Double.isNaN(r.max) && v.asDouble() > r.max) {
                    badConstraint = add(badConstraint, r.name + " (> max " + Values.canonNumber(r.max) + ")");
                }
            }
            if (r.allowed != null && !r.allowed.contains(v.asText())) {
                badConstraint = add(badConstraint, r.name + " (not in allowed_values)");
            }
            if (r.isoTimestamp && v.isTextual() && Values.parseTimestamp(v.asText()) == Values.NO_TIMESTAMP) {
                badConstraint = add(badConstraint, r.name + " (invalid ISO-8601)");
            }
        }
        if (missing == null && badType == null && badConstraint == null) {
            return null;
        }
        List<String> reasons = new ArrayList<>(3);
        if (missing != null) {
            reasons.add("Missing: " + String.join(", ", missing));
        }
        if (badType != null) {
            reasons.add("Invalid Type: " + String.join(", ", badType));
        }
        if (badConstraint != null) {
            reasons.add("Constraint: " + String.join(", ", badConstraint));
        }
        return String.join(" | ", reasons);
    }

    private static List<String> add(List<String> l, String s) {
        if (l == null) {
            l = new ArrayList<>(2);
        }
        l.add(s);
        return l;
    }

    /**
     * Converts a validated JSON event into a {@link ValidatedEvent}, keeping only referenced scalar fields
     * unless {@code projection} is false.
     */
    public ValidatedEvent project(JsonNode event, long eventTimeMs, long ingestTimeMs, boolean projection,
                                  String rawJson) {
        ValidatedEvent.Builder b = ValidatedEvent.builder()
                .entity(event.path("entity_id").asText(null))
                .eventId(event.path("event_id").asText(null))
                .eventType(event.path("event_type").asText("").toLowerCase())
                .source(event.path("source_system").asText(null))
                .time(eventTimeMs)
                .ingest(ingestTimeMs)
                .raw(rawJson);
        Iterator<Map.Entry<String, JsonNode>> it = event.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String name = e.getKey();
            if (projection && !referenced.contains(name)) {
                continue;
            }
            JsonNode v = e.getValue();
            if (v.isTextual()) {
                b.str(name, v.asText());
            } else if (v.isNumber()) {
                b.num(name, v.asDouble());
            } else if (v.isBoolean()) {
                b.bool(name, v.asBoolean());
            }
        }
        return b.build();
    }
}
