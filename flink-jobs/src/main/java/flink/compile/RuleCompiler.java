package flink.compile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Compiles a rule row (Debezium-unwrapped {@code rule_definitions} record, bootstrap row, or a plain
 * rule document) into a {@link CompiledRule}.
 */
public final class RuleCompiler {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RuleCompiler() {
    }

    public static CompiledRule compile(String payload, long version, long effectiveFromMs) {
        JsonNode row;
        try {
            row = MAPPER.readTree(payload);
        } catch (Exception e) {
            throw new CompileException("Rule payload is not valid JSON: " + e.getMessage());
        }
        return compile(row, version, effectiveFromMs);
    }

    public static CompiledRule compile(JsonNode row, long version, long effectiveFromMs) {
        if (row == null || !row.isObject() || !row.hasNonNull("rule_id")) {
            throw new CompileException("Rule payload without rule_id");
        }
        String ruleId = row.get("rule_id").asText();
        String name = row.hasNonNull("name") ? row.get("name").asText() : ruleId;
        boolean enabled = !row.has("enabled") || row.get("enabled").isNull() || parseBool(row.get("enabled"));
        long cooldownSeconds = row.hasNonNull("cooldown_seconds") ? row.get("cooldown_seconds").asLong() : 0L;
        if (cooldownSeconds < 0) {
            throw new CompileException("cooldown_seconds must be >= 0");
        }

        JsonNode doc = row;
        if (row.has("rule_json")) {
            doc = row.get("rule_json");
            if (doc.isTextual()) {
                try {
                    doc = MAPPER.readTree(doc.asText());
                } catch (Exception e) {
                    throw new CompileException("rule_json is not valid JSON: " + e.getMessage());
                }
            }
        }
        if (doc == null || !doc.isObject()) {
            throw new CompileException("rule_json must be an object");
        }

        Set<String> triggers = new HashSet<>();
        parseTriggers(doc.get("trigger_events"), triggers);
        parseTriggers(doc.get("trigger_event"), triggers);
        if (doc != row) {
            parseTriggers(row.get("trigger_events"), triggers);
            parseTriggers(row.get("trigger_event"), triggers);
        }

        JsonNode condition;
        if (doc.has("condition")) {
            condition = doc.get("condition");
        } else if (doc.has("condition_tree")) {
            condition = doc.get("condition_tree");
        } else if (doc != row && (ConditionCompiler.isLogical(doc) || doc.has("type") || doc.has("field"))) {
            condition = doc;
        } else {
            condition = null;
        }
        Node root = ConditionCompiler.compileCondition(condition);
        List<AggRef> refs = new ArrayList<>();
        ConditionCompiler.collectAggRefs(root, refs);

        long eff = effectiveFromMs;
        if (doc.hasNonNull("effective_from")) {
            JsonNode ef = doc.get("effective_from");
            long parsed = ef.isNumber() ? ef.asLong() : Values.parseTimestamp(ef.asText());
            if (parsed != Values.NO_TIMESTAMP) {
                eff = parsed;
            }
        }
        boolean requireFull = doc.has("require_full_window") && parseBool(doc.get("require_full_window"));

        return new CompiledRule(ruleId, name, version, enabled, cooldownSeconds * 1000L, eff, requireFull,
                Collections.unmodifiableSet(triggers), root, Collections.unmodifiableList(refs));
    }

    static boolean parseBool(JsonNode n) {
        if (n.isBoolean()) {
            return n.asBoolean();
        }
        String s = n.asText("").trim();
        return "true".equalsIgnoreCase(s) || "t".equalsIgnoreCase(s) || "1".equals(s);
    }

    static void parseTriggers(JsonNode node, Set<String> out) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return;
        }
        if (node.isArray()) {
            for (JsonNode item : node) {
                if (item.isTextual()) {
                    addTrigger(item.asText(), out);
                }
            }
        } else if (node.isTextual()) {
            for (String part : node.asText().split(",")) {
                addTrigger(part, out);
            }
        }
    }

    private static void addTrigger(String raw, Set<String> out) {
        String v = raw.trim().toLowerCase();
        if (!v.isEmpty()) {
            out.add(v);
        }
    }
}
