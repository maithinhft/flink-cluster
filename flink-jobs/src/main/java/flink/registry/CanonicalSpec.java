package flink.registry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import flink.compile.CompileException;
import flink.compile.ConditionCompiler;
import flink.compile.Node;
import flink.compile.Op;
import flink.compile.RawPredNode;
import flink.compile.Values;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Builds the canonical form of an aggregation spec. Logically equal filters (children in a different
 * order, nested AND inside AND, IN lists in a different order, 5 vs 5.0, NOT(NOT x)) map to the same
 * canonical string and therefore the same {@code specKey}.
 */
public final class CanonicalSpec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CanonicalSpec() {
    }

    public static SpecDefinition define(String field, JsonNode filterJson) {
        String source = canonSourceField(field);
        String filterCanon = canonFilter(filterJson);
        String canonical = "src=" + source + ";filter=" + filterCanon;
        Node filter = ConditionCompiler.compileFilter(filterJson);
        return new SpecDefinition(hash64(canonical), canonical, source, filter, requiredEventTypes(filter));
    }

    public static String canonSourceField(String field) {
        if (field == null) {
            return SpecDefinition.ALL_EVENTS;
        }
        String f = field.trim();
        if (f.isEmpty() || "*".equals(f) || "event_id".equalsIgnoreCase(f) || "events".equalsIgnoreCase(f)) {
            return SpecDefinition.ALL_EVENTS;
        }
        return f;
    }

    public static String canonFilter(JsonNode n) {
        if (n == null || n.isNull() || n.isMissingNode()) {
            return "*";
        }
        if (n.isObject() && n.size() == 0) {
            return "*";
        }
        if (ConditionCompiler.isLogical(n)) {
            String op = ConditionCompiler.logicalOp(n);
            JsonNode children = n.get("children");
            if ("NOT".equals(op)) {
                if (children == null || children.size() == 0) {
                    return "*";
                }
                String inner = canonFilter(children.get(0));
                if (inner.startsWith("NOT(") && inner.endsWith(")")) {
                    return inner.substring(4, inner.length() - 1);
                }
                return "NOT(" + inner + ")";
            }
            TreeSet<String> parts = new TreeSet<>();
            collectParts(n, op, parts);
            parts.remove("*");
            if (parts.isEmpty()) {
                return "*";
            }
            if (parts.size() == 1) {
                return parts.first();
            }
            return op + "(" + String.join(",", parts) + ")";
        }
        return canonLeaf(n);
    }

    private static void collectParts(JsonNode n, String op, Set<String> out) {
        JsonNode children = n.get("children");
        if (children == null) {
            return;
        }
        for (JsonNode c : children) {
            if (ConditionCompiler.isLogical(c) && op.equals(ConditionCompiler.logicalOp(c))) {
                collectParts(c, op, out);
            } else {
                out.add(canonFilter(c));
            }
        }
    }

    private static String canonLeaf(JsonNode n) {
        String field = ConditionCompiler.leafField(n);
        if (field == null || field.isEmpty()) {
            throw new CompileException("Filter leaf without field: " + n);
        }
        Op op = Op.parse(n.has("operator") ? n.get("operator").asText() : null);
        JsonNode value = n.get("value");
        String v;
        if (op == Op.IN || op == Op.NOT_IN) {
            TreeSet<String> vals = new TreeSet<>();
            if (value != null && value.isArray()) {
                for (JsonNode x : value) {
                    vals.add(Values.canonJson(x));
                }
            } else if (value != null) {
                vals.add(Values.canonJson(value));
            }
            v = String.join("\u0001", vals);
        } else if (op == Op.BETWEEN && value != null && value.isArray() && value.size() == 2) {
            double a = Values.toDouble(value.get(0));
            double b = Values.toDouble(value.get(1));
            if (!Double.isNaN(a) && !Double.isNaN(b) && a > b) {
                v = Values.canonNumber(b) + "\u0001" + Values.canonNumber(a);
            } else {
                v = Values.canonJson(value.get(0)) + "\u0001" + Values.canonJson(value.get(1));
            }
        } else {
            v = Values.canonJson(value);
        }
        try {
            return field + "|" + op.name() + "|" + MAPPER.writeValueAsString(v);
        } catch (Exception e) {
            throw new CompileException("Cannot canonicalise value " + v);
        }
    }

    public static Set<String> requiredEventTypes(Node n) {
        if (n instanceof RawPredNode) {
            RawPredNode r = (RawPredNode) n;
            if (!"event_type".equals(r.field())) {
                return null;
            }
            if (r.op() == Op.EQ && r.operand().canon() != null) {
                Set<String> s = new HashSet<>();
                s.add(r.operand().canon().toLowerCase());
                return s;
            }
            if (r.op() == Op.IN) {
                Set<String> s = new HashSet<>();
                for (String x : r.inValues()) {
                    if (x != null) {
                        s.add(x.toLowerCase());
                    }
                }
                return s;
            }
            return null;
        }
        if (n instanceof Node.And) {
            Set<String> acc = null;
            for (Node c : ((Node.And) n).children()) {
                Set<String> s = requiredEventTypes(c);
                if (s == null) {
                    continue;
                }
                if (acc == null) {
                    acc = new HashSet<>(s);
                } else {
                    acc.retainAll(s);
                }
            }
            return acc;
        }
        if (n instanceof Node.Or) {
            Set<String> acc = new HashSet<>();
            for (Node c : ((Node.Or) n).children()) {
                Set<String> s = requiredEventTypes(c);
                if (s == null) {
                    return null;
                }
                acc.addAll(s);
            }
            return acc;
        }
        return null;
    }

    public static long hash64(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] d = md.digest(s.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (d[i] & 0xffL);
            }
            return h;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
