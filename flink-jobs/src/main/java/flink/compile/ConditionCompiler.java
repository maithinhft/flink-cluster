package flink.compile;

import com.fasterxml.jackson.databind.JsonNode;
import flink.registry.CanonicalSpec;
import flink.registry.SpecDefinition;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Compiles JSON condition trees into {@link Node} ASTs. Accepts two dialects:
 * <ul>
 *   <li>Generator dialect: {@code {"operator":"AND","children":[...]}}, leaves with
 *       {@code "type":"RAW_FIELD"|"AGGREGATION"|"LINEAR"} and {@code "field"}.</li>
 *   <li>Report dialect: {@code {"type":"LOGIC",...}} and {@code {"type":"LEAF","field_type":"raw"|"metric",
 *       "field_name":...}}; a metric leaf references a schema metric alias.</li>
 * </ul>
 */
public final class ConditionCompiler {

    public static final long DEFAULT_AGG_LOOKBACK_SECONDS = 300L;
    public static final long DEFAULT_ALIAS_LOOKBACK_SECONDS = 86_400L;

    private ConditionCompiler() {
    }

    public static Node compileCondition(JsonNode n) {
        Node root = compile(n, true);
        assignPaths(root, "c");
        return root;
    }

    public static Node compileFilter(JsonNode n) {
        Node root = compile(n, false);
        assignPaths(root, "f");
        return root;
    }

    public static boolean isLogical(JsonNode n) {
        if (n == null || !n.isObject()) {
            return false;
        }
        String type = n.path("type").asText("").trim().toUpperCase();
        if ("LOGIC".equals(type) || "LOGICAL".equals(type)) {
            return true;
        }
        JsonNode children = n.get("children");
        return children != null && children.isArray();
    }

    public static String logicalOp(JsonNode n) {
        String op = n.path("operator").asText("AND").trim().toUpperCase();
        if (!"AND".equals(op) && !"OR".equals(op) && !"NOT".equals(op)) {
            throw new CompileException("Unsupported logical operator: " + op);
        }
        return op;
    }

    public static String leafField(JsonNode n) {
        if (n.hasNonNull("field")) {
            return n.get("field").asText().trim();
        }
        if (n.hasNonNull("field_name")) {
            return n.get("field_name").asText().trim();
        }
        return null;
    }

    static boolean isAggregationLeaf(JsonNode n) {
        String type = n.path("type").asText("").trim().toUpperCase();
        String fieldType = n.path("field_type").asText("").trim().toLowerCase();
        return "AGGREGATION".equals(type) || n.has("function") || n.has("window") || n.has("metric_id")
                || "metric".equals(fieldType) || "aggregation".equals(fieldType);
    }

    private static Node compile(JsonNode n, boolean allowAgg) {
        if (n == null || n.isNull() || n.isMissingNode() || (n.isObject() && n.size() == 0)) {
            return Node.Const.TRUE;
        }
        if (!n.isObject()) {
            throw new CompileException("Condition node must be an object: " + n);
        }
        if (isLogical(n)) {
            String op = logicalOp(n);
            JsonNode children = n.get("children");
            List<Node> compiled = new ArrayList<>();
            if (children != null) {
                for (JsonNode c : children) {
                    compiled.add(compile(c, allowAgg));
                }
            }
            if ("NOT".equals(op)) {
                if (compiled.isEmpty()) {
                    return Node.Const.TRUE;
                }
                Node child = compiled.get(0);
                if (child instanceof Node.Not) {
                    return ((Node.Not) child).child();
                }
                return new Node.Not(child);
            }
            List<Node> flat = new ArrayList<>();
            for (Node c : compiled) {
                if ("AND".equals(op) && c instanceof Node.And) {
                    for (Node g : ((Node.And) c).children()) {
                        flat.add(g);
                    }
                } else if ("OR".equals(op) && c instanceof Node.Or) {
                    for (Node g : ((Node.Or) c).children()) {
                        flat.add(g);
                    }
                } else {
                    flat.add(c);
                }
            }
            if (flat.isEmpty()) {
                return "AND".equals(op) ? Node.Const.TRUE : Node.Const.FALSE;
            }
            if (flat.size() == 1) {
                return flat.get(0);
            }
            Node[] arr = flat.toArray(new Node[0]);
            return "AND".equals(op) ? new Node.And(arr) : new Node.Or(arr);
        }
        String type = n.path("type").asText("").trim().toUpperCase();
        if ("LINEAR".equals(type)) {
            if (!allowAgg) {
                throw new CompileException("LINEAR is not allowed inside an aggregation filter");
            }
            return compileLinear(n);
        }
        if (isAggregationLeaf(n)) {
            if (!allowAgg) {
                throw new CompileException("AGGREGATION is not allowed inside an aggregation filter");
            }
            AggRef ref = compileAggRef(n);
            double[] t = thresholds(n);
            return new AggPredNode(ref, Op.parse(n.path("operator").asText("GTE")), t[0], t[1], t[2]);
        }
        return compileRaw(n);
    }

    private static Node compileRaw(JsonNode n) {
        String field = leafField(n);
        if (field == null || field.isEmpty()) {
            throw new CompileException("Leaf condition without field: " + n);
        }
        Op op = Op.parse(n.has("operator") ? n.get("operator").asText() : null);
        JsonNode value = n.get("value");
        if (op == Op.IN || op == Op.NOT_IN) {
            Set<String> set = new HashSet<>();
            if (value != null && value.isArray()) {
                for (JsonNode x : value) {
                    String c = Values.canonJson(x);
                    if (c != null) {
                        set.add(c);
                    }
                }
            } else if (value != null && value.isTextual() && value.asText().contains(",")) {
                for (String part : value.asText().split(",")) {
                    set.add(part.trim());
                }
            } else if (value != null && !value.isNull()) {
                set.add(Values.canonJson(value));
            }
            return new RawPredNode(field, op, Operand.of(value), null, null, set);
        }
        if (op == Op.BETWEEN) {
            if (value == null || !value.isArray() || value.size() != 2) {
                throw new CompileException("BETWEEN requires a [low, high] array: " + n);
            }
            JsonNode a = value.get(0);
            JsonNode b = value.get(1);
            double da = Values.toDouble(a);
            double db = Values.toDouble(b);
            if (!Double.isNaN(da) && !Double.isNaN(db) && da > db) {
                JsonNode tmp = a;
                a = b;
                b = tmp;
            }
            return new RawPredNode(field, op, Operand.of(null), Operand.of(a), Operand.of(b), null);
        }
        return new RawPredNode(field, op, Operand.of(value), null, null, null);
    }

    static AggRef compileAggRef(JsonNode n) {
        String fieldType = n.path("field_type").asText("").trim().toLowerCase();
        String alias = null;
        if (n.hasNonNull("metric_id")) {
            alias = n.get("metric_id").asText().trim();
        } else if ("metric".equals(fieldType)) {
            alias = leafField(n);
        }
        AggFunction fn = n.hasNonNull("function") ? AggFunction.parse(n.get("function").asText()) : null;
        long defLookback = alias != null ? DEFAULT_ALIAS_LOOKBACK_SECONDS : DEFAULT_AGG_LOOKBACK_SECONDS;
        WindowPlan window = WindowPlan.parse(n.get("window"), defLookback);
        if (alias != null) {
            return new AggRef(null, alias, fn, window);
        }
        String field = n.hasNonNull("field") ? n.get("field").asText() : "event_id";
        SpecDefinition spec = CanonicalSpec.define(field, n.get("filter"));
        return new AggRef(spec, null, fn == null ? AggFunction.COUNT : fn, window);
    }

    private static Node compileLinear(JsonNode n) {
        JsonNode terms = n.get("terms");
        if (terms == null || !terms.isArray() || terms.size() == 0) {
            throw new CompileException("LINEAR requires a non-empty terms array");
        }
        int k = terms.size();
        double[] coefs = new double[k];
        String[] raw = new String[k];
        AggRef[] aggs = new AggRef[k];
        for (int i = 0; i < k; i++) {
            JsonNode t = terms.get(i);
            coefs[i] = t.has("coef") ? Values.toDouble(t.get("coef")) : 1.0;
            if (Double.isNaN(coefs[i])) {
                throw new CompileException("LINEAR coef must be numeric: " + t);
            }
            JsonNode ref = t.has("ref") ? t.get("ref") : t;
            if (isAggregationLeaf(ref)) {
                aggs[i] = compileAggRef(ref);
            } else {
                String f = leafField(ref);
                if (f == null || f.isEmpty()) {
                    throw new CompileException("LINEAR term without field: " + t);
                }
                raw[i] = f;
            }
        }
        double constant = n.has("constant") ? Values.toDouble(n.get("constant")) : 0.0;
        double[] th = thresholds(n);
        return new LinearPredNode(coefs, raw, aggs, Double.isNaN(constant) ? 0.0 : constant,
                Op.parse(n.path("operator").asText("GTE")), th[0], th[1], th[2]);
    }

    private static double[] thresholds(JsonNode n) {
        JsonNode v = n.get("value");
        Op op = Op.parse(n.path("operator").asText("GTE"));
        if (op == Op.BETWEEN) {
            if (v == null || !v.isArray() || v.size() != 2) {
                throw new CompileException("BETWEEN requires a [low, high] array: " + n);
            }
            double lo = Values.toDouble(v.get(0));
            double hi = Values.toDouble(v.get(1));
            if (Double.isNaN(lo) || Double.isNaN(hi)) {
                throw new CompileException("BETWEEN bounds must be numeric: " + n);
            }
            return new double[]{Double.NaN, Math.min(lo, hi), Math.max(lo, hi)};
        }
        if (op == Op.IN || op == Op.NOT_IN || op == Op.CONTAINS) {
            throw new CompileException("Operator " + op + " is not valid for numeric aggregates");
        }
        double th = Values.toDouble(v);
        if (Double.isNaN(th)) {
            throw new CompileException("Aggregate threshold must be numeric: " + n);
        }
        return new double[]{th, Double.NaN, Double.NaN};
    }

    private static void assignPaths(Node n, String path) {
        n.assignPath(path);
        if (n instanceof Node.And) {
            Node[] c = ((Node.And) n).children();
            for (int i = 0; i < c.length; i++) {
                assignPaths(c[i], path + "." + i);
            }
        } else if (n instanceof Node.Or) {
            Node[] c = ((Node.Or) n).children();
            for (int i = 0; i < c.length; i++) {
                assignPaths(c[i], path + "." + i);
            }
        } else if (n instanceof Node.Not) {
            assignPaths(((Node.Not) n).child(), path + ".0");
        }
    }

    public static void collectAggRefs(Node n, List<AggRef> out) {
        if (n instanceof AggPredNode) {
            out.add(((AggPredNode) n).ref());
        } else if (n instanceof LinearPredNode) {
            for (AggRef r : ((LinearPredNode) n).aggTerms()) {
                if (r != null) {
                    out.add(r);
                }
            }
        } else if (n instanceof Node.And) {
            for (Node c : ((Node.And) n).children()) {
                collectAggRefs(c, out);
            }
        } else if (n instanceof Node.Or) {
            for (Node c : ((Node.Or) n).children()) {
                collectAggRefs(c, out);
            }
        } else if (n instanceof Node.Not) {
            collectAggRefs(((Node.Not) n).child(), out);
        }
    }
}
