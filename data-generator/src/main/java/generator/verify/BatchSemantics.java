package generator.verify;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Independent batch re-computation of the realtime segmentation semantics (no code shared with the Flink job).
 * Given every event read back from Kafka (with partition/offset/timestamp), the rule and schema definitions,
 * it derives the set of alerts the streaming engine should have produced:
 *
 * <ol>
 *   <li>Schema validation (required, type, min/max, allowed_values, ISO-8601) and entity/event_time checks.</li>
 *   <li>Arrival order = (Kafka timestamp, topic, partition, offset). A per-partition bounded-out-of-orderness
 *       watermark with idleness is simulated; events whose 5-minute bucket closed (end + tolerance) before they
 *       arrived are late and excluded.</li>
 *   <li>For every trigger event and rule: windowed aggregates over the entity's accepted events that arrived no
 *       later than the trigger, restricted to buckets of the window and to events after the definition's
 *       effective time; AND/OR/NOT/LINEAR evaluation; cooldown in event time.</li>
 * </ol>
 */
public final class BatchSemantics {

    public static final long B = 300_000L;
    private static final ObjectMapper M = new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    public static final class Options {
        public long lateToleranceMs = 60_000L;
        public long outOfOrderMs = 60_000L;
        public long idlenessMs = 60_000L;
        public boolean eventTimeEffective = true;
        public boolean simulateWatermark = true;
    }

    public static final class InEvent {
        public final String topic;
        public final int partition;
        public final long offset;
        public final long kafkaTs;
        public final String json;

        public InEvent(String topic, int partition, long offset, long kafkaTs, String json) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.kafkaTs = kafkaTs;
            this.json = json;
        }
    }

    public static final class RuleRow {
        public final String ruleId;
        public final long version;
        public final String name;
        public final String ruleJson;
        public final long cooldownSeconds;
        public final long updatedAtMs;

        public RuleRow(String ruleId, long version, String name, String ruleJson, long cooldownSeconds, long updatedAtMs) {
            this.ruleId = ruleId;
            this.version = version;
            this.name = name;
            this.ruleJson = ruleJson;
            this.cooldownSeconds = cooldownSeconds;
            this.updatedAtMs = updatedAtMs;
        }
    }

    public record AlertKey(String entityId, String ruleId, String eventId) implements Comparable<AlertKey> {
        @Override
        public int compareTo(AlertKey o) {
            int c = entityId.compareTo(o.entityId);
            if (c != 0) return c;
            c = ruleId.compareTo(o.ruleId);
            return c != 0 ? c : eventId.compareTo(o.eventId);
        }
    }

    public static final class EventInfo {
        public String eventId;
        public String entityId;
        public String eventType;
        public String topic;
        public long kafkaTs;
        public long eventTimeMs;
        public long watermark;
        public long closeTime;
        public boolean dirty;
        public boolean late;
        public int order;
        JsonNode json;
    }

    public static final class Result {
        public final Set<AlertKey> alerts = new TreeSet<>();
        public final Map<String, EventInfo> events = new HashMap<>();
        public final Map<String, Long> ruleMaxSpanMs = new HashMap<>();
        public final Map<String, Long> ruleCooldownMs = new HashMap<>();
        public final List<String> ruleErrors = new ArrayList<>();
        public int valid;
        public int dirty;
        public int late;
    }

    static final class Rule {
        String id;
        long cooldownMs;
        long effectiveFrom;
        boolean requireFull;
        Set<String> triggers = new HashSet<>();
        JsonNode condition;
        long maxSpanMs = B;
    }

    static final class Spec {
        final String source;
        final JsonNode filter;
        long effectiveFrom = Long.MAX_VALUE;

        Spec(String source, JsonNode filter) {
            this.source = source;
            this.filter = filter;
        }
    }

    private final Options options;
    private final Map<String, JsonNode> schemas = new HashMap<>();
    private final Map<String, ObjectNode> aliases = new HashMap<>();
    private final Map<String, Spec> specs = new HashMap<>();

    public BatchSemantics(Options options) {
        this.options = options;
    }

    public Result run(List<InEvent> input, Map<String, String> schemaPayloads, List<RuleRow> ruleRows) throws Exception {
        Result result = new Result();
        for (Map.Entry<String, String> e : schemaPayloads.entrySet()) {
            JsonNode s = M.readTree(e.getValue());
            schemas.put(e.getKey() + "_" + s.path("version").asText(), s);
            for (JsonNode m : s.path("metrics")) {
                if (m.hasNonNull("metric_id")) {
                    aliases.put(m.get("metric_id").asText(), (ObjectNode) m);
                }
            }
        }
        List<Rule> rules = new ArrayList<>();
        for (RuleRow row : ruleRows) {
            try {
                Rule r = parseRule(row);
                rules.add(r);
                result.ruleMaxSpanMs.put(r.id, r.maxSpanMs);
                result.ruleCooldownMs.put(r.id, r.cooldownMs);
            } catch (Exception ex) {
                result.ruleErrors.add(row.ruleId + ": " + ex.getMessage());
            }
        }

        List<InEvent> ordered = new ArrayList<>(input);
        ordered.sort(Comparator.<InEvent>comparingLong(e -> e.kafkaTs).thenComparing(e -> e.topic)
                .thenComparingInt(e -> e.partition).thenComparingLong(e -> e.offset));

        Map<String, long[]> partitions = new HashMap<>();
        Map<String, List<EventInfo>> history = new HashMap<>();
        Map<String, Long> cooldownUntil = new HashMap<>();
        int order = 0;
        for (InEvent in : ordered) {
            long wm = watermark(partitions, in.kafkaTs);
            String pk = in.topic + "#" + in.partition;
            EventInfo info = validate(in);
            info.order = order++;
            info.watermark = wm;
            if (info.eventId != null) {
                result.events.put(info.eventId, info);
            }
            long ts = info.eventTimeMs != Long.MIN_VALUE ? info.eventTimeMs : in.kafkaTs;
            long[] p = partitions.computeIfAbsent(pk, k -> new long[]{Long.MIN_VALUE, 0L});
            p[0] = Math.max(p[0], ts);
            p[1] = in.kafkaTs;
            if (info.dirty) {
                result.dirty++;
                continue;
            }
            long bucketStart = Math.floorDiv(info.eventTimeMs, B) * B;
            info.closeTime = bucketStart + B + options.lateToleranceMs;
            if (options.simulateWatermark && wm != Long.MIN_VALUE && wm >= info.closeTime) {
                info.late = true;
                result.late++;
                continue;
            }
            result.valid++;
            List<EventInfo> h = history.computeIfAbsent(info.entityId, k -> new ArrayList<>());
            h.add(info);
            for (Rule r : rules) {
                if (!r.triggers.contains(info.eventType)) {
                    continue;
                }
                String ck = info.entityId + "|" + r.id;
                Long until = cooldownUntil.get(ck);
                if (r.cooldownMs > 0 && until != null && info.eventTimeMs < until) {
                    continue;
                }
                Ctx ctx = new Ctx(info, h);
                boolean ok = eval(r.condition, ctx, true);
                if (ok && r.requireFull && !ctx.complete) {
                    ok = false;
                }
                if (ok) {
                    result.alerts.add(new AlertKey(info.entityId, r.id, info.eventId));
                    if (r.cooldownMs > 0) {
                        cooldownUntil.put(ck, info.eventTimeMs + r.cooldownMs);
                    }
                }
            }
        }
        return result;
    }

    private long watermark(Map<String, long[]> partitions, long nowKafkaTs) {
        long wm = Long.MAX_VALUE;
        boolean any = false;
        for (long[] p : partitions.values()) {
            if (nowKafkaTs - p[1] > options.idlenessMs) {
                continue;
            }
            any = true;
            wm = Math.min(wm, p[0] == Long.MIN_VALUE ? Long.MIN_VALUE : p[0] - options.outOfOrderMs - 1);
        }
        return any ? wm : Long.MIN_VALUE;
    }

    private EventInfo validate(InEvent in) {
        EventInfo info = new EventInfo();
        info.topic = in.topic;
        info.kafkaTs = in.kafkaTs;
        info.eventTimeMs = Long.MIN_VALUE;
        JsonNode ev;
        try {
            ev = M.readTree(in.json);
        } catch (Exception e) {
            info.dirty = true;
            return info;
        }
        if (ev == null || !ev.isObject()) {
            info.dirty = true;
            return info;
        }
        info.json = ev;
        info.eventId = ev.path("event_id").asText(null);
        info.entityId = ev.path("entity_id").asText(null);
        info.eventType = ev.path("event_type").asText("").toLowerCase();
        if (ev.hasNonNull("event_time")) {
            info.eventTimeMs = parseTs(ev.get("event_time").asText());
        }
        if (!ev.hasNonNull("source_system") || !ev.hasNonNull("event_type") || !ev.hasNonNull("schema_version")) {
            info.dirty = true;
            return info;
        }
        JsonNode schema = schemas.get(ev.get("source_system").asText() + "_" + ev.get("event_type").asText() + "_"
                + ev.get("schema_version").asText());
        if (schema == null || !conforms(ev, schema)) {
            info.dirty = true;
            return info;
        }
        long t = ev.hasNonNull("event_time") ? parseTs(ev.get("event_time").asText()) : Long.MIN_VALUE;
        if (info.entityId == null || info.entityId.isEmpty() || t == Long.MIN_VALUE) {
            info.dirty = true;
            return info;
        }
        info.eventTimeMs = t;
        return info;
    }

    static boolean conforms(JsonNode ev, JsonNode schema) {
        Iterator<Map.Entry<String, JsonNode>> it = schema.path("fields").fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> f = it.next();
            JsonNode def = f.getValue();
            JsonNode v = ev.get(f.getKey());
            if (v == null || v.isNull()) {
                if (def.path("required").asBoolean(false)) return false;
                continue;
            }
            switch (def.path("type").asText("").toLowerCase()) {
                case "string": if (!v.isTextual()) return false; break;
                case "integer": case "int": case "long": if (!v.isIntegralNumber()) return false; break;
                case "number": case "double": case "float": case "decimal": if (!v.isNumber()) return false; break;
                case "boolean": case "bool": if (!v.isBoolean()) return false; break;
                default: break;
            }
            if (v.isNumber() && def.has("min") && v.asDouble() < def.get("min").asDouble()) return false;
            if (v.isNumber() && def.has("max") && v.asDouble() > def.get("max").asDouble()) return false;
            if (def.has("allowed_values")) {
                boolean found = false;
                for (JsonNode a : def.get("allowed_values")) {
                    if (a.asText().equals(v.asText())) {
                        found = true;
                        break;
                    }
                }
                if (!found) return false;
            }
            if ("ISO-8601".equalsIgnoreCase(def.path("format").asText("")) && v.isTextual()
                    && parseTs(v.asText()) == Long.MIN_VALUE) {
                return false;
            }
        }
        return true;
    }

    private Rule parseRule(RuleRow row) throws Exception {
        JsonNode doc = M.readTree(row.ruleJson);
        Rule r = new Rule();
        r.id = row.ruleId;
        r.cooldownMs = row.cooldownSeconds * 1000L;
        r.effectiveFrom = options.eventTimeEffective ? row.updatedAtMs : Long.MIN_VALUE;
        if (doc.hasNonNull("effective_from")) {
            JsonNode ef = doc.get("effective_from");
            r.effectiveFrom = ef.isNumber() ? ef.asLong() : parseTs(ef.asText());
        }
        r.requireFull = doc.path("require_full_window").asBoolean(false);
        addTriggers(doc.get("trigger_events"), r.triggers);
        addTriggers(doc.get("trigger_event"), r.triggers);
        r.condition = doc.has("condition") ? doc.get("condition") : doc.get("condition_tree");
        r.maxSpanMs = registerSpecs(r.condition, r.effectiveFrom);
        return r;
    }

    private static void addTriggers(JsonNode n, Set<String> out) {
        if (n == null || n.isNull()) return;
        if (n.isArray()) {
            for (JsonNode x : n) add(x.asText(), out);
        } else {
            for (String p : n.asText().split(",")) add(p, out);
        }
    }

    private static void add(String s, Set<String> out) {
        String v = s.trim().toLowerCase();
        if (!v.isEmpty()) out.add(v);
    }

    private long registerSpecs(JsonNode n, long eff) throws Exception {
        if (n == null || n.isNull() || !n.isObject()) return B;
        long span = B;
        if (isLogical(n)) {
            for (JsonNode c : n.path("children")) span = Math.max(span, registerSpecs(c, eff));
            return span;
        }
        if ("LINEAR".equalsIgnoreCase(n.path("type").asText())) {
            for (JsonNode t : n.path("terms")) {
                JsonNode ref = t.has("ref") ? t.get("ref") : t;
                if (isAgg(ref)) span = Math.max(span, registerAgg(ref, eff));
            }
            return span;
        }
        if (isAgg(n)) return registerAgg(n, eff);
        return span;
    }

    private long registerAgg(JsonNode n, long eff) throws Exception {
        Spec s = spec(n);
        if (s != null) s.effectiveFrom = Math.min(s.effectiveFrom, eff);
        long[] w = windows(n, 1_000_000L);
        long min = w[0];
        for (int i = 0; i < w.length; i += 2) min = Math.min(min, w[i]);
        return (1_000_000L - min + 1) * B;
    }

    private Spec spec(JsonNode aggNode) throws Exception {
        String alias = alias(aggNode);
        String source;
        JsonNode filter;
        if (alias != null) {
            ObjectNode m = aliases.get(alias);
            if (m == null) return null;
            source = m.path("source_field").asText("event_id");
            filter = m.get("filter");
        } else {
            source = aggNode.path("field").asText("event_id");
            filter = aggNode.get("filter");
        }
        String src = source.trim();
        if (src.isEmpty() || src.equals("*") || src.equalsIgnoreCase("event_id") || src.equalsIgnoreCase("events")) {
            src = "*";
        }
        String key = src + "|" + (filter == null || filter.isNull() ? "*" : M.writeValueAsString(M.treeToValue(filter, Object.class)));
        return specs.computeIfAbsent(key, k -> new Spec(k.substring(0, k.indexOf('|')), filter));
    }

    private static String alias(JsonNode n) {
        if (n.hasNonNull("metric_id")) return n.get("metric_id").asText();
        if ("metric".equalsIgnoreCase(n.path("field_type").asText())) {
            return n.hasNonNull("field") ? n.get("field").asText() : n.path("field_name").asText(null);
        }
        return null;
    }

    static boolean isLogical(JsonNode n) {
        String t = n.path("type").asText("").toUpperCase();
        return "LOGIC".equals(t) || "LOGICAL".equals(t) || (n.has("children") && n.get("children").isArray());
    }

    static boolean isAgg(JsonNode n) {
        String t = n.path("type").asText("").toUpperCase();
        String ft = n.path("field_type").asText("").toLowerCase();
        return "AGGREGATION".equals(t) || n.has("function") || n.has("window") || n.has("metric_id")
                || "metric".equals(ft) || "aggregation".equals(ft);
    }

    final class Ctx {
        final EventInfo trigger;
        final List<EventInfo> history;
        boolean complete = true;

        Ctx(EventInfo trigger, List<EventInfo> history) {
            this.trigger = trigger;
            this.history = history;
        }
    }

    private boolean eval(JsonNode n, Ctx ctx, boolean allowAgg) throws Exception {
        if (n == null || n.isNull() || (n.isObject() && n.size() == 0)) return true;
        if (isLogical(n)) {
            String op = n.path("operator").asText("AND").toUpperCase();
            JsonNode ch = n.path("children");
            switch (op) {
                case "NOT":
                    return ch.size() == 0 || !eval(ch.get(0), ctx, allowAgg);
                case "OR":
                    for (JsonNode c : ch) if (eval(c, ctx, allowAgg)) return true;
                    return false;
                default:
                    for (JsonNode c : ch) if (!eval(c, ctx, allowAgg)) return false;
                    return true;
            }
        }
        if ("LINEAR".equalsIgnoreCase(n.path("type").asText())) {
            double total = n.has("constant") ? num(n.get("constant")) : 0.0;
            for (JsonNode t : n.path("terms")) {
                double coef = t.has("coef") ? num(t.get("coef")) : 1.0;
                JsonNode ref = t.has("ref") ? t.get("ref") : t;
                Double v;
                if (isAgg(ref)) {
                    long[] w = windows(ref, bucket(ctx.trigger.eventTimeMs));
                    v = aggValue(ref, ctx, w[0], w[1]);
                } else {
                    String f = ref.has("field") ? ref.get("field").asText() : ref.path("field_name").asText();
                    JsonNode x = ctx.trigger.json.get(f);
                    v = x == null || x.isNull() ? null : numeric(x);
                }
                if (v == null || Double.isNaN(v)) return false;
                total += coef * v;
            }
            return testNumber(total, n);
        }
        if (isAgg(n)) {
            return evalAgg(n, ctx);
        }
        return evalRaw(n, ctx.trigger.json);
    }

    private boolean evalAgg(JsonNode n, Ctx ctx) throws Exception {
        long tb = bucket(ctx.trigger.eventTimeMs);
        long[] w = windows(n, tb);
        Spec s = spec(n);
        if (s == null) {
            ctx.complete = false;
            return false;
        }
        if (w[0] * B < s.effectiveFrom) {
            ctx.complete = false;
        }
        String quantifier = quantifier(n);
        boolean any = false;
        boolean all = true;
        for (int i = 0; i < w.length; i += 2) {
            boolean ok = testValue(aggValue(n, ctx, w[i], w[i + 1]), n);
            if (i == 0 && "LATEST".equals(quantifier)) return ok;
            any |= ok;
            all &= ok;
        }
        return "ALL".equals(quantifier) ? all : any;
    }

    private Double aggValue(JsonNode n, Ctx ctx, long startBucket, long endBucket) throws Exception {
        Spec s = spec(n);
        if (s == null) return null;
        String fn = n.hasNonNull("function") ? n.get("function").asText().toUpperCase() : null;
        if (fn == null) {
            String alias = alias(n);
            ObjectNode m = alias == null ? null : aliases.get(alias);
            fn = m == null ? "COUNT" : m.path("aggregation").asText("COUNT").toUpperCase();
        }
        double sum = 0;
        long count = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (EventInfo e : ctx.history) {
            long b = bucket(e.eventTimeMs);
            if (b < startBucket || b > endBucket || e.eventTimeMs < s.effectiveFrom) continue;
            if (s.filter != null && !s.filter.isNull() && !evalFilter(s.filter, e.json)) continue;
            double v;
            if ("*".equals(s.source)) {
                v = 1.0;
            } else {
                JsonNode x = e.json.get(s.source);
                if (x == null || x.isNull()) continue;
                Double d = numeric(x);
                if (d == null || Double.isNaN(d)) continue;
                v = d;
            }
            sum += v;
            count++;
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        switch (fn) {
            case "SUM": return count == 0 ? 0.0 : sum;
            case "COUNT": return (double) count;
            case "AVG": return count == 0 ? null : sum / count;
            case "MIN": return count == 0 ? null : min;
            case "MAX": return count == 0 ? null : max;
            default: throw new IllegalArgumentException("function " + fn);
        }
    }

    private boolean evalFilter(JsonNode n, JsonNode ev) throws Exception {
        if (n == null || n.isNull() || (n.isObject() && n.size() == 0)) return true;
        if (isLogical(n)) {
            String op = n.path("operator").asText("AND").toUpperCase();
            JsonNode ch = n.path("children");
            if ("NOT".equals(op)) return ch.size() == 0 || !evalFilter(ch.get(0), ev);
            if ("OR".equals(op)) {
                for (JsonNode c : ch) if (evalFilter(c, ev)) return true;
                return false;
            }
            for (JsonNode c : ch) if (!evalFilter(c, ev)) return false;
            return true;
        }
        return evalRaw(n, ev);
    }

    static String op(JsonNode n, String def) {
        String s = n.path("operator").asText(def).trim().toUpperCase().replace(' ', '_');
        switch (s) {
            case "=": case "==": return "EQ";
            case "!=": case "<>": case "NE": return "NEQ";
            case ">": return "GT";
            case ">=": case "GE": return "GTE";
            case "<": return "LT";
            case "<=": case "LE": return "LTE";
            case "NIN": return "NOT_IN";
            default: return s;
        }
    }

    private static boolean evalRaw(JsonNode n, JsonNode ev) {
        String field = n.has("field") ? n.get("field").asText() : n.path("field_name").asText();
        String op = op(n, "EQ");
        JsonNode v = ev.get(field);
        JsonNode target = n.get("value");
        if (v == null || v.isNull()) {
            return "NEQ".equals(op) || "NOT_IN".equals(op);
        }
        String actual = canon(v);
        switch (op) {
            case "EQ": return actual.equals(canon(target));
            case "NEQ": return !actual.equals(canon(target));
            case "IN":
            case "NOT_IN": {
                Set<String> set = new HashSet<>();
                if (target != null && target.isArray()) {
                    for (JsonNode x : target) set.add(canon(x));
                } else if (target != null && target.isTextual() && target.asText().contains(",")) {
                    for (String p : target.asText().split(",")) set.add(p.trim());
                } else if (target != null && !target.isNull()) {
                    set.add(canon(target));
                }
                return "IN".equals(op) == set.contains(actual);
            }
            case "CONTAINS": return actual.contains(canon(target));
            case "BETWEEN": {
                JsonNode lo = target.get(0);
                JsonNode hi = target.get(1);
                double a = num(lo);
                double b = num(hi);
                if (!Double.isNaN(a) && !Double.isNaN(b) && a > b) {
                    JsonNode t = lo;
                    lo = hi;
                    hi = t;
                }
                Integer c1 = compare(v, lo);
                Integer c2 = compare(v, hi);
                return c1 != null && c2 != null && c1 >= 0 && c2 <= 0;
            }
            default: {
                Integer c = compare(v, target);
                if (c == null) return false;
                switch (op) {
                    case "GT": return c > 0;
                    case "GTE": return c >= 0;
                    case "LT": return c < 0;
                    case "LTE": return c <= 0;
                    default: throw new IllegalArgumentException("operator " + op);
                }
            }
        }
    }

    private static Integer compare(JsonNode actual, JsonNode target) {
        double tn = num(target);
        long tts = target != null && target.isTextual() ? parseTs(target.asText()) : Long.MIN_VALUE;
        if (actual.isNumber() || actual.isBoolean()) {
            double a = actual.isBoolean() ? (actual.asBoolean() ? 1 : 0) : actual.asDouble();
            if (!Double.isNaN(tn)) return Double.compare(a, tn);
            if (tts != Long.MIN_VALUE) return Double.compare(a, (double) tts);
            return null;
        }
        String s = actual.asText();
        if (tts != Long.MIN_VALUE) {
            long t = parseTs(s);
            if (t != Long.MIN_VALUE) return Long.compare(t, tts);
        }
        if (!Double.isNaN(tn)) {
            double d = parseDouble(s);
            return Double.isNaN(d) ? null : Double.compare(d, tn);
        }
        String c = canon(target);
        return c == null ? null : s.compareTo(c);
    }

    private boolean testValue(Double value, JsonNode n) {
        return value != null && testNumber(value, n);
    }

    private static boolean testNumber(double value, JsonNode n) {
        String op = op(n, "GTE");
        JsonNode t = n.get("value");
        if ("BETWEEN".equals(op)) {
            double a = num(t.get(0));
            double b = num(t.get(1));
            return value >= Math.min(a, b) && value <= Math.max(a, b);
        }
        int c = Double.compare(value, num(t));
        switch (op) {
            case "EQ": return c == 0;
            case "NEQ": return c != 0;
            case "GT": return c > 0;
            case "GTE": return c >= 0;
            case "LT": return c < 0;
            case "LTE": return c <= 0;
            default: throw new IllegalArgumentException("operator " + op);
        }
    }

    static String quantifier(JsonNode n) {
        JsonNode w = n.get("window");
        long[] p = windowParams(n);
        String def = p[3] > p[1] && p[0] != 0 ? "ANY" : "LATEST";
        return w != null && w.hasNonNull("quantifier") ? w.get("quantifier").asText().toUpperCase() : def;
    }

    /** [mode(0=lookback,1=tumbling,2=sliding), sizeB, slideB, lookB] */
    static long[] windowParams(JsonNode n) {
        long defLook = alias(n) != null ? 86_400L : 300L;
        JsonNode w = n.get("window");
        if (w == null || !w.isObject()) {
            return new long[]{0, defLook / 300, 1, defLook / 300};
        }
        String type = w.path("type").asText("").toLowerCase();
        long size = w.path("size_seconds").asLong(0);
        long slide = w.path("slide_seconds").asLong(0);
        long look = w.path("lookback_seconds").asLong(0);
        if (type.isEmpty() || "lookback".equals(type)) {
            long l = look > 0 ? look : (size > 0 ? size : defLook);
            return new long[]{0, l / 300, 1, l / 300};
        }
        long mode = "tumbling".equals(type) ? 1 : 2;
        if (size <= 0) size = look > 0 ? look : defLook;
        if (mode == 1) slide = size;
        else if (slide <= 0) slide = 300;
        if (look < size) look = size;
        return new long[]{mode, size / 300, slide / 300, look / 300};
    }

    static long[] windows(JsonNode n, long tb) {
        long[] p = windowParams(n);
        long mode = p[0], size = p[1], slide = p[2], look = p[3];
        long horizon = tb - look + 1;
        if (mode == 0) return new long[]{horizon, tb};
        long ls;
        long le;
        if (mode == 1) {
            ls = Math.floorDiv(tb, size) * size;
            le = tb;
        } else {
            long endEx = Math.floorDiv(tb, slide) * slide + slide;
            ls = endEx - size;
            le = Math.min(endEx - 1, tb);
        }
        if ("LATEST".equals(quantifier(n))) return new long[]{ls, le};
        long step = mode == 1 ? size : slide;
        List<Long> out = new ArrayList<>();
        out.add(ls);
        out.add(le);
        for (long s = ls - step; s >= horizon; s -= step) {
            out.add(s);
            out.add(s + size - 1);
        }
        long[] r = new long[out.size()];
        for (int i = 0; i < r.length; i++) r[i] = out.get(i);
        return r;
    }

    static long bucket(long t) {
        return Math.floorDiv(t, B);
    }

    static String canon(JsonNode n) {
        if (n == null || n.isNull()) return null;
        if (n.isNumber()) return canonNumber(n.asDouble());
        if (n.isBoolean()) return n.asBoolean() ? "true" : "false";
        if (n.isValueNode()) return n.asText();
        return n.toString();
    }

    static String canonNumber(double d) {
        if (d == Math.rint(d) && !Double.isInfinite(d) && Math.abs(d) < 1e15) return Long.toString((long) d);
        return Double.toString(d);
    }

    static double num(JsonNode n) {
        if (n == null || n.isNull()) return Double.NaN;
        if (n.isNumber()) return n.asDouble();
        return n.isTextual() ? parseDouble(n.asText()) : Double.NaN;
    }

    static Double numeric(JsonNode n) {
        if (n.isNumber()) return n.asDouble();
        if (n.isTextual()) {
            long t = parseTs(n.asText());
            if (t != Long.MIN_VALUE) return (double) t;
            double d = parseDouble(n.asText());
            return Double.isNaN(d) ? null : d;
        }
        return null;
    }

    static double parseDouble(String s) {
        try {
            return Double.parseDouble(s.trim());
        } catch (Exception e) {
            return Double.NaN;
        }
    }

    static long parseTs(String s) {
        if (s == null || s.length() < 10 || s.charAt(4) != '-') return Long.MIN_VALUE;
        try {
            return Instant.parse(s).toEpochMilli();
        } catch (Exception ignored) {
        }
        try {
            return OffsetDateTime.parse(s).toInstant().toEpochMilli();
        } catch (Exception ignored) {
        }
        if (s.length() == 10) {
            try {
                return LocalDate.parse(s).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
            } catch (Exception ignored) {
            }
        }
        return Long.MIN_VALUE;
    }
}
