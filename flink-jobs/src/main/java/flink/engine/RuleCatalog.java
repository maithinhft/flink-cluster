package flink.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import flink.compile.AggFunction;
import flink.compile.AggRef;
import flink.compile.CompileException;
import flink.compile.CompiledRule;
import flink.compile.RuleCompiler;
import flink.index.InvertedIndex;
import flink.model.ControlMessage;
import flink.model.ValidatedEvent;
import flink.registry.CanonicalSpec;
import flink.registry.MetricRegistry;
import flink.registry.SpecDefinition;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Broadcast-side catalog of compiled rules, the metric registry and the inverted index.
 * All mutations happen while processing one control message, so events never observe a half-applied
 * change (atomic hot reload).
 *
 * <p>Versioning: an upsert is applied only if its version is greater than the current one (or the
 * tombstone of a deleted rule); a delete only removes the rule if its version is &gt;= the current one.
 * Late or duplicated CDC messages are therefore harmless.
 */
public final class RuleCatalog {

    public enum Status { APPLIED, REMOVED, IGNORED_STALE, REJECTED }

    public static final class Result {
        public final Status status;
        public final String error;
        public final CompiledRule rule;

        Result(Status status, String error, CompiledRule rule) {
            this.status = status;
            this.error = error;
            this.rule = rule;
        }
    }

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final EngineConfig config;
    private final MetricRegistry registry = new MetricRegistry();
    private final InvertedIndex index;
    private final Map<String, CompiledRule> rules = new HashMap<>();
    private final Map<String, Long> tombstones = new HashMap<>();
    private final List<CompiledRule> byIdx = new ArrayList<>();
    private final ArrayDeque<Integer> freeIdx = new ArrayDeque<>();
    private final Set<String> pendingAliasRules = new LinkedHashSet<>();
    private final Map<String, String> schemaVersions = new HashMap<>();

    public RuleCatalog(EngineConfig config) {
        this.config = config;
        this.index = new InvertedIndex(config.indexMode);
    }

    public MetricRegistry registry() {
        return registry;
    }

    public InvertedIndex index() {
        return index;
    }

    public int ruleCount() {
        return rules.size();
    }

    public CompiledRule rule(String ruleId) {
        return rules.get(ruleId);
    }

    public CompiledRule ruleAt(int idx) {
        return idx >= 0 && idx < byIdx.size() ? byIdx.get(idx) : null;
    }

    public Long tombstone(String ruleId) {
        return tombstones.get(ruleId);
    }

    public RoaringBitmap candidates(ValidatedEvent ev) {
        return index.candidates(ev);
    }

    public Result apply(ControlMessage m, long nowMs) {
        if (m.isRule()) {
            return applyRule(m, nowMs);
        }
        return applySchema(m, nowMs);
    }

    private long currentVersion(String ruleId) {
        CompiledRule existing = rules.get(ruleId);
        if (existing != null) {
            return existing.version();
        }
        Long t = tombstones.get(ruleId);
        return t == null ? Long.MIN_VALUE : t;
    }

    private Result applyRule(ControlMessage m, long nowMs) {
        String id = m.id;
        long current = currentVersion(id);
        CompiledRule existing = rules.get(id);
        if (m.kind == ControlMessage.Kind.RULE_DELETE) {
            if (existing == null && tombstones.containsKey(id) && m.version < current) {
                return new Result(Status.IGNORED_STALE, null, null);
            }
            if (existing != null && m.version < current) {
                return new Result(Status.IGNORED_STALE, null, null);
            }
            uninstall(existing, nowMs);
            tombstones.put(id, Math.max(m.version, current));
            return new Result(Status.REMOVED, null, existing);
        }
        if (m.version <= current) {
            return new Result(Status.IGNORED_STALE, null, null);
        }
        long eff = config.effectiveMode == EngineConfig.EffectiveMode.EVENT_TIME ? m.effectiveFromMs : Long.MIN_VALUE;
        CompiledRule compiled;
        try {
            compiled = RuleCompiler.compile(m.payload, m.version, eff);
        } catch (CompileException e) {
            return new Result(Status.REJECTED, e.getMessage(), null);
        } catch (RuntimeException e) {
            return new Result(Status.REJECTED, "Unexpected compile error: " + e, null);
        }
        if (!compiled.enabled()) {
            uninstall(existing, nowMs);
            tombstones.put(id, m.version);
            return new Result(Status.REMOVED, null, existing);
        }
        install(compiled, existing, nowMs);
        tombstones.remove(id);
        return new Result(Status.APPLIED, null, compiled);
    }

    private void install(CompiledRule rule, CompiledRule existing, long nowMs) {
        acquireSpecs(rule);
        if (existing != null) {
            releaseSpecs(existing, nowMs);
            index.remove(existing);
            rule.setIdx(existing.idx());
        } else {
            rule.setIdx(allocateIdx());
        }
        byIdx.set(rule.idx(), rule);
        rules.put(rule.ruleId(), rule);
        index.add(rule);
        if (rule.hasPendingAliases()) {
            pendingAliasRules.add(rule.ruleId());
        } else {
            pendingAliasRules.remove(rule.ruleId());
        }
    }

    private void uninstall(CompiledRule existing, long nowMs) {
        if (existing == null) {
            return;
        }
        releaseSpecs(existing, nowMs);
        index.remove(existing);
        rules.remove(existing.ruleId());
        pendingAliasRules.remove(existing.ruleId());
        byIdx.set(existing.idx(), null);
        freeIdx.push(existing.idx());
    }

    private int allocateIdx() {
        if (!freeIdx.isEmpty()) {
            return freeIdx.pop();
        }
        byIdx.add(null);
        return byIdx.size() - 1;
    }

    private void acquireSpecs(CompiledRule rule) {
        Map<Long, SpecDefinition> specs = new HashMap<>();
        Map<String, MetricRegistry.Alias> resolved = new HashMap<>();
        boolean pending = false;
        for (AggRef ref : rule.aggRefs()) {
            if (ref.spec() != null) {
                specs.put(ref.spec().specKey(), ref.spec());
            } else {
                MetricRegistry.Alias a = registry.alias(ref.alias());
                if (a == null) {
                    pending = true;
                } else {
                    resolved.put(ref.alias(), a);
                    specs.put(a.spec().specKey(), a.spec());
                }
            }
        }
        long[] keys = new long[specs.size()];
        int i = 0;
        for (SpecDefinition d : specs.values()) {
            registry.acquire(d, rule.effectiveFromMs());
            keys[i++] = d.specKey();
        }
        rule.setAcquiredSpecKeys(keys);
        rule.setResolvedAliases(resolved);
        rule.setPendingAliases(pending);
    }

    private void releaseSpecs(CompiledRule rule, long nowMs) {
        for (long k : rule.acquiredSpecKeys()) {
            registry.release(k, nowMs);
        }
        rule.setAcquiredSpecKeys(new long[0]);
    }

    private Result applySchema(ControlMessage m, long nowMs) {
        String schemaId = m.id;
        if (m.kind == ControlMessage.Kind.SCHEMA_DELETE) {
            schemaVersions.remove(schemaId);
            registry.removeSchemaAliases(schemaId);
            return new Result(Status.REMOVED, null, null);
        }
        String prev = schemaVersions.get(schemaId);
        if (prev != null && SchemaCatalog.compareVersions(m.versionLabel, prev) < 0) {
            return new Result(Status.IGNORED_STALE, null, null);
        }
        Map<String, MetricRegistry.Alias> aliases;
        try {
            aliases = parseAliases(m.payload);
        } catch (CompileException e) {
            return new Result(Status.REJECTED, e.getMessage(), null);
        } catch (Exception e) {
            return new Result(Status.REJECTED, "Invalid schema payload: " + e.getMessage(), null);
        }
        schemaVersions.put(schemaId, m.versionLabel);
        registry.putSchemaAliases(schemaId, aliases);
        resolvePending(nowMs);
        return new Result(Status.APPLIED, null, null);
    }

    private void resolvePending(long nowMs) {
        if (pendingAliasRules.isEmpty()) {
            return;
        }
        for (String id : new ArrayList<>(pendingAliasRules)) {
            CompiledRule r = rules.get(id);
            if (r == null) {
                pendingAliasRules.remove(id);
                continue;
            }
            releaseSpecs(r, nowMs);
            acquireSpecs(r);
            if (!r.hasPendingAliases()) {
                pendingAliasRules.remove(id);
            }
        }
    }

    public static Map<String, MetricRegistry.Alias> parseAliases(String schemaPayload) throws Exception {
        JsonNode schema = MAPPER.readTree(schemaPayload);
        Map<String, MetricRegistry.Alias> out = new HashMap<>();
        JsonNode metrics = schema.get("metrics");
        if (metrics == null || !metrics.isArray()) {
            return out;
        }
        for (JsonNode m : metrics) {
            if (!m.hasNonNull("metric_id")) {
                continue;
            }
            String id = m.get("metric_id").asText();
            String field = m.hasNonNull("source_field") ? m.get("source_field").asText() : "event_id";
            AggFunction fn = AggFunction.parse(m.hasNonNull("aggregation") ? m.get("aggregation").asText() : "COUNT");
            SpecDefinition spec = CanonicalSpec.define(field, m.get("filter"));
            out.put(id, new MetricRegistry.Alias(id, spec, fn));
        }
        return out;
    }

    public Set<String> pendingAliasRules() {
        return new HashSet<>(pendingAliasRules);
    }
}
