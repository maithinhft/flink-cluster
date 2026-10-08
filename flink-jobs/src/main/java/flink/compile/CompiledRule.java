package flink.compile;

import flink.registry.MetricRegistry;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A rule compiled once on the broadcast side. Holds the optimised AST plus bookkeeping used by the
 * catalog (internal index slot, specs it holds a reference on, aliases it resolved).
 */
public final class CompiledRule {

    private final String ruleId;
    private final String name;
    private final long version;
    private final boolean enabled;
    private final long cooldownMs;
    private final long effectiveFromMs;
    private final boolean requireFullWindow;
    private final Set<String> triggers;
    private final Node root;
    private final List<AggRef> aggRefs;

    private int idx = -1;
    private long[] acquiredSpecKeys = new long[0];
    private Map<String, MetricRegistry.Alias> resolvedAliases = Collections.emptyMap();
    private boolean pendingAliases;

    public CompiledRule(String ruleId, String name, long version, boolean enabled, long cooldownMs,
                        long effectiveFromMs, boolean requireFullWindow, Set<String> triggers,
                        Node root, List<AggRef> aggRefs) {
        this.ruleId = ruleId;
        this.name = name;
        this.version = version;
        this.enabled = enabled;
        this.cooldownMs = cooldownMs;
        this.effectiveFromMs = effectiveFromMs;
        this.requireFullWindow = requireFullWindow;
        this.triggers = triggers;
        this.root = root;
        this.aggRefs = aggRefs;
    }

    public boolean matchesTrigger(String eventType) {
        return eventType != null && triggers.contains(eventType);
    }

    public String ruleId() {
        return ruleId;
    }

    public String name() {
        return name;
    }

    public long version() {
        return version;
    }

    public boolean enabled() {
        return enabled;
    }

    public long cooldownMs() {
        return cooldownMs;
    }

    public long effectiveFromMs() {
        return effectiveFromMs;
    }

    public boolean requireFullWindow() {
        return requireFullWindow;
    }

    public Set<String> triggers() {
        return triggers;
    }

    public Node root() {
        return root;
    }

    public List<AggRef> aggRefs() {
        return aggRefs;
    }

    public int idx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public long[] acquiredSpecKeys() {
        return acquiredSpecKeys;
    }

    public void setAcquiredSpecKeys(long[] keys) {
        this.acquiredSpecKeys = keys;
    }

    public MetricRegistry.Alias alias(String id) {
        return resolvedAliases.get(id);
    }

    public void setResolvedAliases(Map<String, MetricRegistry.Alias> aliases) {
        this.resolvedAliases = aliases.isEmpty() ? Collections.emptyMap() : new HashMap<>(aliases);
    }

    public boolean hasPendingAliases() {
        return pendingAliases;
    }

    public void setPendingAliases(boolean pending) {
        this.pendingAliases = pending;
    }

    @Override
    public String toString() {
        return "CompiledRule{" + ruleId + " v" + version + ", triggers=" + triggers + '}';
    }
}
