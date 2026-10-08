package flink.registry;

import flink.compile.AggFunction;
import flink.model.ValidatedEvent;
import flink.state.CompactBucket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Registry of materialised aggregation specs (the "N definitions").
 *
 * <p>Specs are reference counted by the rules that use them. When the count drops to zero the spec is
 * retired: its storage key is remembered so stale cells can be dropped from buckets (cleanup layer 2).
 * If the same spec is acquired again later it gets a new epoch and therefore a new storage key, so it
 * never reads data aggregated by a previous incarnation ("only aggregate from when the definition
 * becomes effective").
 *
 * <p>Events are routed to specs by required {@code event_type}; specs without such a constraint are
 * evaluated for every event (wildcard list).
 */
public final class MetricRegistry {

    public static final class Entry {
        final SpecDefinition def;
        int refCount;
        long epoch;
        long effectiveFromMs;
        long storageKey;
        boolean seeded;
        boolean seededActive;

        Entry(SpecDefinition def) {
            this.def = def;
        }

        public SpecDefinition def() {
            return def;
        }

        public int refCount() {
            return refCount;
        }

        public long epoch() {
            return epoch;
        }

        public long effectiveFromMs() {
            return effectiveFromMs;
        }

        public long storageKey() {
            return storageKey;
        }

        public boolean active() {
            return refCount > 0;
        }
    }

    public static final class Alias {
        private final String metricId;
        private final SpecDefinition spec;
        private final AggFunction function;

        public Alias(String metricId, SpecDefinition spec, AggFunction function) {
            this.metricId = metricId;
            this.spec = spec;
            this.function = function;
        }

        public String metricId() {
            return metricId;
        }

        public SpecDefinition spec() {
            return spec;
        }

        public AggFunction function() {
            return function;
        }
    }

    private static final Entry[] EMPTY = new Entry[0];

    private final Map<Long, Entry> entries = new HashMap<>();
    private final Map<Long, Long> retired = new HashMap<>();
    private final Map<Long, long[]> seeds = new HashMap<>();
    private final TreeMap<String, Map<String, Alias>> aliasesBySchema = new TreeMap<>();
    private final Map<String, Alias> aliases = new HashMap<>();
    private final Set<Long> dirtyMeta = new HashSet<>();
    private boolean retiredDirty;

    private Map<String, Entry[]> byEventType = Collections.emptyMap();
    private Entry[] wildcard = EMPTY;
    private boolean routingDirty = true;

    public static long storageKey(long specKey, long epoch) {
        if (epoch == 0) {
            return specKey;
        }
        long z = specKey ^ (epoch * 0x9E3779B97F4A7C15L);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        return z ^ (z >>> 31);
    }

    public Entry acquire(SpecDefinition def, long effectiveFromMs) {
        Entry e = entries.get(def.specKey());
        boolean fresh = false;
        if (e == null) {
            e = new Entry(def);
            long[] seed = seeds.remove(def.specKey());
            if (seed != null) {
                e.epoch = seed[0];
                e.effectiveFromMs = seed[1];
                e.seeded = true;
                e.seededActive = seed[2] != 0;
            } else {
                e.epoch = 0;
                e.effectiveFromMs = effectiveFromMs;
                fresh = true;
            }
            e.storageKey = storageKey(def.specKey(), e.epoch);
            entries.put(def.specKey(), e);
        }
        if (e.refCount == 0) {
            if (e.seeded && e.seededActive) {
                e.seeded = false;
            } else if (!fresh) {
                if (!retired.containsKey(e.storageKey)) {
                    retired.put(e.storageKey, Long.MIN_VALUE);
                    retiredDirty = true;
                }
                e.seeded = false;
                e.epoch++;
                e.storageKey = storageKey(def.specKey(), e.epoch);
                e.effectiveFromMs = effectiveFromMs;
            }
            routingDirty = true;
        }
        e.refCount++;
        dirtyMeta.add(def.specKey());
        return e;
    }

    public void release(long specKey, long nowMs) {
        Entry e = entries.get(specKey);
        if (e == null || e.refCount == 0) {
            return;
        }
        e.refCount--;
        if (e.refCount == 0) {
            retired.put(e.storageKey, nowMs);
            retiredDirty = true;
            routingDirty = true;
        }
        dirtyMeta.add(specKey);
    }

    public Entry entry(long specKey) {
        return entries.get(specKey);
    }

    public Entry activeEntry(SpecDefinition def) {
        Entry e = entries.get(def.specKey());
        return e != null && e.refCount > 0 ? e : null;
    }

    public int accumulate(ValidatedEvent ev, CompactBucket bucket) {
        ensureRouting();
        int n = 0;
        if (ev.eventType != null) {
            Entry[] typed = byEventType.get(ev.eventType);
            if (typed != null) {
                n += accumulate(typed, ev, bucket);
            }
        }
        n += accumulate(wildcard, ev, bucket);
        return n;
    }

    private static int accumulate(Entry[] list, ValidatedEvent ev, CompactBucket bucket) {
        int n = 0;
        for (Entry e : list) {
            if (ev.eventTimeMs < e.effectiveFromMs) {
                continue;
            }
            if (!e.def.matches(ev)) {
                continue;
            }
            double v = e.def.extract(ev);
            if (Double.isNaN(v)) {
                continue;
            }
            bucket.add(e.storageKey, v);
            n++;
        }
        return n;
    }

    private void ensureRouting() {
        if (!routingDirty) {
            return;
        }
        Map<String, List<Entry>> typed = new HashMap<>();
        List<Entry> wild = new ArrayList<>();
        for (Entry e : entries.values()) {
            if (e.refCount == 0) {
                continue;
            }
            Set<String> types = e.def.eventTypes();
            if (types == null) {
                wild.add(e);
            } else {
                for (String t : types) {
                    typed.computeIfAbsent(t, k -> new ArrayList<>()).add(e);
                }
            }
        }
        Map<String, Entry[]> m = new HashMap<>();
        for (Map.Entry<String, List<Entry>> x : typed.entrySet()) {
            m.put(x.getKey(), x.getValue().toArray(EMPTY));
        }
        byEventType = m;
        wildcard = wild.toArray(EMPTY);
        routingDirty = false;
    }

    public boolean hasRetired() {
        return !retired.isEmpty();
    }

    public boolean isRetired(long storageKey) {
        return retired.containsKey(storageKey);
    }

    public int pruneRetired(long nowMs, long retentionMs) {
        int removed = 0;
        Iterator<Map.Entry<Long, Long>> it = retired.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> x = it.next();
            if (x.getValue() == Long.MIN_VALUE) {
                x.setValue(nowMs);
                retiredDirty = true;
            } else if (nowMs - x.getValue() > retentionMs) {
                it.remove();
                removed++;
                retiredDirty = true;
            }
        }
        return removed;
    }

    public void putSchemaAliases(String schemaId, Map<String, Alias> schemaAliases) {
        if (schemaAliases == null || schemaAliases.isEmpty()) {
            aliasesBySchema.remove(schemaId);
        } else {
            aliasesBySchema.put(schemaId, schemaAliases);
        }
        rebuildAliases();
    }

    public void removeSchemaAliases(String schemaId) {
        aliasesBySchema.remove(schemaId);
        rebuildAliases();
    }

    private void rebuildAliases() {
        aliases.clear();
        for (Map<String, Alias> m : aliasesBySchema.values()) {
            aliases.putAll(m);
        }
    }

    public Alias alias(String metricId) {
        return aliases.get(metricId);
    }

    public int aliasCount() {
        return aliases.size();
    }

    public int activeCount() {
        int n = 0;
        for (Entry e : entries.values()) {
            if (e.refCount > 0) {
                n++;
            }
        }
        return n;
    }

    public int retiredCount() {
        return retired.size();
    }

    public String encodeMeta(long specKey) {
        Entry e = entries.get(specKey);
        if (e == null) {
            return null;
        }
        return e.epoch + "|" + e.effectiveFromMs + "|" + (e.refCount > 0 ? 1 : 0);
    }

    public void seed(long specKey, String encoded) {
        String[] p = encoded.split("\\|");
        if (p.length < 3) {
            return;
        }
        seeds.put(specKey, new long[]{Long.parseLong(p[0]), Long.parseLong(p[1]), Long.parseLong(p[2])});
    }

    public String encodeRetired() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, Long> x : retired.entrySet()) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(x.getKey()).append(':').append(x.getValue());
        }
        return sb.toString();
    }

    public void seedRetired(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return;
        }
        for (String part : encoded.split(",")) {
            int c = part.lastIndexOf(':');
            if (c > 0) {
                retired.put(Long.parseLong(part.substring(0, c)), Long.parseLong(part.substring(c + 1)));
            }
        }
    }

    public Set<Long> drainDirtyMeta() {
        if (dirtyMeta.isEmpty()) {
            return Collections.emptySet();
        }
        Set<Long> out = new HashSet<>(dirtyMeta);
        dirtyMeta.clear();
        return out;
    }

    public boolean drainRetiredDirty() {
        boolean d = retiredDirty;
        retiredDirty = false;
        return d;
    }
}
