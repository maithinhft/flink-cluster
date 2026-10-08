package flink.engine;

import flink.compile.AggFunction;
import flink.compile.AggPredNode;
import flink.compile.AggRef;
import flink.compile.CompiledRule;
import flink.compile.EvalContext;
import flink.compile.WindowPlan;
import flink.model.ValidatedEvent;
import flink.registry.MetricRegistry;
import flink.registry.SpecDefinition;
import flink.state.CompactBucket;
import flink.state.TimeBuckets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Evaluation context for one event of one entity. Bucket reads are lazy and cached for the whole event
 * (shared by all candidate rules); window accumulators are memoised per (storage key, window range).
 * When more than {@code fullLoadThreshold} distinct slots are requested the whole ring of the entity is
 * loaded with one prefix scan instead of many point lookups.
 */
public final class WindowEvaluator implements EvalContext {

    private record WinKey(long storageKey, long start, long end) {
    }

    private final MetricRegistry registry;
    private final int fullLoadThreshold;
    private final CompactBucket[] cache = new CompactBucket[TimeBuckets.SLOTS];
    private final boolean[] loaded = new boolean[TimeBuckets.SLOTS];
    private final Map<WinKey, double[]> memo = new HashMap<>();
    private final Map<String, Double> recorded = new LinkedHashMap<>();

    private ValidatedEvent event;
    private long tb;
    private EntityProcessor.BucketStore store;
    private boolean allLoaded;
    private int pointReads;
    private CompiledRule rule;
    private boolean windowComplete;

    public WindowEvaluator(MetricRegistry registry, int fullLoadThreshold) {
        this.registry = registry;
        this.fullLoadThreshold = fullLoadThreshold;
    }

    public void reset(ValidatedEvent ev, EntityProcessor.BucketStore store, int currentSlot, CompactBucket current) {
        this.event = ev;
        this.tb = TimeBuckets.bucketIndex(ev.eventTimeMs);
        this.store = store;
        Arrays.fill(cache, null);
        Arrays.fill(loaded, false);
        memo.clear();
        allLoaded = false;
        pointReads = 0;
        cache[currentSlot] = current;
        loaded[currentSlot] = true;
    }

    public void startRule(CompiledRule r) {
        this.rule = r;
        this.windowComplete = true;
        recorded.clear();
    }

    public boolean windowComplete() {
        return windowComplete;
    }

    public Map<String, Double> recorded() {
        return recorded;
    }

    public int pointReads() {
        return pointReads;
    }

    @Override
    public ValidatedEvent event() {
        return event;
    }

    @Override
    public void record(String path, Double value) {
        recorded.put(path, value);
    }

    private CompactBucket bucketAt(long bucketIndex) {
        int slot = TimeBuckets.slotOfIndex(bucketIndex);
        if (!loaded[slot]) {
            if (!allLoaded && pointReads >= fullLoadThreshold) {
                loadAll();
            }
            if (!loaded[slot]) {
                try {
                    cache[slot] = store.get(slot);
                } catch (Exception e) {
                    throw new IllegalStateException("State read failed", e);
                }
                loaded[slot] = true;
                pointReads++;
            }
        }
        CompactBucket b = cache[slot];
        return b != null && b.getStartMs() == bucketIndex * TimeBuckets.BUCKET_MS ? b : null;
    }

    private void loadAll() {
        try {
            store.forEach((slot, b) -> {
                if (!loaded[slot]) {
                    cache[slot] = b;
                }
            });
        } catch (Exception e) {
            throw new IllegalStateException("State scan failed", e);
        }
        Arrays.fill(loaded, true);
        allLoaded = true;
    }

    private double[] accumulate(long storageKey, long start, long end) {
        WinKey k = new WinKey(storageKey, start, end);
        double[] acc = memo.get(k);
        if (acc != null) {
            return acc;
        }
        double sum = 0.0;
        long count = 0;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (long b = start; b <= end; b++) {
            CompactBucket bucket = bucketAt(b);
            if (bucket == null) {
                continue;
            }
            int i = bucket.indexOf(storageKey);
            if (i < 0) {
                continue;
            }
            sum += bucket.sumAt(i);
            count += bucket.countAt(i);
            min = Math.min(min, bucket.minAt(i));
            max = Math.max(max, bucket.maxAt(i));
        }
        acc = new double[]{sum, count, min, max};
        memo.put(k, acc);
        return acc;
    }

    private static Double value(AggFunction fn, double[] acc) {
        return fn.value(acc[0], (long) acc[1], acc[2], acc[3]);
    }

    private SpecDefinition specOf(AggRef ref) {
        if (ref.spec() != null) {
            return ref.spec();
        }
        MetricRegistry.Alias a = rule == null ? null : rule.alias(ref.alias());
        return a == null ? null : a.spec();
    }

    private AggFunction functionOf(AggRef ref) {
        if (ref.function() != null) {
            return ref.function();
        }
        MetricRegistry.Alias a = rule == null ? null : rule.alias(ref.alias());
        return a == null ? AggFunction.COUNT : a.function();
    }

    @Override
    public boolean testAggregate(AggPredNode node) {
        AggRef ref = node.ref();
        SpecDefinition spec = specOf(ref);
        MetricRegistry.Entry entry = spec == null ? null : registry.activeEntry(spec);
        if (entry == null) {
            windowComplete = false;
            record(ref.path(), null);
            return false;
        }
        AggFunction fn = functionOf(ref);
        WindowPlan plan = ref.window();
        long[] w = plan.windows(tb);
        if (w[0] * TimeBuckets.BUCKET_MS < entry.effectiveFromMs()) {
            windowComplete = false;
        }
        Double latest = value(fn, accumulate(entry.storageKey(), w[0], w[1]));
        switch (plan.quantifier()) {
            case ANY: {
                for (int i = 0; i < w.length; i += 2) {
                    Double v = i == 0 ? latest : value(fn, accumulate(entry.storageKey(), w[i], w[i + 1]));
                    if (node.test(v)) {
                        record(ref.path(), v);
                        return true;
                    }
                }
                record(ref.path(), latest);
                return false;
            }
            case ALL: {
                for (int i = 0; i < w.length; i += 2) {
                    Double v = i == 0 ? latest : value(fn, accumulate(entry.storageKey(), w[i], w[i + 1]));
                    if (!node.test(v)) {
                        record(ref.path(), v);
                        return false;
                    }
                }
                record(ref.path(), latest);
                return true;
            }
            default:
                record(ref.path(), latest);
                return node.test(latest);
        }
    }

    @Override
    public Double aggregateValue(AggRef ref) {
        SpecDefinition spec = specOf(ref);
        MetricRegistry.Entry entry = spec == null ? null : registry.activeEntry(spec);
        if (entry == null) {
            windowComplete = false;
            return null;
        }
        long[] w = ref.window().windows(tb);
        if (w[0] * TimeBuckets.BUCKET_MS < entry.effectiveFromMs()) {
            windowComplete = false;
        }
        Double v = value(functionOf(ref), accumulate(entry.storageKey(), w[0], w[1]));
        record(ref.path(), v);
        return v;
    }
}
