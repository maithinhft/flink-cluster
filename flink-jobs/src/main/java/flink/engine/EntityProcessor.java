package flink.engine;

import flink.compile.CompiledRule;
import flink.model.KeyedRecord;
import flink.model.ValidatedEvent;
import flink.registry.MetricRegistry;
import flink.state.CompactBucket;
import flink.state.TimeBuckets;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-entity processing logic, independent of Flink so it can be unit tested with in-memory stores.
 *
 * <ol>
 *   <li>Reject late data: bucket already closed by the watermark (end + tolerance) or older than 24h.</li>
 *   <li>Write: circular overwrite of the slot (cleanup layer 3), lazy drop of retired specs
 *       (cleanup layer 2), incremental update of every matching spec.</li>
 *   <li>Select candidate rules through the inverted index, check trigger and cooldown,
 *       evaluate the compiled AST and emit alerts.</li>
 * </ol>
 */
public final class EntityProcessor {

    public interface SlotConsumer {
        void accept(int slot, CompactBucket bucket) throws Exception;
    }

    public interface BucketStore {
        CompactBucket get(int slot) throws Exception;

        void put(int slot, CompactBucket bucket) throws Exception;

        void remove(int slot) throws Exception;

        void forEach(SlotConsumer consumer) throws Exception;
    }

    public interface CooldownStore {
        Long get(String ruleId) throws Exception;

        void put(String ruleId, long untilMs) throws Exception;
    }

    public interface AlertOutput {
        void emit(KeyedRecord alert);
    }

    private final RuleCatalog catalog;
    private final EngineConfig config;
    private final AlertBuilder alerts;
    private final WindowEvaluator evaluator;
    private final EngineStats stats = new EngineStats();

    public EntityProcessor(RuleCatalog catalog, EngineConfig config) {
        this.catalog = catalog;
        this.config = config;
        this.alerts = new AlertBuilder(config.includePayload);
        this.evaluator = new WindowEvaluator(catalog.registry(), config.fullLoadThreshold);
    }

    public EngineStats stats() {
        return stats;
    }

    public RuleCatalog catalog() {
        return catalog;
    }

    public String lateReason(ValidatedEvent ev, long watermark) {
        long bucketStart = TimeBuckets.bucketStart(ev.eventTimeMs);
        long closeTime = bucketStart + TimeBuckets.BUCKET_MS + config.lateToleranceMs;
        if (watermark != Long.MIN_VALUE && watermark >= closeTime) {
            return "Late event rejected: watermark=" + watermark + " >= bucketCloseTime=" + closeTime
                    + " (bucketEnd=" + (bucketStart + TimeBuckets.BUCKET_MS) + ")";
        }
        if (watermark != Long.MIN_VALUE && ev.eventTimeMs < watermark - TimeBuckets.MAX_WINDOW_MS) {
            return "Event older than 24h window: eventTime=" + ev.eventTimeMs + ", watermark=" + watermark;
        }
        return null;
    }

    public String process(ValidatedEvent ev, long watermark, long nowMs, BucketStore buckets,
                          CooldownStore cooldowns, AlertOutput out) throws Exception {
        stats.eventsProcessed++;
        String late = lateReason(ev, watermark);
        if (late != null) {
            stats.eventsLate++;
            return late;
        }
        long bucketStart = TimeBuckets.bucketStart(ev.eventTimeMs);
        int slot = TimeBuckets.slotOf(ev.eventTimeMs);
        CompactBucket bucket = buckets.get(slot);
        if (bucket == null || bucket.getStartMs() < bucketStart) {
            bucket = new CompactBucket(bucketStart);
        } else if (bucket.getStartMs() > bucketStart) {
            stats.eventsLate++;
            return "Circular buffer overwrite: slot holds newer bucket (>24h)";
        }
        MetricRegistry registry = catalog.registry();
        boolean changed = false;
        if (registry.hasRetired()) {
            changed = bucket.removeIf(registry::isRetired);
        }
        int updates = registry.accumulate(ev, bucket);
        stats.specUpdates += updates;
        if (updates > 0 || changed) {
            buckets.put(slot, bucket);
        }

        evaluate(ev, buckets, cooldowns, out, slot, bucket, nowMs);
        return null;
    }

    private void evaluate(ValidatedEvent ev, BucketStore buckets, CooldownStore cooldowns, AlertOutput out,
                          int slot, CompactBucket bucket, long nowMs) throws Exception {
        if (catalog.ruleCount() == 0 || ev.eventType == null) {
            return;
        }
        stats.rulesConsidered += catalog.ruleCount();
        stats.candidatesAfterTrigger += catalog.index().triggerCandidates(ev);
        RoaringBitmap candidates = catalog.candidates(ev);
        stats.candidatesAfterIndex += candidates.getCardinality();
        if (candidates.isEmpty()) {
            return;
        }
        evaluator.reset(ev, buckets, slot, bucket);
        IntIterator it = candidates.getIntIterator();
        while (it.hasNext()) {
            CompiledRule rule = catalog.ruleAt(it.next());
            if (rule == null || !rule.matchesTrigger(ev.eventType)) {
                continue;
            }
            if (rule.cooldownMs() > 0) {
                Long until = cooldowns.get(rule.ruleId());
                if (until != null && ev.eventTimeMs < until) {
                    stats.suppressedByCooldown++;
                    continue;
                }
            }
            stats.rulesEvaluated++;
            boolean ok;
            evaluator.startRule(rule);
            try {
                ok = rule.root().eval(evaluator);
            } catch (RuntimeException e) {
                stats.evalErrors++;
                continue;
            }
            if (ok && rule.requireFullWindow() && !evaluator.windowComplete()) {
                ok = false;
            }
            if (!ok) {
                continue;
            }
            stats.rulesMatched++;
            long until = rule.cooldownMs() > 0 ? ev.eventTimeMs + rule.cooldownMs() : 0L;
            if (until > 0) {
                cooldowns.put(rule.ruleId(), until);
            }
            out.emit(alerts.build(ev, rule, evaluator.recorded(), evaluator.windowComplete(), until,
                    config.lateToleranceMs, nowMs));
        }
    }

    /**
     * Timer-driven sweep: drop buckets older than 24h before the watermark (layer 3) and retired spec cells
     * (layer 2) from the remaining ones.
     */
    public void sweep(long watermark, BucketStore buckets) throws Exception {
        MetricRegistry registry = catalog.registry();
        long cutoff = watermark - TimeBuckets.MAX_WINDOW_MS;
        List<Integer> toRemove = new ArrayList<>();
        List<Object[]> toRewrite = new ArrayList<>();
        buckets.forEach((slot, b) -> {
            if (b.getEndMs() <= cutoff) {
                toRemove.add(slot);
            } else if (registry.hasRetired()) {
                CompactBucket copy = b.copy();
                if (copy.removeIf(registry::isRetired)) {
                    toRewrite.add(new Object[]{slot, copy});
                }
            }
        });
        for (Integer slot : toRemove) {
            buckets.remove(slot);
        }
        for (Object[] x : toRewrite) {
            CompactBucket b = (CompactBucket) x[1];
            if (b.isEmpty()) {
                buckets.remove((Integer) x[0]);
            } else {
                buckets.put((Integer) x[0], b);
            }
        }
        stats.bucketsPurged += toRemove.size();
    }

    public long cleanupDeadline(long lastEventTimeMs) {
        return TimeBuckets.roundUpToBucket(lastEventTimeMs + TimeBuckets.MAX_WINDOW_MS
                + TimeBuckets.BUCKET_MS + config.lateToleranceMs);
    }
}
