package flink.operators;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import flink.compile.CompiledRule;
import flink.control.PostgresControlBootstrap;
import flink.engine.EngineConfig;
import flink.engine.EngineStats;
import flink.engine.EntityProcessor;
import flink.engine.RuleCatalog;
import flink.model.ControlMessage;
import flink.model.KeyedRecord;
import flink.model.ValidatedEvent;
import flink.registry.MetricRegistry;
import flink.state.CompactBucket;
import flink.state.CompactBucketSerializer;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.ToLongFunction;

public class RingBufferRuleProcessFunction
        extends KeyedBroadcastProcessFunction<String, ValidatedEvent, ControlMessage, KeyedRecord> {

    private static final long serialVersionUID = 2L;
    private static final Logger LOG = LoggerFactory.getLogger(RingBufferRuleProcessFunction.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SEP = "\u0001";
    private static final String TOMBSTONE = "T";
    private static final String RETIRED_KEY = "__retired";

    public static final OutputTag<KeyedRecord> LATE_DATA_TAG = new OutputTag<KeyedRecord>("late-events") {
    };

    public static final MapStateDescriptor<String, String> RULE_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("engineRules", Types.STRING, Types.STRING);

    public static final MapStateDescriptor<String, String> SCHEMA_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("engineSchemas", Types.STRING, Types.STRING);

    public static final MapStateDescriptor<String, String> SPEC_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("engineSpecs", Types.STRING, Types.STRING);

    private final EngineConfig config;

    private transient MapState<Integer, CompactBucket> buckets;
    private transient MapState<String, Long> cooldowns;
    private transient ValueState<Long> lastEventTime;
    private transient ValueState<Long> cleanupTimer;
    private transient RuleCatalog catalog;
    private transient EntityProcessor processor;
    private transient List<ControlMessage> bootstrap;
    private transient EntityProcessor.BucketStore bucketStore;
    private transient EntityProcessor.CooldownStore cooldownStore;
    private transient DescriptiveStatisticsHistogram e2eLatency;
    private transient DescriptiveStatisticsHistogram processMicros;
    private transient DescriptiveStatisticsHistogram eventTimeLag;
    private transient DescriptiveStatisticsHistogram ruleEffectiveLatency;

    public RingBufferRuleProcessFunction(EngineConfig config) {
        this.config = config;
    }

    public static MapStateDescriptor<?, ?>[] descriptors() {
        return new MapStateDescriptor<?, ?>[]{RULE_STATE_DESCRIPTOR, SCHEMA_STATE_DESCRIPTOR, SPEC_STATE_DESCRIPTOR};
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        MapStateDescriptor<Integer, CompactBucket> bucketDescriptor =
                new MapStateDescriptor<>("ringBuffer", IntSerializer.INSTANCE, CompactBucketSerializer.INSTANCE);
        MapStateDescriptor<String, Long> cooldownDescriptor =
                new MapStateDescriptor<>("cooldownUntil", Types.STRING, Types.LONG);
        ValueStateDescriptor<Long> lastDescriptor = new ValueStateDescriptor<>("lastEventTime", Types.LONG);
        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("cleanupTimer", Types.LONG);
        if (config.stateTtlHours > 0) {
            StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(config.stateTtlHours))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .cleanupInRocksdbCompactFilter(1000)
                    .build();
            bucketDescriptor.enableTimeToLive(ttl);
            cooldownDescriptor.enableTimeToLive(ttl);
        }
        buckets = getRuntimeContext().getMapState(bucketDescriptor);
        cooldowns = getRuntimeContext().getMapState(cooldownDescriptor);
        lastEventTime = getRuntimeContext().getState(lastDescriptor);
        cleanupTimer = getRuntimeContext().getState(timerDescriptor);

        bucketStore = new EntityProcessor.BucketStore() {
            @Override
            public CompactBucket get(int slot) throws Exception {
                return buckets.get(slot);
            }

            @Override
            public void put(int slot, CompactBucket bucket) throws Exception {
                buckets.put(slot, bucket);
            }

            @Override
            public void remove(int slot) throws Exception {
                buckets.remove(slot);
            }

            @Override
            public void forEach(EntityProcessor.SlotConsumer consumer) throws Exception {
                for (Map.Entry<Integer, CompactBucket> e : buckets.entries()) {
                    consumer.accept(e.getKey(), e.getValue());
                }
            }
        };
        cooldownStore = new EntityProcessor.CooldownStore() {
            @Override
            public Long get(String ruleId) throws Exception {
                return cooldowns.get(ruleId);
            }

            @Override
            public void put(String ruleId, long untilMs) throws Exception {
                cooldowns.put(ruleId, untilMs);
            }
        };

        bootstrap = config.bootstrapEnabled
                ? PostgresControlBootstrap.load(config.bootstrapUrl, config.bootstrapUser, config.bootstrapPassword, true, true)
                : Collections.emptyList();

        MetricGroup g = getRuntimeContext().getMetricGroup();
        e2eLatency = new DescriptiveStatisticsHistogram(4096);
        processMicros = new DescriptiveStatisticsHistogram(4096);
        ruleEffectiveLatency = new DescriptiveStatisticsHistogram(1024);
        g.histogram("e2e_latency_ms", e2eLatency);
        g.histogram("engine_process_us", processMicros);
        eventTimeLag = new DescriptiveStatisticsHistogram(4096);
        g.histogram("event_time_lag_ms", eventTimeLag);
        g.histogram("rule_effective_latency_ms", ruleEffectiveLatency);
        statGauge(g, "events_processed", s -> s.eventsProcessed);
        statGauge(g, "events_late", s -> s.eventsLate);
        statGauge(g, "spec_updates", s -> s.specUpdates);
        statGauge(g, "rules_considered", s -> s.rulesConsidered);
        statGauge(g, "candidates_after_trigger", s -> s.candidatesAfterTrigger);
        statGauge(g, "candidates_after_index", s -> s.candidatesAfterIndex);
        statGauge(g, "rules_evaluated", s -> s.rulesEvaluated);
        statGauge(g, "rules_matched", s -> s.rulesMatched);
        statGauge(g, "alerts_suppressed_by_cooldown", s -> s.suppressedByCooldown);
        statGauge(g, "rule_eval_errors", s -> s.evalErrors);
        statGauge(g, "state_entities_cleared", s -> s.entitiesCleared);
        statGauge(g, "state_buckets_purged", s -> s.bucketsPurged);
        g.gauge("rules_active", (Gauge<Integer>) () -> catalog == null ? 0 : catalog.ruleCount());
        g.gauge("specs_active", (Gauge<Integer>) () -> catalog == null ? 0 : catalog.registry().activeCount());
        g.gauge("specs_retired", (Gauge<Integer>) () -> catalog == null ? 0 : catalog.registry().retiredCount());
        g.gauge("index_posting_keys", (Gauge<Integer>) () -> catalog == null ? 0 : catalog.index().postingKeys());
        g.gauge("index_rules_without_axis_b", (Gauge<Integer>) () -> catalog == null ? 0 : catalog.index().rulesWithoutAxisB());
    }

    private void statGauge(MetricGroup g, String name, ToLongFunction<EngineStats> f) {
        g.gauge(name, (Gauge<Long>) () -> processor == null ? 0L : f.applyAsLong(processor.stats()));
    }

    private void ensureCatalog(ReadOnlyBroadcastState<String, String> rules,
                               ReadOnlyBroadcastState<String, String> schemas,
                               ReadOnlyBroadcastState<String, String> specs) throws Exception {
        if (catalog != null) {
            return;
        }
        catalog = new RuleCatalog(config);
        processor = new EntityProcessor(catalog, config);
        MetricRegistry registry = catalog.registry();
        long now = System.currentTimeMillis();
        for (Map.Entry<String, String> e : specs.immutableEntries()) {
            if (RETIRED_KEY.equals(e.getKey())) {
                registry.seedRetired(e.getValue());
            } else {
                registry.seed(Long.parseUnsignedLong(e.getKey(), 16), e.getValue());
            }
        }
        for (Map.Entry<String, String> e : sorted(schemas).entrySet()) {
            String[] v = e.getValue().split(SEP, 2);
            catalog.apply(new ControlMessage(ControlMessage.Kind.SCHEMA_UPSERT, e.getKey(), 0L, v[0],
                    Long.MIN_VALUE, 0L, v.length > 1 ? v[1] : "{}"), now);
        }
        for (Map.Entry<String, String> e : sorted(rules).entrySet()) {
            catalog.apply(decodeRule(e.getKey(), e.getValue()), now);
        }
        if (bootstrap != null) {
            for (ControlMessage m : bootstrap) {
                if (!m.isRule()) {
                    catalog.apply(m, now);
                }
            }
            for (ControlMessage m : bootstrap) {
                if (m.isRule()) {
                    catalog.apply(m, now);
                }
            }
            bootstrap = null;
        }
        LOG.info("Engine catalog ready: {} rules, {} active specs, {} aliases, index={}",
                catalog.ruleCount(), registry.activeCount(), registry.aliasCount(), catalog.index().mode());
    }

    private static TreeMap<String, String> sorted(ReadOnlyBroadcastState<String, String> state) throws Exception {
        TreeMap<String, String> m = new TreeMap<>();
        for (Map.Entry<String, String> e : state.immutableEntries()) {
            m.put(e.getKey(), e.getValue());
        }
        return m;
    }

    private static String encodeRule(ControlMessage m) {
        return m.version + SEP + m.effectiveFromMs + SEP + m.sourceTsMs + SEP + m.payload;
    }

    private static ControlMessage decodeRule(String ruleId, String encoded) {
        String[] p = encoded.split(SEP, 4);
        if (TOMBSTONE.equals(p[0])) {
            return new ControlMessage(ControlMessage.Kind.RULE_DELETE, ruleId, Long.parseLong(p[1]), p[1],
                    Long.MIN_VALUE, 0L, "{}");
        }
        return new ControlMessage(ControlMessage.Kind.RULE_UPSERT, ruleId, Long.parseLong(p[0]), p[0],
                Long.parseLong(p[1]), Long.parseLong(p[2]), p[3]);
    }

    @Override
    public void processBroadcastElement(ControlMessage m, Context ctx, Collector<KeyedRecord> out) throws Exception {
        BroadcastState<String, String> rules = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);
        BroadcastState<String, String> schemas = ctx.getBroadcastState(SCHEMA_STATE_DESCRIPTOR);
        BroadcastState<String, String> specs = ctx.getBroadcastState(SPEC_STATE_DESCRIPTOR);
        ensureCatalog(rules, schemas, specs);
        long now = ctx.currentProcessingTime();
        RuleCatalog.Result r = catalog.apply(m, now);
        switch (r.status) {
            case APPLIED:
                if (m.isRule()) {
                    rules.put(m.id, encodeRule(m));
                } else {
                    schemas.put(m.id, m.versionLabel + SEP + m.payload);
                }
                break;
            case REMOVED:
                if (m.isRule()) {
                    Long t = catalog.tombstone(m.id);
                    rules.put(m.id, TOMBSTONE + SEP + (t == null ? m.version : t));
                } else {
                    schemas.remove(m.id);
                }
                break;
            case IGNORED_STALE:
                if (m.isRule() && !rules.contains(m.id)) {
                    CompiledRule current = catalog.rule(m.id);
                    if (current != null && current.version() == m.version) {
                        rules.put(m.id, encodeRule(m));
                    }
                } else if (!m.isRule() && !schemas.contains(m.id) && !m.isDelete()) {
                    schemas.put(m.id, m.versionLabel + SEP + m.payload);
                }
                break;
            default:
                ObjectNode err = MAPPER.createObjectNode();
                err.put("error_reason", "Definition rejected: " + r.error);
                err.put("definition_kind", m.kind.name());
                err.put("definition_id", m.id);
                err.put("definition_version", m.versionLabel);
                ctx.output(LATE_DATA_TAG, new KeyedRecord(m.id, err.toString()));
                LOG.warn("Rejected {} {} v{}: {}", m.kind, m.id, m.versionLabel, r.error);
                break;
        }
        MetricRegistry registry = catalog.registry();
        registry.pruneRetired(now, config.retiredRetentionMs);
        for (Long k : registry.drainDirtyMeta()) {
            String meta = registry.encodeMeta(k);
            if (meta != null) {
                specs.put(Long.toHexString(k), meta);
            }
        }
        if (registry.drainRetiredDirty()) {
            specs.put(RETIRED_KEY, registry.encodeRetired());
        }
        if (m.sourceTsMs > 0 && r.status == RuleCatalog.Status.APPLIED) {
            ruleEffectiveLatency.update(Math.max(0L, now - m.sourceTsMs));
        }
    }

    @Override
    public void processElement(ValidatedEvent ev, ReadOnlyContext ctx, Collector<KeyedRecord> out) throws Exception {
        ensureCatalog(ctx.getBroadcastState(RULE_STATE_DESCRIPTOR), ctx.getBroadcastState(SCHEMA_STATE_DESCRIPTOR),
                ctx.getBroadcastState(SPEC_STATE_DESCRIPTOR));
        long startNs = System.nanoTime();
        long now = ctx.currentProcessingTime();
        long watermark = ctx.timerService().currentWatermark();
        String late = processor.process(ev, watermark, now, bucketStore, cooldownStore, out::collect);
        if (late != null) {
            ctx.output(LATE_DATA_TAG, lateRecord(ev, late));
            return;
        }
        Long last = lastEventTime.value();
        long latest = last == null ? ev.eventTimeMs : Math.max(last, ev.eventTimeMs);
        if (last == null || ev.eventTimeMs > last) {
            lastEventTime.update(latest);
        }
        if (cleanupTimer.value() == null) {
            long t = processor.cleanupDeadline(latest);
            ctx.timerService().registerEventTimeTimer(t);
            cleanupTimer.update(t);
        }
        if (ev.ingestTimeMs > 0) {
            e2eLatency.update(Math.max(0L, now - ev.ingestTimeMs));
        }
        eventTimeLag.update(Math.max(0L, now - ev.eventTimeMs));
        processMicros.update((System.nanoTime() - startNs) / 1000L);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<KeyedRecord> out) throws Exception {
        ensureCatalog(ctx.getBroadcastState(RULE_STATE_DESCRIPTOR), ctx.getBroadcastState(SCHEMA_STATE_DESCRIPTOR),
                ctx.getBroadcastState(SPEC_STATE_DESCRIPTOR));
        cleanupTimer.clear();
        Long last = lastEventTime.value();
        long deadline = last == null ? Long.MIN_VALUE : processor.cleanupDeadline(last);
        if (last != null && deadline > timestamp) {
            processor.sweep(timestamp, bucketStore);
            List<String> expired = new ArrayList<>();
            for (Map.Entry<String, Long> e : cooldowns.entries()) {
                if (e.getValue() <= timestamp) {
                    expired.add(e.getKey());
                }
            }
            for (String k : expired) {
                cooldowns.remove(k);
            }
            ctx.timerService().registerEventTimeTimer(deadline);
            cleanupTimer.update(deadline);
            return;
        }
        buckets.clear();
        cooldowns.clear();
        lastEventTime.clear();
        processor.stats().entitiesCleared++;
    }

    private static KeyedRecord lateRecord(ValidatedEvent ev, String reason) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("entity_id", ev.entityId);
        n.put("event_id", ev.eventId);
        n.put("event_type", ev.eventType);
        n.put("source_system", ev.sourceSystem);
        n.put("event_time", Instant.ofEpochMilli(ev.eventTimeMs).toString());
        n.put("event_time_ms", ev.eventTimeMs);
        n.put("error_reason", reason);
        if (ev.rawJson != null) {
            try {
                n.set("payload", MAPPER.readTree(ev.rawJson));
            } catch (Exception ignored) {
                n.put("payload", ev.rawJson);
            }
        }
        return new KeyedRecord(ev.entityId, n.toString());
    }
}
