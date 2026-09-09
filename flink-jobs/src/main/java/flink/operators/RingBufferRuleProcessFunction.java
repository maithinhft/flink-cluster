package flink.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import flink.evaluators.RuleEvaluator;
import flink.models.Bucket;
import flink.models.RingBuffer;
import flink.models.RuleDefinition;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class RingBufferRuleProcessFunction extends KeyedBroadcastProcessFunction<String, String, String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(RingBufferRuleProcessFunction.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final long LATE_TOLERANCE_MS = 60_000L;
    public static final OutputTag<String> LATE_DATA_TAG = new OutputTag<String>("late-events") {};

    public static final MapStateDescriptor<String, String> RULE_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("rulesBroadcastState", Types.STRING, Types.STRING);

    private transient MapState<Integer, Bucket> ringBufferMapState;
    private transient MapState<String, Long> ruleCooldownMapState;
    private transient Map<String, RuleDefinition> parsedRuleCache;
    private transient Set<String> cachedGlobalActiveMetricIds;
    private transient org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram pipelineLatencyHistogram;
    private transient org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram eventTimeLagHistogram;

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofHours(25))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<Integer, Bucket> descriptor =
                new MapStateDescriptor<>("ringBufferMapState", Integer.class, Bucket.class);
        descriptor.enableTimeToLive(ttlConfig);
        ringBufferMapState = getRuntimeContext().getMapState(descriptor);

        MapStateDescriptor<String, Long> cooldownDescriptor =
                new MapStateDescriptor<>("ruleCooldownMapState", Types.STRING, Types.LONG);
        cooldownDescriptor.enableTimeToLive(ttlConfig);
        ruleCooldownMapState = getRuntimeContext().getMapState(cooldownDescriptor);

        pipelineLatencyHistogram = new org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram(2048);
        getRuntimeContext().getMetricGroup().histogram("pipeline_latency_ms", pipelineLatencyHistogram);

        eventTimeLagHistogram = new org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram(2048);
        getRuntimeContext().getMetricGroup().histogram("event_time_lag_ms", eventTimeLagHistogram);
    }

    @Override
    public void processBroadcastElement(String ruleJson, Context ctx, Collector<String> out) throws Exception {
        try {
            JsonNode cdcNode = mapper.readTree(ruleJson);
            if (cdcNode == null || !cdcNode.has("rule_id")) {
                return;
            }

            String ruleId = cdcNode.get("rule_id").asText();
            BroadcastState<String, String> ruleState = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);

            boolean isDelete = cdcNode.has("__op") && "d".equalsIgnoreCase(cdcNode.get("__op").asText());
            boolean isEnabled = !cdcNode.has("enabled") || cdcNode.get("enabled").asBoolean();

            if (isDelete || !isEnabled) {
                ruleState.remove(ruleId);
                if (parsedRuleCache != null) {
                    parsedRuleCache.remove(ruleId);
                }
            } else {
                ruleState.put(ruleId, ruleJson);
                RuleDefinition rule = RuleDefinition.fromCdcJson(cdcNode, mapper);
                if (rule != null) {
                    if (parsedRuleCache == null) {
                        parsedRuleCache = new HashMap<>();
                    }
                    parsedRuleCache.put(ruleId, rule);
                }
            }
        } catch (Exception e) {
            LOG.warn("Failed to process broadcast rule JSON: {}", ruleJson, e);
        }
    }

    private Map<String, RuleDefinition> getOrRebuildRuleCache(ReadOnlyContext ctx) {
        if (parsedRuleCache != null) {
            return parsedRuleCache;
        }
        parsedRuleCache = new HashMap<>();
        try {
            ReadOnlyBroadcastState<String, String> ruleState = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);
            for (Map.Entry<String, String> entry : ruleState.immutableEntries()) {
                try {
                    JsonNode node = mapper.readTree(entry.getValue());
                    RuleDefinition rule = RuleDefinition.fromCdcJson(node, mapper);
                    if (rule != null && rule.isEnabled()) {
                        parsedRuleCache.put(rule.getRuleId(), rule);
                    }
                } catch (Exception ignored) {
                }
            }
        } catch (Exception ignored) {
        }
        return parsedRuleCache;
    }

    @Override
    public void processElement(String eventJson, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        JsonNode eventNode;
        try {
            eventNode = mapper.readTree(eventJson);
        } catch (Exception e) {
            LOG.warn("Failed to parse event JSON in aggregation stage: {}", eventJson, e);
            return;
        }

        if (eventNode.has("_ingest_time") && pipelineLatencyHistogram != null) {
            long ingestTime = eventNode.get("_ingest_time").asLong();
            pipelineLatencyHistogram.update(Math.max(0L, System.currentTimeMillis() - ingestTime));
        }

        long eventTimeMs = extractEventTime(eventNode);
        if (eventTimeMs > 0 && eventTimeLagHistogram != null) {
            eventTimeLagHistogram.update(Math.max(0L, System.currentTimeMillis() - eventTimeMs));
        }
        if (eventTimeMs < 0) {
            if (eventNode.isObject()) {
                ((ObjectNode) eventNode).put("error_reason", "Missing or unparseable ISO-8601 event_time");
            }
            ctx.output(LATE_DATA_TAG, mapper.writeValueAsString(eventNode));
            return;
        }

        long bucketStart = RingBuffer.normalizeToBucketStartTime(eventTimeMs);
        long bucketEnd = bucketStart + RingBuffer.BUCKET_DURATION_MS;
        long bucketCloseTime = bucketEnd + LATE_TOLERANCE_MS;

        long currentWatermark = ctx.timerService().currentWatermark();

        if (currentWatermark >= bucketCloseTime) {
            if (eventNode.isObject()) {
                ((ObjectNode) eventNode).put("error_reason",
                        String.format("Late event rejected: watermark=%d >= bucketCloseTime=%d (bucketEnd=%d)",
                                currentWatermark, bucketCloseTime, bucketEnd));
            }
            ctx.output(LATE_DATA_TAG, mapper.writeValueAsString(eventNode));
            return;
        }

        if (currentWatermark > 0 && eventTimeMs < currentWatermark - RingBuffer.MAX_WINDOW_MS) {
            if (eventNode.isObject()) {
                ((ObjectNode) eventNode).put("error_reason",
                        String.format("Event older than 24h window: eventTime=%d, watermark=%d",
                                eventTimeMs, currentWatermark));
            }
            ctx.output(LATE_DATA_TAG, mapper.writeValueAsString(eventNode));
            return;
        }

        int slot = RingBuffer.getSlotIndex(bucketStart);
        Bucket bucket = ringBufferMapState.get(slot);

        if (bucket == null || bucketStart > bucket.getStartTimeMs()) {
            bucket = new Bucket(bucketStart);
        } else if (bucketStart < bucket.getStartTimeMs()) {
            if (eventNode.isObject()) {
                ((ObjectNode) eventNode).put("error_reason", "Circular buffer overwrite: slot holds newer bucket (>24h)");
            }
            ctx.output(LATE_DATA_TAG, mapper.writeValueAsString(eventNode));
            return;
        }

        JsonNode globalActiveNode = eventNode.get("_global_active_metric_ids");
        if (globalActiveNode != null && globalActiveNode.isArray() && globalActiveNode.size() > 0
                && bucket.getMetrics() != null && !bucket.getMetrics().isEmpty()) {
            if (cachedGlobalActiveMetricIds == null) cachedGlobalActiveMetricIds = new HashSet<>();
            else cachedGlobalActiveMetricIds.clear();
            for (JsonNode idNode : globalActiveNode) {
                cachedGlobalActiveMetricIds.add(idNode.asText());
            }
            bucket.getMetrics().keySet().removeIf(mId -> !cachedGlobalActiveMetricIds.contains(mId));
        }

        if (eventNode.has("_matched_metrics") && eventNode.get("_matched_metrics").isArray()) {
            for (JsonNode m : eventNode.get("_matched_metrics")) {
                String metricId = m.has("metric_id") ? m.get("metric_id").asText() : null;
                String aggregation = m.has("aggregation") ? m.get("aggregation").asText() : null;
                double value = m.has("value") ? m.get("value").asDouble() : 0.0;

                if (metricId != null && aggregation != null) {
                    bucket.updateMetric(metricId, aggregation, value);
                }
            }
        }

        ringBufferMapState.put(slot, bucket);

        if (eventNode.isObject()) {
            ObjectNode en = (ObjectNode) eventNode;
            en.remove("_matched_metrics");
            en.remove("_global_active_metric_ids");
            en.remove("_ingest_time");
        }

        Map<String, RuleDefinition> activeRules = getOrRebuildRuleCache(ctx);
        if (activeRules != null && !activeRules.isEmpty()) {
            String entityId = ctx.getCurrentKey();
            long now = System.currentTimeMillis();

            for (RuleDefinition rule : activeRules.values()) {
                if (!rule.isEnabled()) {
                    continue;
                }

                if (rule.getTriggerEvents() != null && !rule.getTriggerEvents().isEmpty()) {
                    if (!eventNode.has("event_type")) {
                        continue;
                    }
                    String eventType = eventNode.get("event_type").asText();
                    if (!rule.getTriggerEvents().contains(eventType)) {
                        continue;
                    }
                }

                Long lastTriggered = ruleCooldownMapState.get(rule.getRuleId());
                long cooldownMs = rule.getCooldownSeconds() * 1000L;
                if (lastTriggered != null && eventTimeMs < lastTriggered + cooldownMs) {
                    continue;
                }

                boolean satisfied = RuleEvaluator.evaluateRule(rule, eventNode, eventTimeMs, ringBufferMapState);
                if (satisfied) {
                    ruleCooldownMapState.put(rule.getRuleId(), eventTimeMs);

                    ObjectNode auditNode = mapper.createObjectNode();
                    auditNode.put("alert_id", UUID.randomUUID().toString());
                    auditNode.put("entity_id", entityId);
                    auditNode.put("rule_id", rule.getRuleId());
                    auditNode.put("rule_name", rule.getName());
                    auditNode.put("rule_version", rule.getVersion());
                    auditNode.put("triggered_at", Instant.ofEpochMilli(now).toString());
                    auditNode.put("triggered_at_ms", now);
                    auditNode.put("event_time", Instant.ofEpochMilli(eventTimeMs).toString());
                    auditNode.put("event_time_ms", eventTimeMs);

                    ObjectNode triggerEventNode = mapper.createObjectNode();
                    if (eventNode.has("event_id")) triggerEventNode.put("event_id", eventNode.get("event_id").asText());
                    if (eventNode.has("event_type")) triggerEventNode.put("event_type", eventNode.get("event_type").asText());
                    if (eventNode.has("source_system")) triggerEventNode.put("source_system", eventNode.get("source_system").asText());
                    triggerEventNode.set("payload", eventNode);
                    auditNode.set("trigger_event", triggerEventNode);

                    ObjectNode auditMeta = mapper.createObjectNode();
                    auditMeta.put("cooldown_seconds", rule.getCooldownSeconds());
                    if (cooldownMs > 0) {
                        auditMeta.put("cooldown_until", Instant.ofEpochMilli(eventTimeMs + cooldownMs).toString());
                        auditMeta.put("cooldown_until_ms", eventTimeMs + cooldownMs);
                    } else {
                        auditMeta.putNull("cooldown_until");
                        auditMeta.put("cooldown_until_ms", 0L);
                    }
                    auditMeta.put("bucket_start_ms", bucketStart);
                    auditMeta.put("bucket_end_ms", bucketEnd);
                    auditMeta.put("bucket_close_time_ms", bucketCloseTime);
                    auditNode.set("audit", auditMeta);

                    out.collect(mapper.writeValueAsString(auditNode));
                }
            }
        }
    }

    private long extractEventTime(JsonNode eventNode) {
        if (eventNode != null && eventNode.has("event_time") && !eventNode.get("event_time").isNull()) {
            try {
                return Instant.parse(eventNode.get("event_time").asText()).toEpochMilli();
            } catch (Exception ignored) {
            }
        }
        return -1L;
    }
}

