package flink.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import flink.models.Bucket;
import flink.models.RingBuffer;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public class BucketAggregationProcessFunction extends KeyedProcessFunction<String, String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(BucketAggregationProcessFunction.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final long LATE_TOLERANCE_MS = 60_000L;
    public static final OutputTag<String> LATE_DATA_TAG = new OutputTag<String>("late-events") {};

    private transient MapState<Integer, Bucket> ringBufferMapState;
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

        pipelineLatencyHistogram = new org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram(2048);
        getRuntimeContext().getMetricGroup().histogram("pipeline_latency_ms", pipelineLatencyHistogram);

        eventTimeLagHistogram = new org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram(2048);
        getRuntimeContext().getMetricGroup().histogram("event_time_lag_ms", eventTimeLagHistogram);
    }

    @Override
    public void processElement(String eventJson, Context ctx, Collector<String> out) throws Exception {
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

        ObjectNode outputNode = mapper.createObjectNode();
        outputNode.put("entity_id", ctx.getCurrentKey());
        outputNode.put("event_time", eventTimeMs);
        outputNode.put("bucket_start", bucketStart);
        outputNode.put("bucket_end", bucketEnd);
        outputNode.put("bucket_close_time", bucketCloseTime);
        outputNode.set("event", eventNode);

        out.collect(mapper.writeValueAsString(outputNode));
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
