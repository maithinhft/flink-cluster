package flink.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import flink.models.RingBuffer;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * KeyedProcessFunction for aggregating cleaned events into 5-minute Ring Buffer buckets (up to 24 hours).
 *
 * Implements:
 * 1. Late event rejection after bucket close threshold (bucketEndTime + 1 minute).
 * 2. 5 aggregate operations (SUM, COUNT, AVG, MIN, MAX) stored per bucket.
 * 3. 3-layer state cleanup:
 *    - Layer 1: Ring Buffer circular slot overwrite for buckets older than 24 hours.
 *    - Layer 2: Event-time timers fired at bucketCloseTime to close buckets and clean up aged buckets.
 *    - Layer 3: Flink StateTtlConfig (25 hours) to purge inactive entity state.
 */
public class BucketAggregationProcessFunction extends KeyedProcessFunction<String, String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(BucketAggregationProcessFunction.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final long LATE_TOLERANCE_MS = 60_000L; // 1 minute allowed lateness after bucket end
    public static final OutputTag<String> LATE_DATA_TAG = new OutputTag<String>("late-events") {};

    private transient ValueState<RingBuffer> ringBufferState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<RingBuffer> descriptor =
                new ValueStateDescriptor<>("ringBufferState", RingBuffer.class);

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.hours(25))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        descriptor.enableTimeToLive(ttlConfig);

        ringBufferState = getRuntimeContext().getState(descriptor);
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

        long eventTimeMs = extractEventTime(eventNode);
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

        RingBuffer ringBuffer = ringBufferState.value();
        if (ringBuffer == null) {
            ringBuffer = new RingBuffer();
        }

        if (eventNode.has("_matched_metrics") && eventNode.get("_matched_metrics").isArray()) {
            for (JsonNode m : eventNode.get("_matched_metrics")) {
                String metricId = m.has("metric_id") ? m.get("metric_id").asText() : null;
                String aggregation = m.has("aggregation") ? m.get("aggregation").asText() : null;
                double value = m.has("value") ? m.get("value").asDouble() : 0.0;

                if (metricId != null && aggregation != null) {
                    ringBuffer.updateMetric(eventTimeMs, metricId, aggregation, value);
                }
            }
        }

        ringBufferState.update(ringBuffer);

        ctx.timerService().registerEventTimeTimer(bucketCloseTime);

        ObjectNode outputNode = mapper.createObjectNode();
        outputNode.put("entity_id", ctx.getCurrentKey());
        outputNode.put("event_time", eventTimeMs);
        outputNode.put("bucket_start", bucketStart);
        outputNode.put("bucket_end", bucketEnd);
        outputNode.put("bucket_close_time", bucketCloseTime);
        outputNode.put("active_buckets", ringBuffer.getActiveBucketsCount());
        outputNode.set("event", eventNode);

        out.collect(mapper.writeValueAsString(outputNode));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        RingBuffer ringBuffer = ringBufferState.value();
        if (ringBuffer != null) {
            long cutoffTime = timestamp - RingBuffer.MAX_WINDOW_MS;
            ringBuffer.cleanup(cutoffTime);
            ringBufferState.update(ringBuffer);
            LOG.debug("Timer fired at {} for entity {}. Cleaned buckets <= {}", timestamp, ctx.getCurrentKey(), cutoffTime);
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

