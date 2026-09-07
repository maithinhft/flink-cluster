package flink.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import flink.models.Bucket;
import flink.models.RingBuffer;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;

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

    private transient MapState<Integer, Bucket> ringBufferMapState;
    private transient ValueState<Long> lastRegisteredTimerState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Integer, Bucket> mapDescriptor =
                new MapStateDescriptor<>("ringBufferMapState", Integer.class, Bucket.class);

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofHours(25))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        mapDescriptor.enableTimeToLive(ttlConfig);
        ringBufferMapState = getRuntimeContext().getMapState(mapDescriptor);

        ValueStateDescriptor<Long> timerDescriptor =
                new ValueStateDescriptor<>("lastRegisteredTimerState", Long.class);
        timerDescriptor.enableTimeToLive(ttlConfig);
        lastRegisteredTimerState = getRuntimeContext().getState(timerDescriptor);
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

        int slot = RingBuffer.getSlotIndex(bucketStart);
        Bucket bucket = ringBufferMapState.get(slot);

        if (bucket == null || bucketStart > bucket.getStartTimeMs()) {
            bucket = new Bucket(bucketStart);
        } else if (bucketStart < bucket.getStartTimeMs()) {
            if (eventNode.isObject()) {
                ((ObjectNode) eventNode).put("error_reason", "Circular buffer overwrite: slot holds newer bucket");
            }
            ctx.output(LATE_DATA_TAG, mapper.writeValueAsString(eventNode));
            return;
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

        Long lastTimer = lastRegisteredTimerState.value();
        if (lastTimer == null || bucketCloseTime > lastTimer) {
            ctx.timerService().registerEventTimeTimer(bucketCloseTime);
            lastRegisteredTimerState.update(bucketCloseTime);
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

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        long cutoffTime = timestamp - RingBuffer.MAX_WINDOW_MS;
        Iterator<Map.Entry<Integer, Bucket>> it = ringBufferMapState.iterator();
        while (it.hasNext()) {
            Map.Entry<Integer, Bucket> entry = it.next();
            Bucket b = entry.getValue();
            if (b != null && b.getEndTimeMs() <= cutoffTime) {
                it.remove();
            }
        }
        LOG.debug("Timer fired at {} for entity {}. Cleaned buckets <= {}", timestamp, ctx.getCurrentKey(), cutoffTime);
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

