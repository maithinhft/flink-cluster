package flink.evaluators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import flink.models.Bucket;
import flink.models.MetricBucketValue;
import flink.models.RingBuffer;
import flink.models.RuleDefinition;
import org.apache.flink.api.common.state.MapState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RuleEvaluator {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static boolean evaluateRule(RuleDefinition rule, JsonNode eventNode, long eventTimeMs,
                                       MapState<Integer, Bucket> ringBufferMapState) {
        if (rule == null || !rule.isEnabled()) {
            return false;
        }

        if (rule.getTriggerEvents() != null && !rule.getTriggerEvents().isEmpty()) {
            if (eventNode == null || !eventNode.has("event_type")) {
                return false;
            }
            String eventType = eventNode.get("event_type").asText();
            if (!rule.getTriggerEvents().contains(eventType)) {
                return false;
            }
        }

        JsonNode conditionNode = rule.getConditionNode();
        if (conditionNode == null) {
            conditionNode = rule.getOrParseCondition(mapper);
        }

        if (conditionNode == null || conditionNode.isNull() || conditionNode.isMissingNode()) {
            return true;
        }

        try {
            return evaluateCondition(conditionNode, eventNode, eventTimeMs, ringBufferMapState);
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean evaluateCondition(JsonNode conditionNode, JsonNode eventNode, long eventTimeMs,
                                           MapState<Integer, Bucket> ringBufferMapState) throws Exception {
        if (conditionNode == null || conditionNode.isNull() || conditionNode.isMissingNode()) {
            return true;
        }

        if (conditionNode.has("operator") && conditionNode.has("children")) {
            String op = conditionNode.get("operator").asText().trim().toUpperCase();
            JsonNode children = conditionNode.get("children");

            if ("AND".equals(op)) {
                if (children != null && children.isArray()) {
                    for (JsonNode child : children) {
                        if (!evaluateCondition(child, eventNode, eventTimeMs, ringBufferMapState)) {
                            return false;
                        }
                    }
                }
                return true;
            } else if ("OR".equals(op)) {
                if (children != null && children.isArray()) {
                    for (JsonNode child : children) {
                        if (evaluateCondition(child, eventNode, eventTimeMs, ringBufferMapState)) {
                            return true;
                        }
                    }
                }
                return false;
            } else if ("NOT".equals(op)) {
                if (children != null && children.isArray() && children.size() > 0) {
                    return !evaluateCondition(children.get(0), eventNode, eventTimeMs, ringBufferMapState);
                }
                return true;
            }
        }

        String type = conditionNode.has("type") ? conditionNode.get("type").asText() : "";
        if ("AGGREGATION".equalsIgnoreCase(type) || conditionNode.has("function") || conditionNode.has("window")) {
            return evaluateAggregation(conditionNode, eventTimeMs, ringBufferMapState);
        }

        return FilterEvaluator.evaluate(conditionNode, eventNode);
    }

    private static boolean evaluateAggregation(JsonNode aggNode, long eventTimeMs,
                                              MapState<Integer, Bucket> ringBufferMapState) throws Exception {
        String field = aggNode.has("field") ? aggNode.get("field").asText() : "event_id";
        String function = aggNode.has("function") ? aggNode.get("function").asText().toUpperCase() : "COUNT";

        long lookbackSeconds = 300L;
        if (aggNode.has("window") && aggNode.get("window").isObject()) {
            JsonNode win = aggNode.get("window");
            if (win.has("lookback_seconds") && win.get("lookback_seconds").asLong() > 0) {
                lookbackSeconds = win.get("lookback_seconds").asLong();
            } else if (win.has("size_seconds") && win.get("size_seconds").asLong() > 0) {
                lookbackSeconds = win.get("size_seconds").asLong();
            }
        }

        long lookbackMs = Math.min(RingBuffer.MAX_WINDOW_MS, Math.max(RingBuffer.BUCKET_DURATION_MS, lookbackSeconds * 1000L));
        long windowEndBucketStart = RingBuffer.normalizeToBucketStartTime(eventTimeMs);
        int numBuckets = Math.max(1, (int) Math.round((double) lookbackMs / RingBuffer.BUCKET_DURATION_MS));
        numBuckets = Math.min(numBuckets, RingBuffer.TOTAL_BUCKETS);
        long windowStartBucketStart = windowEndBucketStart - (long) (numBuckets - 1) * RingBuffer.BUCKET_DURATION_MS;

        List<String> candidates = new ArrayList<>();
        if (aggNode.has("metric_id")) {
            candidates.add(aggNode.get("metric_id").asText());
        }
        buildMetricCandidates(candidates, field, function, aggNode.get("filter"));

        MetricBucketValue combined = new MetricBucketValue();
        for (long bStart = windowStartBucketStart; bStart <= windowEndBucketStart; bStart += RingBuffer.BUCKET_DURATION_MS) {
            int slot = RingBuffer.getSlotIndex(bStart);
            Bucket b = ringBufferMapState.get(slot);
            if (b != null && b.getStartTimeMs() == bStart && b.getMetrics() != null) {
                MetricBucketValue val = findMatchingMetricValue(b.getMetrics(), candidates);
                if (val != null) {
                    combined.merge(val);
                }
            }
        }

        double actualValue = combined.getValue(function);
        String operator = aggNode.has("operator") ? aggNode.get("operator").asText() : "GTE";
        JsonNode targetValue = aggNode.get("value");

        return FilterEvaluator.compare(mapper.valueToTree(actualValue), operator, targetValue);
    }

    private static void buildMetricCandidates(List<String> candidates, String field, String function, JsonNode filterNode) {
        String funcLower = function.toLowerCase();
        List<String> bases = new ArrayList<>();

        if ("COUNT".equalsIgnoreCase(function) && ("event_id".equalsIgnoreCase(field) || "events".equalsIgnoreCase(field))) {
            bases.add("count_events");
            bases.add("count_event_id");
        } else {
            bases.add(funcLower + "_" + field);
            if ("event_id".equalsIgnoreCase(field)) {
                bases.add(funcLower + "_events");
            }
        }

        for (String b : bases) {
            candidates.add(b);
        }

        if (filterNode != null && !filterNode.isNull() && !filterNode.isMissingNode()) {
            List<String> filterSuffixes = new ArrayList<>();
            extractFilterSuffixes(filterNode, filterSuffixes);

            for (String b : bases) {
                for (String suffix : filterSuffixes) {
                    candidates.add(0, b + "_where_" + suffix);
                }
            }
        }
    }

    private static void extractFilterSuffixes(JsonNode filterNode, List<String> suffixes) {
        if (filterNode.has("field") && filterNode.has("value") && filterNode.get("value").isValueNode()) {
            suffixes.add(filterNode.get("field").asText() + "_" + filterNode.get("value").asText());
        } else if (filterNode.has("children") && filterNode.get("children").isArray()) {
            StringBuilder combined = new StringBuilder();
            for (JsonNode child : filterNode.get("children")) {
                if (child.has("field") && child.has("value") && child.get("value").isValueNode()) {
                    suffixes.add(child.get("field").asText() + "_" + child.get("value").asText());
                    if (combined.length() > 0) combined.append("_and_");
                    combined.append(child.get("field").asText()).append("_").append(child.get("value").asText());
                }
            }
            if (combined.length() > 0) {
                suffixes.add(0, combined.toString());
            }
        }
    }

    private static MetricBucketValue findMatchingMetricValue(Map<String, MetricBucketValue> metrics, List<String> candidates) {
        for (String cand : candidates) {
            if (metrics.containsKey(cand)) {
                return metrics.get(cand);
            }
        }
        for (String cand : candidates) {
            for (Map.Entry<String, MetricBucketValue> entry : metrics.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(cand)) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }
}

