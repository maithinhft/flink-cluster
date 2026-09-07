package flink.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SchemaValidationBroadcastProcessFunction extends BroadcastProcessFunction<String, String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaValidationBroadcastProcessFunction.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static final MapStateDescriptor<String, String> SCHEMA_STATE_DESCRIPTOR = new MapStateDescriptor<>(
            "schemaBroadcastState", Types.STRING, Types.STRING);

    public static final MapStateDescriptor<String, Long> DEPRECATED_SCHEMAS_DESCRIPTOR = new MapStateDescriptor<>(
            "deprecatedSchemas", Types.STRING, Types.LONG);

    public static final MapStateDescriptor<String, String> LATEST_VERSION_DESCRIPTOR = new MapStateDescriptor<>(
            "latestSchemaVersions", Types.STRING, Types.STRING);

    public static final OutputTag<String> DIRTY_DATA_TAG = new OutputTag<String>("dirty-events") {
    };

    @Override
    public void processElement(String eventJson, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        try {
            JsonNode eventNode = mapper.readTree(eventJson);

            if (!eventNode.has("source_system") || !eventNode.has("event_type") || !eventNode.has("schema_version")) {
                ((ObjectNode) eventNode).put("error_reason",
                        "Missing source_system or action or version for schema mapping");
                ctx.output(DIRTY_DATA_TAG, mapper.writeValueAsString(eventNode));
                return;
            }

            String sourceName = eventNode.get("source_system").asText();
            String eventType = eventNode.get("event_type").asText();
            String version = eventNode.get("schema_version").asText();
            String schemaKey = sourceName + "_" + eventType + "_" + version;

            ReadOnlyBroadcastState<String, String> schemaState = ctx.getBroadcastState(SCHEMA_STATE_DESCRIPTOR);
            String schemaJson = schemaState.get(schemaKey);

            if (schemaJson == null) {
                ((ObjectNode) eventNode).put("error_reason", "Schema not found for key: " + schemaKey);
                ctx.output(DIRTY_DATA_TAG, mapper.writeValueAsString(eventNode));
                return;
            }

            JsonNode schemaNode = mapper.readTree(schemaJson);
            JsonNode fieldsNode = schemaNode.get("fields");
            if (fieldsNode == null) {
                evaluateMetricsAndCollect(eventNode, schemaNode, out);
                return;
            }

            List<String> missingFields = new ArrayList<>();
            List<String> invalidTypeFields = new ArrayList<>();
            List<String> invalidConstraintFields = new ArrayList<>();
            JsonNode eventFields = eventNode;

            Iterator<Map.Entry<String, JsonNode>> fieldsIter = fieldsNode.fields();
            while (fieldsIter.hasNext()) {
                Map.Entry<String, JsonNode> fieldEntry = fieldsIter.next();
                String fieldName = fieldEntry.getKey();
                JsonNode fieldDef = fieldEntry.getValue();

                boolean isPresent = eventFields.has(fieldName) && !eventFields.get(fieldName).isNull();

                if (fieldDef.has("required") && fieldDef.get("required").asBoolean()) {
                    if (!isPresent) {
                        missingFields.add(fieldName);
                    }
                }

                if (isPresent) {
                    JsonNode val = eventFields.get(fieldName);
                    
                    if (fieldDef.has("type")) {
                        String type = fieldDef.get("type").asText();
                        if (type.equals("string") && !val.isTextual()) {
                            invalidTypeFields.add(fieldName + " (expected string)");
                        } else if (type.equals("integer") && !val.isIntegralNumber()) {
                            invalidTypeFields.add(fieldName + " (expected integer)");
                        } else if (type.equals("number") && !val.isNumber()) {
                            invalidTypeFields.add(fieldName + " (expected number)");
                        } else if (type.equals("boolean") && !val.isBoolean()) {
                            invalidTypeFields.add(fieldName + " (expected boolean)");
                        }
                    }

                    if (fieldDef.has("min") && val.isNumber()) {
                        if (val.asDouble() < fieldDef.get("min").asDouble()) {
                            invalidConstraintFields.add(fieldName + " (< min " + fieldDef.get("min").asText() + ")");
                        }
                    }
                    if (fieldDef.has("max") && val.isNumber()) {
                        if (val.asDouble() > fieldDef.get("max").asDouble()) {
                            invalidConstraintFields.add(fieldName + " (> max " + fieldDef.get("max").asText() + ")");
                        }
                    }
                    if (fieldDef.has("allowed_values")) {
                        boolean matched = false;
                        for (JsonNode allowedVal : fieldDef.get("allowed_values")) {
                            if (val.asText().equals(allowedVal.asText())) {
                                matched = true;
                                break;
                            }
                        }
                        if (!matched) {
                            invalidConstraintFields.add(fieldName + " (not in allowed_values)");
                        }
                    }
                }
            }

            if (!missingFields.isEmpty() || !invalidTypeFields.isEmpty() || !invalidConstraintFields.isEmpty()) {
                List<String> reasons = new ArrayList<>();
                if (!missingFields.isEmpty()) reasons.add("Missing: " + String.join(", ", missingFields));
                if (!invalidTypeFields.isEmpty()) reasons.add("Invalid Type: " + String.join(", ", invalidTypeFields));
                if (!invalidConstraintFields.isEmpty()) reasons.add("Constraint: " + String.join(", ", invalidConstraintFields));
                
                ((ObjectNode) eventNode).put("error_reason", String.join(" | ", reasons));
                ctx.output(DIRTY_DATA_TAG, mapper.writeValueAsString(eventNode));
            } else {
                evaluateMetricsAndCollect(eventNode, schemaNode, out);
            }

        } catch (Exception e) {
            LOG.warn("Failed to parse event JSON", e);
            ObjectNode errorNode = mapper.createObjectNode();
            errorNode.put("original_payload", eventJson);
            errorNode.put("error_reason", "JSON Parse Exception: " + e.getMessage());
            ctx.output(DIRTY_DATA_TAG, mapper.writeValueAsString(errorNode));
        }
    }

    private int compareVersions(String v1, String v2) {
        try {
            String[] parts1 = v1.split("\\.");
            String[] parts2 = v2.split("\\.");
            int length = Math.max(parts1.length, parts2.length);
            for (int i = 0; i < length; i++) {
                int p1 = i < parts1.length ? Integer.parseInt(parts1[i]) : 0;
                int p2 = i < parts2.length ? Integer.parseInt(parts2[i]) : 0;
                if (p1 != p2) {
                    return Integer.compare(p1, p2);
                }
            }
            return 0;
        } catch (Exception e) {
            return v1.compareTo(v2);
        }
    }

    @Override
    public void processBroadcastElement(String messageJson, Context ctx, Collector<String> out) throws Exception {
        try {
            JsonNode messageNode = mapper.readTree(messageJson);

            if (messageNode.has("schema_payload") && messageNode.has("schema_id")) {

                String schemaId = messageNode.get("schema_id").asText();
                String schemaJson = messageNode.get("schema_payload").asText();

                JsonNode payloadNode = mapper.readTree(schemaJson);

                if (payloadNode.has("version")) {
                    String version = payloadNode.get("version").asText();
                    String schemaKey = schemaId + "_" + version;

                    BroadcastState<String, String> schemaState = ctx.getBroadcastState(SCHEMA_STATE_DESCRIPTOR);
                    BroadcastState<String, String> latestVersionState = ctx
                            .getBroadcastState(LATEST_VERSION_DESCRIPTOR);
                    BroadcastState<String, Long> deprecatedState = ctx.getBroadcastState(DEPRECATED_SCHEMAS_DESCRIPTOR);

                    String currentLatestVersion = latestVersionState.get(schemaId);

                    if (currentLatestVersion == null) {
                        latestVersionState.put(schemaId, version);
                        schemaState.put(schemaKey, schemaJson);
                        LOG.info("Received and cached initial schema for key: {}", schemaKey);
                    } else {
                        int cmp = compareVersions(version, currentLatestVersion);
                        if (cmp > 0) {
                            String oldSchemaKey = schemaId + "_" + currentLatestVersion;
                            deprecatedState.put(oldSchemaKey, ctx.currentProcessingTime());
                            LOG.info("Deprecated old schema version: {}", oldSchemaKey);

                            latestVersionState.put(schemaId, version);
                            schemaState.put(schemaKey, schemaJson);
                            LOG.info("Received and cached newer schema for key: {}", schemaKey);
                        } else if (cmp < 0) {
                            schemaState.put(schemaKey, schemaJson);
                            deprecatedState.put(schemaKey, ctx.currentProcessingTime());
                            LOG.info("Received out-of-order old schema version: {}, immediately deprecating it",
                                    schemaKey);
                        } else {
                            schemaState.put(schemaKey, schemaJson);
                        }
                    }

                    long currentTime = ctx.currentProcessingTime();
                    List<String> keysToRemove = new ArrayList<>();
                    for (Map.Entry<String, Long> entry : deprecatedState.entries()) {
                        if (currentTime - entry.getValue() > 5 * 60 * 1000L) {
                            keysToRemove.add(entry.getKey());
                        }
                    }

                    for (String key : keysToRemove) {
                        schemaState.remove(key);
                        deprecatedState.remove(key);
                        LOG.info("Cleaned up expired old schema: {}", key);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to parse and store schema from Broadcast Stream", e);
        }
    }

    private void evaluateMetricsAndCollect(JsonNode eventNode, JsonNode schemaNode, Collector<String> out) {
        try {
            if (schemaNode != null && schemaNode.has("metrics")) {
                JsonNode metricsNode = schemaNode.get("metrics");
                if (metricsNode != null && metricsNode.isArray()) {
                    com.fasterxml.jackson.databind.node.ArrayNode matchedMetricsArr = mapper.createArrayNode();
                    for (JsonNode mDef : metricsNode) {
                        String metricId = mDef.has("metric_id") ? mDef.get("metric_id").asText() : null;
                        String sourceField = mDef.has("source_field") ? mDef.get("source_field").asText() : null;
                        String aggregation = mDef.has("aggregation") ? mDef.get("aggregation").asText() : null;
                        JsonNode filter = mDef.get("filter");

                        if (metricId != null && aggregation != null) {
                            if (flink.evaluators.FilterEvaluator.evaluate(filter, eventNode)) {
                                double val = 0.0;
                                if ("COUNT".equalsIgnoreCase(aggregation)) {
                                    val = 1.0;
                                } else if (sourceField != null && eventNode.has(sourceField) && !eventNode.get(sourceField).isNull()) {
                                    JsonNode sfVal = eventNode.get(sourceField);
                                    if (sfVal.isNumber()) {
                                        val = sfVal.asDouble();
                                    } else if (sfVal.isTextual()) {
                                        try {
                                            val = java.time.Instant.parse(sfVal.asText()).toEpochMilli();
                                        } catch (Exception e) {
                                            try {
                                                val = Double.parseDouble(sfVal.asText());
                                            } catch (Exception ignored) {
                                                val = 0.0;
                                            }
                                        }
                                    }
                                }
                                com.fasterxml.jackson.databind.node.ObjectNode mObj = mapper.createObjectNode();
                                mObj.put("metric_id", metricId);
                                mObj.put("aggregation", aggregation);
                                mObj.put("value", val);
                                matchedMetricsArr.add(mObj);
                            }
                        }
                    }
                    ((ObjectNode) eventNode).set("_matched_metrics", matchedMetricsArr);
                }
            }
            out.collect(mapper.writeValueAsString(eventNode));
        } catch (Exception e) {
            LOG.error("Failed to evaluate metrics for valid event", e);
            try {
                out.collect(mapper.writeValueAsString(eventNode));
            } catch (Exception ignored) {
            }
        }
    }
}
