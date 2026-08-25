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

    // MapStateDescriptor để định nghĩa cấu trúc lưu trữ Schema (Key: Tên schema,
    // Value: Nội dung JSON của Schema)
    public static final MapStateDescriptor<String, String> SCHEMA_STATE_DESCRIPTOR = new MapStateDescriptor<>(
            "schemaBroadcastState", Types.STRING, Types.STRING);

    // OutputTag để rẽ nhánh dữ liệu bẩn ra luồng riêng (Side Output)
    public static final OutputTag<String> DIRTY_DATA_TAG = new OutputTag<String>("dirty-events") {
    };

    @Override
    public void processElement(String eventJson, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        try {
            JsonNode eventNode = mapper.readTree(eventJson);

            // Xác định source_system và action để suy ra Schema Key (VD: crm_login)
            if (!eventNode.has("source_system") || !eventNode.has("event_type")) {
                // Thiếu thông tin cơ bản để map schema -> Đẩy thẳng vào DIRTY
                ((ObjectNode) eventNode).put("error_reason", "Missing source_system or action for schema mapping");
                ctx.output(DIRTY_DATA_TAG, mapper.writeValueAsString(eventNode));
                return;
            }

            String sourceName = eventNode.get("source_system").asText();
            String action = eventNode.get("event_type").asText();
            String schemaKey = sourceName + "_" + action;

            ReadOnlyBroadcastState<String, String> schemaState = ctx.getBroadcastState(SCHEMA_STATE_DESCRIPTOR);
            String schemaJson = schemaState.get(schemaKey);

            if (schemaJson == null) {
                // Nếu chưa nhận được schema từ Broadcast, có thể tạm coi là hợp lệ hoặc không.
                // Ở đây ta ghi nhận lỗi thiếu schema.
                ((ObjectNode) eventNode).put("error_reason", "Schema not found for key: " + schemaKey);
                ctx.output(DIRTY_DATA_TAG, mapper.writeValueAsString(eventNode));
                return;
            }

            // Parse schema để kiểm tra các trường
            JsonNode schemaNode = mapper.readTree(schemaJson);
            JsonNode fieldsNode = schemaNode.get("fields");
            if (fieldsNode == null) {
                out.collect(eventJson);
                return;
            }

            List<String> missingFields = new ArrayList<>();
            // Event payload is a flat JSON, so the fields are at the root of the eventNode
            JsonNode eventFields = eventNode;
            
            Iterator<Map.Entry<String, JsonNode>> fieldsIter = fieldsNode.fields();
            while (fieldsIter.hasNext()) {
                Map.Entry<String, JsonNode> fieldEntry = fieldsIter.next();
                String fieldName = fieldEntry.getKey();
                JsonNode fieldDef = fieldEntry.getValue();

                // Kiểm tra cờ required
                if (fieldDef.has("required") && fieldDef.get("required").asBoolean()) {
                    if (!eventFields.has(fieldName) || eventFields.get(fieldName).isNull()) {
                        missingFields.add(fieldName);
                    }
                }
            }

            if (!missingFields.isEmpty()) {
                // Sự kiện thiếu trường bắt buộc -> DIRTY DATA
                ((ObjectNode) eventNode).put("error_reason",
                        "Missing required fields: " + String.join(", ", missingFields));
                ctx.output(DIRTY_DATA_TAG, mapper.writeValueAsString(eventNode));
            } else {
                // Dữ liệu Sạch -> Đẩy ra luồng chính
                out.collect(eventJson);
            }

        } catch (Exception e) {
            // Lỗi parse JSON -> Chắc chắn là DIRTY DATA
            LOG.warn("Failed to parse event JSON", e);
            ObjectNode errorNode = mapper.createObjectNode();
            errorNode.put("original_payload", eventJson);
            errorNode.put("error_reason", "JSON Parse Exception: " + e.getMessage());
            ctx.output(DIRTY_DATA_TAG, mapper.writeValueAsString(errorNode));
        }
    }

    @Override
    public void processBroadcastElement(String messageJson, Context ctx, Collector<String> out) throws Exception {
        try {
            JsonNode messageNode = mapper.readTree(messageJson);
            
            String schemaKey;
            String schemaJson;

            // Hỗ trợ luồng dữ liệu mới từ PostgreSQL CDC
            if (messageNode.has("schema_payload") && messageNode.has("schema_id")) {
                schemaKey = messageNode.get("schema_id").asText();
                schemaJson = messageNode.get("schema_payload").asText(); // Debezium convert JSONB thành String
            } else {
                // Hỗ trợ ngược cho luồng dữ liệu cũ bắn trực tiếp lên Kafka
                schemaJson = messageJson;
                if (messageNode.has("name") && "unified_dictionary".equals(messageNode.get("name").asText())) {
                    schemaKey = "unified_schema";
                } else {
                    String sourceName = messageNode.has("source_name") ? messageNode.get("source_name").asText() : "unknown";
                    String action = messageNode.has("action") ? messageNode.get("action").asText() : "unknown";
                    schemaKey = sourceName + "_" + action;
                }
            }

            BroadcastState<String, String> broadcastState = ctx.getBroadcastState(SCHEMA_STATE_DESCRIPTOR);
            broadcastState.put(schemaKey, schemaJson);
            
            LOG.info("Received and cached schema for key: {}", schemaKey);
            
        } catch (Exception e) {
            LOG.error("Failed to parse and store schema from Broadcast Stream", e);
        }
    }
}
