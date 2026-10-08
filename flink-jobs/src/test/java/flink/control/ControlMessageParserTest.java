package flink.control;

import flink.model.ControlMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlMessageParserTest {

    @Test
    void parsesDebeziumRuleUpsertWithMicroTimestamps() {
        String cdc = "{\"id\":7,\"rule_id\":\"0d8c\",\"name\":\"vip\",\"rule_json\":\"{\\\"trigger_events\\\":[\\\"purchase\\\"]}\","
                + "\"cooldown_seconds\":60,\"version\":4,\"enabled\":true,\"created_at\":1700000000000000,"
                + "\"updated_at\":1700000123456789,\"__op\":\"u\",\"__table\":\"rule_definitions\","
                + "\"__deleted\":\"false\",\"__source_ts_ms\":1700000123999}";
        ControlMessage m = ControlMessageParser.parse(cdc);
        assertEquals(ControlMessage.Kind.RULE_UPSERT, m.kind);
        assertEquals("0d8c", m.id);
        assertEquals(4, m.version);
        assertEquals(1700000123456L, m.effectiveFromMs);
        assertEquals(1700000123999L, m.sourceTsMs);
    }

    @Test
    void parsesDeletesFromRewriteModeOrOp() {
        String a = "{\"rule_id\":\"x\",\"version\":2,\"__table\":\"rule_definitions\",\"__deleted\":\"true\"}";
        String b = "{\"rule_id\":\"x\",\"version\":2,\"__op\":\"d\"}";
        assertEquals(ControlMessage.Kind.RULE_DELETE, ControlMessageParser.parse(a).kind);
        assertEquals(ControlMessage.Kind.RULE_DELETE, ControlMessageParser.parse(b).kind);
    }

    @Test
    void parsesSchemaPayloadAsStringOrObject() {
        String s = "{\"schema_id\":\"ecommerce_purchase\",\"schema_payload\":\"{\\\"version\\\":\\\"1.2\\\",\\\"fields\\\":{}}\","
                + "\"__table\":\"schema_definitions\"}";
        ControlMessage m = ControlMessageParser.parse(s);
        assertEquals(ControlMessage.Kind.SCHEMA_UPSERT, m.kind);
        assertEquals("1.2", m.versionLabel);
        assertTrue(m.payload.contains("fields"));
        String o = "{\"schema_id\":\"crm_login\",\"schema_payload\":{\"version\":\"1.0\"}}";
        assertEquals("1.0", ControlMessageParser.parse(o).versionLabel);
    }

    @Test
    void dropsHeartbeatsTombstonesAndGarbage() {
        assertNull(ControlMessageParser.parse(null));
        assertNull(ControlMessageParser.parse(""));
        assertNull(ControlMessageParser.parse("{\"ts_ms\":1}"));
        assertNull(ControlMessageParser.parse("not json"));
        assertNull(ControlMessageParser.parse("{\"schema_id\":\"x\",\"schema_payload\":\"{}\"}"),
                "schema without version cannot be keyed");
    }

    @Test
    void timestampFormats() {
        com.fasterxml.jackson.databind.ObjectMapper m = new com.fasterxml.jackson.databind.ObjectMapper();
        assertEquals(1700000000000L, ControlMessageParser.timestamp(m.valueToTree(1700000000000000L)));
        assertEquals(1700000000000L, ControlMessageParser.timestamp(m.valueToTree(1700000000000L)));
        assertEquals(1700000000000L, ControlMessageParser.timestamp(m.valueToTree(1700000000L)));
        assertEquals(1700000000000L, ControlMessageParser.timestamp(m.valueToTree("2023-11-14T22:13:20Z")));
        assertEquals(1700000000000L, ControlMessageParser.timestamp(m.valueToTree("2023-11-14 22:13:20")));
    }
}
