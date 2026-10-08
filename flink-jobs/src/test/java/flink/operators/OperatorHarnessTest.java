package flink.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import flink.engine.EngineConfig;
import flink.model.ControlMessage;
import flink.model.KeyedRecord;
import flink.model.RawEvent;
import flink.model.ValidatedEvent;
import flink.state.TimeBuckets;
import flink.testutil.TestSupport;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.BroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedBroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static flink.testutil.TestSupport.T0;
import static flink.testutil.TestSupport.event;
import static flink.testutil.TestSupport.rule;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperatorHarnessTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final String PURCHASE_RULE = "{\"trigger_events\":[\"purchase\"],\"condition\":{\"type\":\"AGGREGATION\","
            + "\"field\":\"total_amount\",\"function\":\"SUM\",\"window\":{\"lookback_seconds\":3600},\"operator\":\"GTE\",\"value\":10}}";

    private static KeyedBroadcastOperatorTestHarness<String, ValidatedEvent, ControlMessage, KeyedRecord> engineHarness()
            throws Exception {
        CoBroadcastWithKeyedOperator<String, ValidatedEvent, ControlMessage, KeyedRecord> op =
                new CoBroadcastWithKeyedOperator<>(new RingBufferRuleProcessFunction(TestSupport.config()),
                        Arrays.asList(RingBufferRuleProcessFunction.RULE_STATE_DESCRIPTOR,
                                RingBufferRuleProcessFunction.SCHEMA_STATE_DESCRIPTOR,
                                RingBufferRuleProcessFunction.SPEC_STATE_DESCRIPTOR));
        return new KeyedBroadcastOperatorTestHarness<>(op, ev -> ev.entityId, Types.STRING, 1, 1, 0);
    }

    private static int keyedEntries(KeyedBroadcastOperatorTestHarness<String, ValidatedEvent, ControlMessage, KeyedRecord> h) {
        return ((HeapKeyedStateBackend<?>) h.getOperator().getKeyedStateBackend()).numKeyValueStateEntries();
    }

    private static Set<String> firedRules(List<KeyedRecord> out) throws Exception {
        Set<String> s = new HashSet<>();
        for (KeyedRecord r : out) {
            s.add(M.readTree(r.value).get("rule_id").asText());
        }
        return s;
    }

    @Test
    void restoredRulesSurviveARuleArrivingBeforeTheFirstEvent() throws Exception {
        OperatorSubtaskState snapshot;
        try (var h = engineHarness()) {
            h.setup();
            h.open();
            h.processBroadcastWatermark(Long.MAX_VALUE - 1);
            h.processBroadcastElement(rule("a", 1, PURCHASE_RULE, 0), 1L);
            h.processBroadcastElement(rule("b", 1, PURCHASE_RULE.replace("\"value\":10", "\"value\":5"), 0), 1L);
            h.processElement(event("u1", "purchase", T0).num("total_amount", 6).build(), T0);
            assertEquals(Set.of("b"), firedRules(h.extractOutputValues()));
            snapshot = h.snapshot(1L, 1L);
        }
        try (var h = engineHarness()) {
            h.setup();
            h.initializeState(snapshot);
            h.open();
            h.processBroadcastWatermark(Long.MAX_VALUE - 1);
            h.processBroadcastElement(rule("c", 1, PURCHASE_RULE.replace("\"value\":10", "\"value\":12"), 0), 2L);
            h.processElement(event("u1", "purchase", T0 + 1000).num("total_amount", 6).build(), T0 + 1000);
            assertEquals(Set.of("a", "b", "c"), firedRules(h.extractOutputValues()),
                    "state (sum=12) and all three rules must be present after restore");
        }
    }

    @Test
    void entityStateIsClearedByEventTimeTimerOnlyWhenIdle() throws Exception {
        try (var h = engineHarness()) {
            h.setup();
            h.open();
            h.processBroadcastWatermark(Long.MAX_VALUE - 1);
            h.processBroadcastElement(rule("a", 1, PURCHASE_RULE, 0), 1L);
            h.processElement(event("u1", "purchase", T0).num("total_amount", 1).build(), T0);
            assertEquals(1, h.numEventTimeTimers());
            assertTrue(keyedEntries(h) > 0);

            long firstDeadline = TimeBuckets.roundUpToBucket(T0 + TimeBuckets.MAX_WINDOW_MS + TimeBuckets.BUCKET_MS + 60_000L);
            h.processWatermark(T0 + 23 * 3_600_000L);
            h.processElement(event("u1", "purchase", T0 + 23 * 3_600_000L).num("total_amount", 1).build(), T0 + 23 * 3_600_000L);
            h.processWatermark(firstDeadline);
            assertEquals(1, h.numEventTimeTimers(), "timer re-armed because the entity was active");
            assertTrue(keyedEntries(h) > 0);

            h.processWatermark(firstDeadline + 24 * 3_600_000L);
            assertEquals(0, h.numEventTimeTimers());
            assertEquals(0, keyedEntries(h), "layer 1: idle entity fully cleared");
        }
    }

    @Test
    void lateEventsGoToSideOutputOnceWatermarkPassesBucketClose() throws Exception {
        try (var h = engineHarness()) {
            h.setup();
            h.open();
            h.processBroadcastWatermark(Long.MAX_VALUE - 1);
            h.processBroadcastElement(rule("a", 1, PURCHASE_RULE, 0), 1L);
            h.processWatermark(T0 + TimeBuckets.BUCKET_MS + 60_000L);
            h.processElement(event("u1", "purchase", T0).num("total_amount", 100).build(), T0);
            ConcurrentLinkedQueue<StreamRecord<KeyedRecord>> late = h.getSideOutput(RingBufferRuleProcessFunction.LATE_DATA_TAG);
            assertEquals(1, late.size());
            assertTrue(late.peek().getValue().value.contains("Late event rejected"));
            assertTrue(h.extractOutputValues().isEmpty());
        }
    }

    @Test
    void invalidRuleIsReportedAndPreviousVersionKept() throws Exception {
        try (var h = engineHarness()) {
            h.setup();
            h.open();
            h.processBroadcastWatermark(Long.MAX_VALUE - 1);
            h.processBroadcastElement(rule("a", 1, PURCHASE_RULE, 0), 1L);
            h.processBroadcastElement(rule("a", 2, "{\"trigger_events\":[\"purchase\"],\"condition\":{\"field\":\"x\",\"operator\":\"LIKE\",\"value\":1}}", 0), 2L);
            assertEquals(1, h.getSideOutput(RingBufferRuleProcessFunction.LATE_DATA_TAG).size());
            h.processElement(event("u1", "purchase", T0).num("total_amount", 50).build(), T0);
            assertEquals(Set.of("a"), firedRules(h.extractOutputValues()));
        }
    }

    private static final String SCHEMA = "{\"version\":\"1.0\",\"source_name\":\"ecommerce\",\"action\":\"purchase\",\"fields\":{"
            + "\"event_id\":{\"type\":\"string\",\"required\":true},"
            + "\"entity_id\":{\"type\":\"string\",\"required\":true},"
            + "\"event_time\":{\"type\":\"string\",\"format\":\"ISO-8601\",\"required\":true},"
            + "\"total_amount\":{\"type\":\"double\",\"min\":0},"
            + "\"product_category\":{\"type\":\"string\",\"allowed_values\":[\"electronics\",\"books\"]}},"
            + "\"metrics\":[{\"metric_id\":\"sum_total_amount\",\"source_field\":\"total_amount\",\"aggregation\":\"SUM\"}]}";

    private static RawEvent raw(String extra) {
        return new RawEvent("{\"event_id\":\"e1\",\"entity_id\":\"u1\",\"event_type\":\"purchase\",\"source_system\":\"ecommerce\","
                + "\"schema_version\":\"1.0\",\"event_time\":\"2023-11-14T22:15:00Z\",\"ignored_field\":\"zzz\"" + extra + "}",
                1700000100000L, "events_ecommerce");
    }

    private static BroadcastOperatorTestHarness<RawEvent, ControlMessage, ValidatedEvent> validationHarness(EngineConfig cfg)
            throws Exception {
        return ProcessFunctionTestHarnesses.forBroadcastProcessFunction(
                new DynamicSchemaValidationFunction(cfg), DynamicSchemaValidationFunction.descriptors());
    }

    @Test
    void validationProjectsCleanEventsAndRoutesDirtyOnes() throws Exception {
        try (var h = validationHarness(TestSupport.config())) {
            h.processBroadcastElement(TestSupport.schema("ecommerce_purchase", "1.0", SCHEMA), 1L);
            h.processElement(raw(",\"total_amount\":12.5,\"product_category\":\"books\""), 1L);
            h.processElement(raw(",\"total_amount\":-1"), 2L);
            h.processElement(raw(",\"total_amount\":\"12\""), 3L);
            h.processElement(raw(",\"product_category\":\"toys\""), 4L);
            h.processElement(new RawEvent("{broken", 0L, "t"), 5L);
            h.processElement(new RawEvent("{\"event_id\":\"x\",\"entity_id\":\"u\",\"event_type\":\"purchase\",\"source_system\":\"ecommerce\","
                    + "\"schema_version\":\"1.0\",\"event_time\":\"yesterday\"}", 0L, "t"), 6L);

            List<ValidatedEvent> ok = h.extractOutputValues();
            assertEquals(1, ok.size());
            ValidatedEvent ev = ok.get(0);
            assertEquals("u1", ev.entityId);
            assertEquals(1700000100000L, ev.ingestTimeMs);
            assertTrue(ev.find("total_amount") >= 0, "metric source field is projected");
            assertTrue(ev.find("ignored_field") < 0, "unreferenced field is dropped");

            List<String> reasons = new java.util.ArrayList<>();
            for (StreamRecord<KeyedRecord> r : h.getSideOutput(DynamicSchemaValidationFunction.DIRTY_DATA_TAG)) {
                JsonNode n = M.readTree(r.getValue().value);
                reasons.add(n.get("error_reason").asText());
            }
            assertEquals(5, reasons.size(), reasons.toString());
            assertTrue(reasons.get(0).contains("< min"));
            assertTrue(reasons.get(1).contains("expected number"));
            assertTrue(reasons.get(2).contains("allowed_values"));
            assertTrue(reasons.get(3).contains("JSON Parse"));
            assertTrue(reasons.get(4).contains("ISO-8601"));
        }
    }

    @Test
    void ruleFieldsExtendProjectionAndColdStartBufferFlushesOnSchemaArrival() throws Exception {
        try (var h = validationHarness(TestSupport.config())) {
            h.processElement(raw(",\"total_amount\":3,\"ignored_field2\":1"), 1L);
            assertTrue(h.extractOutputValues().isEmpty());
            assertTrue(h.getSideOutput(DynamicSchemaValidationFunction.DIRTY_DATA_TAG) == null
                    || h.getSideOutput(DynamicSchemaValidationFunction.DIRTY_DATA_TAG).isEmpty(), "buffered, not dirty");

            h.processBroadcastElement(rule("r", 1, "{\"trigger_events\":[\"purchase\"],\"condition\":"
                    + "{\"field\":\"ignored_field2\",\"operator\":\"EQ\",\"value\":1}}", 0), 2L);
            h.processBroadcastElement(TestSupport.schema("ecommerce_purchase", "1.0", SCHEMA), 3L);
            List<ValidatedEvent> out = h.extractOutputValues();
            assertEquals(1, out.size());
            assertTrue(out.get(0).find("ignored_field2") >= 0);
        }
    }

    @Test
    void unknownSchemaAfterColdStartWindowIsDirty() throws Exception {
        EngineConfig cfg = TestSupport.config();
        cfg.coldStartBufferMs = 0;
        try (var h = validationHarness(cfg)) {
            h.processElement(raw(""), 1L);
            assertEquals(1, h.getSideOutput(DynamicSchemaValidationFunction.DIRTY_DATA_TAG).size());
        }
    }
}
