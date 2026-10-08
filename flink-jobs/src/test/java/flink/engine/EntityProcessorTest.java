package flink.engine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import flink.state.CompactBucket;
import flink.state.TimeBuckets;
import flink.testutil.TestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static flink.testutil.TestSupport.T0;
import static flink.testutil.TestSupport.event;
import static flink.testutil.TestSupport.rule;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EntityProcessorTest {

    private static final ObjectMapper M = new ObjectMapper();
    private static final long B = TimeBuckets.BUCKET_MS;

    private RuleCatalog catalog;
    private EntityProcessor processor;
    private TestSupport.Entity user;

    @BeforeEach
    void setUp() {
        EngineConfig cfg = TestSupport.config();
        catalog = new RuleCatalog(cfg);
        processor = new EntityProcessor(catalog, cfg);
        user = new TestSupport.Entity(processor);
    }

    private void addRule(String id, String json, long cooldownSeconds) {
        RuleCatalog.Result r = catalog.apply(rule(id, 1, json, cooldownSeconds), 0L);
        assertEquals(RuleCatalog.Status.APPLIED, r.status, String.valueOf(r.error));
    }

    private static String sumRule(String trigger, long lookback, String op, double value) {
        return "{\"trigger_events\":[\"" + trigger + "\"],\"condition\":{\"type\":\"AGGREGATION\",\"field\":\"total_amount\","
                + "\"function\":\"SUM\",\"window\":{\"lookback_seconds\":" + lookback + "},\"operator\":\"" + op + "\",\"value\":" + value + "}}";
    }

    @Test
    void triggerEventsGateEvaluation() throws Exception {
        addRule("r1", "{\"trigger_events\":[\"purchase\",\"payment_success\"],\"condition\":"
                + "{\"type\":\"RAW_FIELD\",\"field\":\"source_system\",\"operator\":\"EQ\",\"value\":\"ecommerce\"}}", 0);
        user.send(event("u1", "purchase", T0).str("source_system", "ecommerce").build());
        assertEquals(1, user.alerts.size());
        user.send(event("u1", "login", T0 + 1000).str("source_system", "ecommerce").build());
        assertEquals(1, user.alerts.size());
    }

    @Test
    void triggerVariantsCsvCaseAndMissingTriggers() throws Exception {
        addRule("csv", "{\"trigger_events\":\" Purchase , PAYMENT_SUCCESS \",\"condition\":null}", 0);
        addRule("single", "{\"trigger_event\":\"refund\"}", 0);
        addRule("none", "{\"condition\":{\"field\":\"source_system\",\"value\":\"ecommerce\"}}", 0);
        user.send(event("u1", "payment_success", T0).build());
        user.send(event("u1", "refund", T0 + 1).build());
        user.send(event("u1", "login", T0 + 2).str("source_system", "ecommerce").build());
        assertEquals(2, user.alerts.size());
    }

    @Test
    void slidingSumAcrossBuckets() throws Exception {
        addRule("spend", "{\"trigger_events\":[\"purchase\"],\"condition\":{\"type\":\"AGGREGATION\",\"field\":\"total_amount\","
                + "\"function\":\"SUM\",\"window\":{\"type\":\"sliding\",\"size_seconds\":900,\"lookback_seconds\":900},"
                + "\"operator\":\"GTE\",\"value\":5000000}}", 0);
        user.send(event("u1", "purchase", T0 + 10).num("total_amount", 2_000_000).build());
        user.send(event("u1", "purchase", T0 + B + 10).num("total_amount", 1_500_000).build());
        assertEquals(0, user.alerts.size());
        user.send(event("u1", "purchase", T0 + 2 * B + 10).num("total_amount", 2_000_000).build());
        assertEquals(1, user.alerts.size());
        user.send(event("u1", "purchase", T0 + 5 * B).num("total_amount", 1).build());
        assertEquals(1, user.alerts.size(), "first buckets slid out of the 15 minute window");
    }

    @Test
    void complexTreeWithFilteredCount() throws Exception {
        addRule("fraud", "{\"trigger_events\":[\"payment_failed\"],\"condition\":{\"operator\":\"AND\",\"children\":["
                + "{\"type\":\"AGGREGATION\",\"field\":\"event_id\",\"function\":\"COUNT\",\"window\":{\"lookback_seconds\":3600},"
                + "\"filter\":{\"type\":\"RAW_FIELD\",\"field\":\"transaction_status\",\"operator\":\"EQ\",\"value\":\"failed\"},"
                + "\"operator\":\"GTE\",\"value\":3},"
                + "{\"operator\":\"OR\",\"children\":[{\"field\":\"payment_method\",\"value\":\"credit_card\"},"
                + "{\"field\":\"is_3ds_verified\",\"value\":false}]}]}}", 0);
        for (int i = 0; i < 3; i++) {
            user.send(event("u1", "payment_failed", T0 + i * 60_000L).str("transaction_status", "failed")
                    .str("payment_method", "wallet").bool("is_3ds_verified", true).build());
        }
        assertEquals(0, user.alerts.size(), "count reached but OR branch false");
        user.send(event("u1", "payment_failed", T0 + 4 * 60_000L).str("transaction_status", "failed")
                .str("payment_method", "credit_card").build());
        assertEquals(1, user.alerts.size());
        JsonNode alert = M.readTree(user.alerts.records.get(0).value);
        assertEquals(4.0, alert.get("evaluated_values").elements().next().asDouble(), 1e-9);
    }

    @Test
    void cooldownIsPerRuleAndEventTimeBased() throws Exception {
        addRule("cd", "{\"trigger_events\":[\"purchase\"],\"condition\":{\"field\":\"total_amount\",\"operator\":\"GT\",\"value\":10}}", 3600);
        user.send(event("u1", "purchase", T0).num("total_amount", 100).build());
        user.send(event("u1", "purchase", T0 + 600_000L).num("total_amount", 100).build());
        assertEquals(1, user.alerts.size());
        assertEquals(1, processor.stats().suppressedByCooldown);
        user.send(event("u1", "purchase", T0 + 3_600_000L).num("total_amount", 100).build());
        assertEquals(2, user.alerts.size());
        JsonNode a = M.readTree(user.alerts.records.get(1).value);
        assertEquals(T0 + 3_600_000L + 3_600_000L, a.get("audit").get("cooldown_until_ms").asLong());
    }

    @Test
    void shortCircuitSkipsStateReadsWhenRawConditionFails() throws Exception {
        addRule("sc", "{\"trigger_events\":[\"purchase\"],\"condition\":{\"operator\":\"AND\",\"children\":["
                + "{\"type\":\"AGGREGATION\",\"field\":\"total_amount\",\"function\":\"SUM\",\"window\":{\"lookback_seconds\":86400},\"operator\":\"GT\",\"value\":0},"
                + "{\"field\":\"source_system\",\"operator\":\"EQ\",\"value\":\"crm\"}]}}", 0);
        user.store.resetCounters();
        user.send(event("u1", "purchase", T0).str("source_system", "ecommerce").num("total_amount", 5).build());
        assertEquals(1, user.store.gets, "only the write path read the current slot");
        assertEquals(0, user.store.scans);
        user.store.resetCounters();
        user.send(event("u1", "purchase", T0 + 1).str("source_system", "crm").num("total_amount", 5).build());
        assertEquals(1, user.alerts.size());
        assertTrue(user.store.gets + user.store.scans > 1, "aggregate branch reads history");
    }

    @Test
    void lateEventsAreRejectedByWatermarkAndCircularOverwrite() throws Exception {
        addRule("any", sumRule("purchase", 300, "GTE", 0), 0);
        String reason = user.send(event("u1", "purchase", T0).num("total_amount", 1).build(), T0 + B + 60_000L);
        assertNotNull(reason);
        assertTrue(reason.startsWith("Late event rejected"));
        assertNull(user.send(event("u1", "purchase", T0).num("total_amount", 1).build(), T0 + B + 59_999L),
                "inside the 1 minute tolerance");
        assertTrue(user.send(event("u1", "purchase", T0 - TimeBuckets.MAX_WINDOW_MS - 1).build(), T0).startsWith("Late event"));

        user.send(event("u1", "purchase", T0 + TimeBuckets.MAX_WINDOW_MS).num("total_amount", 1).build());
        String overwrite = user.send(event("u1", "purchase", T0 + 5).num("total_amount", 1).build());
        assertTrue(overwrite.startsWith("Circular buffer overwrite"));
    }

    @Test
    void circularOverwriteAfter24hResetsSlot() throws Exception {
        addRule("sum", sumRule("purchase", 300, "GTE", 0), 0);
        user.send(event("u1", "purchase", T0).num("total_amount", 10).build());
        user.send(event("u1", "purchase", T0 + TimeBuckets.MAX_WINDOW_MS).num("total_amount", 3).build());
        CompactBucket b = user.store.map.get(TimeBuckets.slotOf(T0));
        assertEquals(T0 + TimeBuckets.MAX_WINDOW_MS, b.getStartMs());
        assertEquals(3.0, b.sumAt(0), 1e-9);
    }

    @Test
    void fiveFunctionsAndEmptyWindowSemantics() throws Exception {
        String agg = "{\"trigger_events\":[\"login\"],\"condition\":{\"type\":\"AGGREGATION\",\"field\":\"total_amount\","
                + "\"function\":\"%s\",\"window\":{\"lookback_seconds\":3600},\"operator\":\"%s\",\"value\":%s}}";
        addRule("min-empty", String.format(agg, "MIN", "LT", "100"), 0);
        addRule("sum-empty", String.format(agg, "SUM", "EQ", "0"), 0);
        user.send(event("u1", "login", T0).build());
        assertEquals(1, user.alerts.size(), "SUM of empty window is 0, MIN is undefined");
        assertEquals("sum-empty", M.readTree(user.alerts.records.get(0).value).get("rule_id").asText());

        addRule("purchases", sumRule("purchase", 300, "GTE", 1e18), 0);
        for (double v : new double[]{10, -4, 30}) {
            user.send(event("u1", "purchase", T0 + 1000).num("total_amount", v).build());
        }
        catalog.apply(rule("min-empty", 2, String.format(agg, "MIN", "EQ", "-4"), 0), 0L);
        catalog.apply(rule("sum-empty", 2, String.format(agg, "SUM", "EQ", "36"), 0), 0L);
        addRule("avg", String.format(agg, "AVG", "EQ", "12"), 0);
        addRule("max", String.format(agg, "MAX", "EQ", "30"), 0);
        addRule("count", String.format(agg, "COUNT", "EQ", "3"), 0);
        user.alerts.clear();
        user.send(event("u1", "login", T0 + 2000).build());
        assertEquals(5, user.alerts.size());
    }

    @Test
    void linearExpressionMixesAggregatesAndRawFields() throws Exception {
        addRule("lin", "{\"trigger_events\":[\"purchase\"],\"condition\":{\"type\":\"LINEAR\",\"terms\":["
                + "{\"coef\":1,\"ref\":{\"type\":\"AGGREGATION\",\"field\":\"total_amount\",\"function\":\"SUM\",\"window\":{\"lookback_seconds\":3600}}},"
                + "{\"coef\":-1000,\"ref\":{\"type\":\"AGGREGATION\",\"field\":\"event_id\",\"function\":\"COUNT\","
                + "\"filter\":{\"field\":\"event_type\",\"value\":\"refund\"},\"window\":{\"lookback_seconds\":3600}}},"
                + "{\"coef\":10,\"field\":\"loyalty_points\"}],\"constant\":0,\"operator\":\"GT\",\"value\":5000}}", 0);
        user.send(event("u1", "refund", T0).build());
        user.send(event("u1", "purchase", T0 + 1000).num("total_amount", 5500).num("loyalty_points", 40).build());
        assertEquals(0, user.alerts.size(), "5500 - 1000 + 400 = 4900");
        user.send(event("u1", "purchase", T0 + 2000).num("total_amount", 100).num("loyalty_points", 50).build());
        assertEquals(1, user.alerts.size(), "5600 - 1000 + 500 = 5100");
        user.send(event("u1", "purchase", T0 + 3000).num("total_amount", 100).build());
        assertEquals(1, user.alerts.size(), "missing raw term makes the expression undefined");
    }

    @Test
    void definitionsOnlyAggregateEventsFromTheirEffectiveTime() throws Exception {
        catalog.apply(rule("eff", 1, "{\"trigger_events\":[\"purchase\"],\"condition\":{\"type\":\"AGGREGATION\","
                + "\"field\":\"total_amount\",\"function\":\"SUM\",\"window\":{\"lookback_seconds\":3600},"
                + "\"operator\":\"GTE\",\"value\":100}}", 0, true, T0 + 2 * B), 0L);
        user.send(event("u1", "purchase", T0).num("total_amount", 500).build());
        assertEquals(0, user.alerts.size(), "event before the definition became effective is not aggregated");
        user.send(event("u1", "purchase", T0 + 2 * B).num("total_amount", 100).build());
        assertEquals(1, user.alerts.size());
        assertFalse(M.readTree(user.alerts.records.get(0).value).get("window_complete").asBoolean());

        catalog.apply(rule("full", 1, "{\"trigger_events\":[\"purchase\"],\"require_full_window\":true,\"condition\":"
                + "{\"type\":\"AGGREGATION\",\"field\":\"total_amount\",\"function\":\"COUNT\",\"window\":{\"lookback_seconds\":3600},"
                + "\"operator\":\"GTE\",\"value\":1}}", 0, true, T0 + 2 * B), 0L);
        user.alerts.clear();
        user.send(event("u1", "purchase", T0 + 3 * B).num("total_amount", 1).build());
        assertEquals(1, user.alerts.size(), "only the non-strict rule fires while the window is incomplete");
        user.alerts.clear();
        user.send(event("u1", "purchase", T0 + 2 * B + 3_600_000L).num("total_amount", 200).build());
        assertEquals(2, user.alerts.size(), "window now starts after the effective time");
    }

    @Test
    void retiredSpecsAreDroppedFromBucketsOnWriteAndSweep() throws Exception {
        addRule("a", sumRule("purchase", 3600, "GTE", 1e12), 0);
        addRule("b", "{\"trigger_events\":[\"purchase\"],\"condition\":{\"type\":\"AGGREGATION\",\"field\":\"quantity\","
                + "\"function\":\"MAX\",\"window\":{\"lookback_seconds\":3600},\"operator\":\"GT\",\"value\":1e12}}", 0);
        user.send(event("u1", "purchase", T0).num("total_amount", 1).num("quantity", 2).build());
        user.send(event("u1", "purchase", T0 + 2 * B).num("total_amount", 1).num("quantity", 2).build());
        assertEquals(2, user.store.map.get(TimeBuckets.slotOf(T0)).size());

        catalog.apply(TestSupport.deleteRule("b", 1), 0L);
        user.send(event("u1", "purchase", T0 + 3).num("total_amount", 1).num("quantity", 2).build());
        assertEquals(1, user.store.map.get(TimeBuckets.slotOf(T0)).size(), "layer 2 on write");
        assertEquals(2, user.store.map.get(TimeBuckets.slotOf(T0 + 2 * B)).size());
        processor.sweep(T0 + 3 * B, user.store);
        assertEquals(1, user.store.map.get(TimeBuckets.slotOf(T0 + 2 * B)).size(), "layer 2 on sweep");

        processor.sweep(T0 + TimeBuckets.MAX_WINDOW_MS + B + 1, user.store);
        assertNull(user.store.map.get(TimeBuckets.slotOf(T0)), "layer 3 drops buckets older than 24h");
        assertNotNull(user.store.map.get(TimeBuckets.slotOf(T0 + 2 * B)));
    }

    @Test
    void schemaMetricAliasesResolveEvenWhenSchemaArrivesLater() throws Exception {
        addRule("vip", "{\"rule_id\":\"vip\",\"trigger_events\":[\"purchase\"],\"condition_tree\":{\"type\":\"LOGIC\",\"operator\":\"AND\",\"children\":["
                + "{\"type\":\"LEAF\",\"field_type\":\"metric\",\"field_name\":\"total_spend_24h\",\"operator\":\">\",\"value\":1000},"
                + "{\"type\":\"LEAF\",\"field_type\":\"raw\",\"field_name\":\"product_category\",\"operator\":\"=\",\"value\":\"electronics\"}]}}", 0);
        assertTrue(catalog.pendingAliasRules().contains("vip"));
        user.send(event("u1", "purchase", T0).num("total_amount", 5000).str("product_category", "electronics").build());
        assertEquals(0, user.alerts.size());

        catalog.apply(TestSupport.schema("ecommerce_purchase", "1.0", "{\"version\":\"1.0\",\"metrics\":[{\"metric_id\":\"total_spend_24h\","
                + "\"source_field\":\"total_amount\",\"aggregation\":\"SUM\",\"filter\":{\"field\":\"event_type\",\"value\":\"purchase\"}}]}"), 0L);
        assertFalse(catalog.pendingAliasRules().contains("vip"));
        user.send(event("u1", "purchase", T0 + 1000).num("total_amount", 600).str("product_category", "electronics").build());
        assertEquals(0, user.alerts.size(), "600 aggregated since the alias became effective");
        user.send(event("u1", "purchase", T0 + 2000).num("total_amount", 600).str("product_category", "electronics").build());
        assertEquals(1, user.alerts.size());
    }

    @Test
    void alertIdIsDeterministicAndPayloadIsOptIn() throws Exception {
        addRule("r", "{\"trigger_events\":[\"purchase\"]}", 0);
        user.send(event("u1", "purchase", T0).raw("{\"x\":1}").build());
        JsonNode a = M.readTree(user.alerts.records.get(0).value);
        assertEquals(AlertBuilder.alertId("u1", "r", 1, "u1-purchase-" + T0), a.get("alert_id").asText());
        assertEquals("u1", user.alerts.records.get(0).key);
        assertFalse(a.get("trigger_event").has("payload"));
    }

    @Test
    void manyBucketReadsSwitchToSingleScan() throws Exception {
        addRule("day", sumRule("purchase", 86400, "GTE", 1e18), 0);
        for (int i = 0; i < 100; i++) {
            user.send(event("u1", "purchase", T0 + i * B).num("total_amount", 1).build());
        }
        user.store.resetCounters();
        user.send(event("u1", "purchase", T0 + 100 * B).num("total_amount", 1).build());
        assertEquals(1, user.store.scans);
        assertTrue(user.store.gets <= TestSupport.config().fullLoadThreshold + 1);
    }
}
