package flink.evaluators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import flink.models.Bucket;
import flink.models.RingBuffer;
import flink.models.RuleDefinition;
import org.apache.flink.api.common.state.MapState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RuleEvaluatorTest {

    private static final ObjectMapper mapper = new ObjectMapper();
    private MockMapState<Integer, Bucket> mockRingBufferState;

    @BeforeEach
    public void setUp() {
        mockRingBufferState = new MockMapState<>();
    }

    @Test
    public void testTriggerEventsFiltering() throws Exception {
        String ruleJson = "{\n" +
                "  \"trigger_events\": [\"purchase\", \"payment_success\"],\n" +
                "  \"condition\": {\n" +
                "    \"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"EQ\", \"value\": \"ecommerce\"\n" +
                "  }\n" +
                "}";

        JsonNode cdcNode = mapper.createObjectNode()
                .put("rule_id", "rule-001")
                .put("name", "Test Trigger Rule")
                .put("enabled", true)
                .put("rule_json", ruleJson);

        RuleDefinition rule = RuleDefinition.fromCdcJson(cdcNode, mapper);
        assertNotNull(rule);

        JsonNode matchingEvent = mapper.readTree("{\"event_type\": \"purchase\", \"source_system\": \"ecommerce\"}");
        assertTrue(RuleEvaluator.evaluateRule(rule, matchingEvent, 1700000000000L, mockRingBufferState));

        JsonNode nonMatchingEvent = mapper.readTree("{\"event_type\": \"login\", \"source_system\": \"ecommerce\"}");
        assertFalse(RuleEvaluator.evaluateRule(rule, nonMatchingEvent, 1700000000000L, mockRingBufferState));
    }

    @Test
    public void testAggregationWindowEvaluation() throws Exception {
        long baseTime = 1700000000000L;
        long b0 = RingBuffer.normalizeToBucketStartTime(baseTime);
        long b1 = b0 + RingBuffer.BUCKET_DURATION_MS;
        long b2 = b1 + RingBuffer.BUCKET_DURATION_MS;

        Bucket bucket0 = new Bucket(b0);
        bucket0.updateMetric("sum_total_amount", "SUM", 2000000.0);
        mockRingBufferState.put(RingBuffer.getSlotIndex(b0), bucket0);

        Bucket bucket1 = new Bucket(b1);
        bucket1.updateMetric("sum_total_amount", "SUM", 1500000.0);
        mockRingBufferState.put(RingBuffer.getSlotIndex(b1), bucket1);

        Bucket bucket2 = new Bucket(b2);
        bucket2.updateMetric("sum_total_amount", "SUM", 2000000.0);
        mockRingBufferState.put(RingBuffer.getSlotIndex(b2), bucket2);

        String ruleJson = "{\n" +
                "  \"trigger_events\": [\"purchase\"],\n" +
                "  \"condition\": {\n" +
                "    \"type\": \"AGGREGATION\",\n" +
                "    \"field\": \"total_amount\",\n" +
                "    \"function\": \"SUM\",\n" +
                "    \"window\": {\"type\": \"sliding\", \"size_seconds\": 900, \"lookback_seconds\": 900},\n" +
                "    \"operator\": \"GTE\",\n" +
                "    \"value\": 5000000\n" +
                "  }\n" +
                "}";

        JsonNode cdcNode = mapper.createObjectNode()
                .put("rule_id", "high-spend-15m")
                .put("name", "High Spender 15m")
                .put("enabled", true)
                .put("rule_json", ruleJson);

        RuleDefinition rule = RuleDefinition.fromCdcJson(cdcNode, mapper);

        JsonNode eventAtB2 = mapper.readTree("{\"event_type\": \"purchase\", \"total_amount\": 2000000.0}");
        assertTrue(RuleEvaluator.evaluateRule(rule, eventAtB2, b2 + 1000L, mockRingBufferState));

        JsonNode eventAtB1 = mapper.readTree("{\"event_type\": \"purchase\", \"total_amount\": 1500000.0}");
        assertFalse(RuleEvaluator.evaluateRule(rule, eventAtB1, b1 + 1000L, mockRingBufferState));
    }

    @Test
    public void testComplexLogicalTreeEvaluation() throws Exception {
        long t = RingBuffer.normalizeToBucketStartTime(1700000000000L);
        Bucket bucket = new Bucket(t);
        bucket.updateMetric("count_events_where_transaction_status_failed", "COUNT", 1.0);
        bucket.updateMetric("count_events_where_transaction_status_failed", "COUNT", 1.0);
        bucket.updateMetric("count_events_where_transaction_status_failed", "COUNT", 1.0);
        mockRingBufferState.put(RingBuffer.getSlotIndex(t), bucket);

        String ruleJson = "{\n" +
                "  \"trigger_events\": [\"payment_failed\"],\n" +
                "  \"condition\": {\n" +
                "    \"operator\": \"AND\",\n" +
                "    \"children\": [\n" +
                "      {\n" +
                "        \"type\": \"AGGREGATION\",\n" +
                "        \"field\": \"event_id\",\n" +
                "        \"function\": \"COUNT\",\n" +
                "        \"window\": {\"lookback_seconds\": 3600},\n" +
                "        \"filter\": {\"type\": \"RAW_FIELD\", \"field\": \"transaction_status\", \"operator\": \"EQ\", \"value\": \"failed\"},\n" +
                "        \"operator\": \"GTE\",\n" +
                "        \"value\": 3\n" +
                "      },\n" +
                "      {\n" +
                "        \"type\": \"RAW_FIELD\",\n" +
                "        \"field\": \"source_system\",\n" +
                "        \"operator\": \"EQ\",\n" +
                "        \"value\": \"payment\"\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        JsonNode cdcNode = mapper.createObjectNode()
                .put("rule_id", "fraud-failed-tx")
                .put("name", "Repeated Failed Payments")
                .put("enabled", true)
                .put("rule_json", ruleJson);

        RuleDefinition rule = RuleDefinition.fromCdcJson(cdcNode, mapper);

        JsonNode matchingEvent = mapper.readTree("{\"event_type\": \"payment_failed\", \"source_system\": \"payment\"}");
        assertTrue(RuleEvaluator.evaluateRule(rule, matchingEvent, t + 1000L, mockRingBufferState));

        JsonNode nonMatchingEvent = mapper.readTree("{\"event_type\": \"payment_failed\", \"source_system\": \"crm\"}");
        assertFalse(RuleEvaluator.evaluateRule(rule, nonMatchingEvent, t + 1000L, mockRingBufferState));
    }

    @Test
    public void testCooldownSecondsEvaluation() throws Exception {
        JsonNode cdcNode = mapper.createObjectNode()
                .put("rule_id", "rule-cooldown-01")
                .put("name", "Cooldown Test Rule")
                .put("cooldown_seconds", 300L)
                .put("enabled", true)
                .put("rule_json", "{\"trigger_events\": [\"purchase\"], \"condition\": {\"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"EQ\", \"value\": \"ecommerce\"}}");

        RuleDefinition rule = RuleDefinition.fromCdcJson(cdcNode, mapper);
        assertEquals(300L, rule.getCooldownSeconds());

        MockMapState<String, Long> cooldownState = new MockMapState<>();
        long baseTime = 1700000000000L;
        long cooldownMs = rule.getCooldownSeconds() * 1000L;

        Long lastEmit = cooldownState.get(rule.getRuleId());
        boolean inCooldown = (lastEmit != null && baseTime < lastEmit + cooldownMs);
        assertFalse(inCooldown);
        cooldownState.put(rule.getRuleId(), baseTime);

        long event2Time = baseTime + 100_000L;
        lastEmit = cooldownState.get(rule.getRuleId());
        inCooldown = (lastEmit != null && event2Time < lastEmit + cooldownMs);
        assertTrue(inCooldown);

        long event3Time = baseTime + 301_000L;
        lastEmit = cooldownState.get(rule.getRuleId());
        inCooldown = (lastEmit != null && event3Time < lastEmit + cooldownMs);
        assertFalse(inCooldown);
        cooldownState.put(rule.getRuleId(), event3Time);
        assertEquals(event3Time, cooldownState.get(rule.getRuleId()));
    }

    @Test
    public void testTriggerEventVariationsAndExclusions() throws Exception {
        JsonNode cdcSingular = mapper.createObjectNode()
                .put("rule_id", "rule-singular")
                .put("rule_json", "{\"trigger_event\": \"purchase\", \"condition\": {\"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"EQ\", \"value\": \"ecommerce\"}}");
        RuleDefinition ruleSingular = RuleDefinition.fromCdcJson(cdcSingular, mapper);
        assertNotNull(ruleSingular);
        assertTrue(ruleSingular.matchesTriggerEvent("purchase"));
        assertTrue(ruleSingular.matchesTriggerEvent("PURCHASE"));
        assertFalse(ruleSingular.matchesTriggerEvent("order_created"));
        assertFalse(ruleSingular.matchesTriggerEvent(null));

        JsonNode cdcArraySingular = mapper.createObjectNode()
                .put("rule_id", "rule-arr-singular")
                .put("rule_json", "{\"trigger_event\": [\"refund\", \"chargeback\"], \"condition\": {}}");
        RuleDefinition ruleArrSingular = RuleDefinition.fromCdcJson(cdcArraySingular, mapper);
        assertTrue(ruleArrSingular.matchesTriggerEvent("refund"));
        assertTrue(ruleArrSingular.matchesTriggerEvent("chargeback"));
        assertFalse(ruleArrSingular.matchesTriggerEvent("payment_success"));

        JsonNode cdcCsv = mapper.createObjectNode()
                .put("rule_id", "rule-csv")
                .put("rule_json", "{\"trigger_events\": \"payment_success, payment_failed\", \"condition\": {}}");
        RuleDefinition ruleCsv = RuleDefinition.fromCdcJson(cdcCsv, mapper);
        assertTrue(ruleCsv.matchesTriggerEvent("payment_success"));
        assertTrue(ruleCsv.matchesTriggerEvent("payment_failed"));
        assertFalse(ruleCsv.matchesTriggerEvent("login"));

        JsonNode cdcRoot = mapper.createObjectNode()
                .put("rule_id", "rule-root")
                .put("trigger_events", "login, logout")
                .put("rule_json", "{\"condition\": {}}");
        RuleDefinition ruleRoot = RuleDefinition.fromCdcJson(cdcRoot, mapper);
        assertTrue(ruleRoot.matchesTriggerEvent("login"));
        assertTrue(ruleRoot.matchesTriggerEvent("logout"));
        assertFalse(ruleRoot.matchesTriggerEvent("purchase"));

        JsonNode cdcEmpty = mapper.createObjectNode()
                .put("rule_id", "rule-empty")
                .put("rule_json", "{\"condition\": {}}");
        RuleDefinition ruleEmpty = RuleDefinition.fromCdcJson(cdcEmpty, mapper);
        assertFalse(ruleEmpty.matchesTriggerEvent("purchase"));
        assertFalse(ruleEmpty.matchesTriggerEvent("login"));
        assertFalse(RuleEvaluator.evaluateRule(ruleEmpty, mapper.readTree("{\"event_type\": \"purchase\"}"), 1700000000000L, mockRingBufferState));
    }

    @Test
    public void testShortCircuitingInConditionTree() throws Exception {
        class CountingMapState extends MockMapState<Integer, Bucket> {
            int getCount = 0;
            @Override
            public Bucket get(Integer key) {
                getCount++;
                return super.get(key);
            }
        }

        CountingMapState state = new CountingMapState();

        String ruleJsonAnd = "{\n" +
                "  \"trigger_events\": [\"purchase\"],\n" +
                "  \"condition\": {\n" +
                "    \"operator\": \"AND\",\n" +
                "    \"children\": [\n" +
                "      {\"type\": \"AGGREGATION\", \"field\": \"total_amount\", \"function\": \"SUM\", \"operator\": \"GT\", \"value\": 100},\n" +
                "      {\"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"EQ\", \"value\": \"payment\"}\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        RuleDefinition ruleAnd = RuleDefinition.fromCdcJson(mapper.createObjectNode()
                .put("rule_id", "rule-and-sc")
                .put("rule_json", ruleJsonAnd), mapper);

        JsonNode eventEcommerce = mapper.readTree("{\"event_type\": \"purchase\", \"source_system\": \"ecommerce\"}");
        boolean resultAnd = RuleEvaluator.evaluateRule(ruleAnd, eventEcommerce, 1700000000000L, state);
        assertFalse(resultAnd);
        assertEquals(0, state.getCount);

        String ruleJsonOr = "{\n" +
                "  \"trigger_events\": [\"purchase\"],\n" +
                "  \"condition\": {\n" +
                "    \"operator\": \"OR\",\n" +
                "    \"children\": [\n" +
                "      {\"type\": \"RAW_FIELD\", \"field\": \"source_system\", \"operator\": \"EQ\", \"value\": \"ecommerce\"},\n" +
                "      {\"type\": \"AGGREGATION\", \"field\": \"total_amount\", \"function\": \"SUM\", \"operator\": \"GT\", \"value\": 100}\n" +
                "    ]\n" +
                "  }\n" +
                "}";

        RuleDefinition ruleOr = RuleDefinition.fromCdcJson(mapper.createObjectNode()
                .put("rule_id", "rule-or-sc")
                .put("rule_json", ruleJsonOr), mapper);

        boolean resultOr = RuleEvaluator.evaluateRule(ruleOr, eventEcommerce, 1700000000000L, state);
        assertTrue(resultOr);
        assertEquals(0, state.getCount);
    }

    private static class MockMapState<K, V> implements MapState<K, V> {
        private final Map<K, V> map = new HashMap<>();

        @Override
        public V get(K key) {
            return map.get(key);
        }

        @Override
        public void put(K key, V value) {
            map.put(key, value);
        }

        @Override
        public void putAll(Map<K, V> map) {
            this.map.putAll(map);
        }

        @Override
        public void remove(K key) {
            map.remove(key);
        }

        @Override
        public boolean contains(K key) {
            return map.containsKey(key);
        }

        @Override
        public Iterable<Map.Entry<K, V>> entries() {
            return map.entrySet();
        }

        @Override
        public Iterable<K> keys() {
            return map.keySet();
        }

        @Override
        public Iterable<V> values() {
            return map.values();
        }

        @Override
        public Iterator<Map.Entry<K, V>> iterator() {
            return map.entrySet().iterator();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public void clear() {
            map.clear();
        }
    }
}

