package flink.index;

import flink.engine.EngineConfig;
import flink.engine.EntityProcessor;
import flink.engine.RuleCatalog;
import flink.model.KeyedRecord;
import flink.model.ValidatedEvent;
import flink.testutil.TestSupport;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The index must only remove candidates: for random rules and events the set of alerts produced with
 * TRIGGER and DF2 indexing must be identical to a full scan, while evaluating fewer rules.
 */
class InvertedIndexPropertyTest {

    private static final String[] TYPES = {"purchase", "add_to_cart", "login", "refund", "payment_failed"};
    private static final String[] FIELDS = {"product_category", "payment_method", "order_status", "subscription_tier"};
    private static final String[][] VALUES = {
            {"electronics", "fashion", "food", "books"},
            {"credit_card", "wallet", "bank_transfer"},
            {"created", "paid", "shipped", "completed", "cancelled"},
            {"free", "basic", "premium"}
    };

    private static String leaf(Random r) {
        double p = r.nextDouble();
        int f = r.nextInt(FIELDS.length);
        if (p < 0.45) {
            return "{\"field\":\"" + FIELDS[f] + "\",\"operator\":\"EQ\",\"value\":\"" + VALUES[f][r.nextInt(VALUES[f].length)] + "\"}";
        }
        if (p < 0.65) {
            return "{\"field\":\"" + FIELDS[f] + "\",\"operator\":\"IN\",\"value\":[\"" + VALUES[f][0] + "\",\"" + VALUES[f][1] + "\"]}";
        }
        if (p < 0.75) {
            return "{\"field\":\"" + FIELDS[f] + "\",\"operator\":\"NEQ\",\"value\":\"" + VALUES[f][0] + "\"}";
        }
        if (p < 0.85) {
            return "{\"field\":\"total_amount\",\"operator\":\"GT\",\"value\":" + r.nextInt(1000) + "}";
        }
        return "{\"type\":\"AGGREGATION\",\"field\":\"event_id\",\"function\":\"COUNT\",\"window\":{\"lookback_seconds\":3600},"
                + "\"filter\":{\"field\":\"event_type\",\"value\":\"" + TYPES[r.nextInt(TYPES.length)] + "\"},"
                + "\"operator\":\"GTE\",\"value\":" + (1 + r.nextInt(3)) + "}";
    }

    private static String node(Random r, int depth) {
        if (depth == 0 || r.nextDouble() < 0.35) {
            return leaf(r);
        }
        double p = r.nextDouble();
        if (p < 0.1) {
            return "{\"operator\":\"NOT\",\"children\":[" + node(r, depth - 1) + "]}";
        }
        String op = p < 0.65 ? "AND" : "OR";
        int n = 2 + r.nextInt(2);
        List<String> c = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            c.add(node(r, depth - 1));
        }
        return "{\"operator\":\"" + op + "\",\"children\":[" + String.join(",", c) + "]}";
    }

    private static String ruleJson(Random r) {
        TreeSet<String> triggers = new TreeSet<>();
        int k = 1 + r.nextInt(2);
        while (triggers.size() < k) {
            triggers.add(TYPES[r.nextInt(TYPES.length)]);
        }
        return "{\"trigger_events\":[\"" + String.join("\",\"", triggers) + "\"],\"condition\":" + node(r, 3) + "}";
    }

    private static ValidatedEvent event(Random r, int i) {
        ValidatedEvent.Builder b = TestSupport.event("u" + r.nextInt(20), TYPES[r.nextInt(TYPES.length)],
                TestSupport.T0 + i * 7_000L);
        b.eventId("e" + i);
        for (int f = 0; f < FIELDS.length; f++) {
            if (r.nextDouble() < 0.8) {
                b.str(FIELDS[f], VALUES[f][r.nextInt(VALUES[f].length)]);
            }
        }
        b.num("total_amount", r.nextInt(1500));
        return b.build();
    }

    private static final class Run {
        final RuleCatalog catalog;
        final EntityProcessor processor;
        final java.util.Map<String, TestSupport.Entity> entities = new java.util.HashMap<>();
        final TreeSet<String> alerts = new TreeSet<>();

        Run(InvertedIndex.Mode mode) {
            EngineConfig cfg = TestSupport.config();
            cfg.indexMode = mode;
            catalog = new RuleCatalog(cfg);
            processor = new EntityProcessor(catalog, cfg);
        }

        void send(ValidatedEvent ev) throws Exception {
            TestSupport.Entity e = entities.computeIfAbsent(ev.entityId, k -> new TestSupport.Entity(processor));
            e.send(ev);
            for (KeyedRecord a : e.alerts.records) {
                alerts.add(a.value.substring(0, a.value.indexOf("\"triggered_at\"")));
            }
            e.alerts.clear();
        }
    }

    @Test
    void indexedModesProduceExactlyTheFullScanAlerts() throws Exception {
        Random rr = new Random(42);
        Run full = new Run(InvertedIndex.Mode.FULL);
        Run trigger = new Run(InvertedIndex.Mode.TRIGGER);
        Run df2 = new Run(InvertedIndex.Mode.DF2);
        for (int i = 0; i < 1500; i++) {
            String json = ruleJson(rr);
            for (Run run : new Run[]{full, trigger, df2}) {
                assertEquals(RuleCatalog.Status.APPLIED, run.catalog.apply(TestSupport.rule("r" + i, 1, json, 0), 0L).status);
            }
        }
        for (int i = 0; i < 200; i++) {
            for (Run run : new Run[]{full, trigger, df2}) {
                run.catalog.apply(TestSupport.deleteRule("r" + (i * 7), 1), 0L);
            }
        }
        Random er = new Random(7);
        for (int i = 0; i < 3000; i++) {
            ValidatedEvent ev = event(er, i);
            full.send(ev);
            trigger.send(ev);
            df2.send(ev);
        }
        assertTrue(full.alerts.size() > 50, "test data must produce matches");
        assertEquals(full.alerts, trigger.alerts);
        assertEquals(full.alerts, df2.alerts);
        long fullCandidates = full.processor.stats().candidatesAfterIndex;
        long triggerCandidates = trigger.processor.stats().candidatesAfterIndex;
        long dfCandidates = df2.processor.stats().candidatesAfterIndex;
        assertTrue(triggerCandidates < fullCandidates);
        assertTrue(dfCandidates < triggerCandidates, "df2=" + dfCandidates + " trigger=" + triggerCandidates);
        assertTrue(df2.catalog.index().rebuilds() > 0, "bulk load triggers a rebuild");
    }
}
