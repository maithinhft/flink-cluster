package generator.verify;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchSemanticsTest {

    private static final long T0 = 1_790_000_100_000L - (1_790_000_100_000L % BatchSemantics.B);
    private static final String SCHEMA = "{\"version\":\"1.0\",\"fields\":{\"entity_id\":{\"type\":\"string\",\"required\":true},"
            + "\"total_amount\":{\"type\":\"double\"}}}";

    private static String ev(String id, String entity, long t, double amount) {
        return "{\"event_id\":\"" + id + "\",\"entity_id\":\"" + entity + "\",\"event_type\":\"purchase\","
                + "\"source_system\":\"ecommerce\",\"schema_version\":\"1.0\",\"event_time\":\"" + Instant.ofEpochMilli(t)
                + "\",\"total_amount\":" + amount + "}";
    }

    private static BatchSemantics.Result run(List<BatchSemantics.InEvent> in, String rule, long cooldown, boolean watermark)
            throws Exception {
        BatchSemantics.Options o = new BatchSemantics.Options();
        o.simulateWatermark = watermark;
        o.eventTimeEffective = false;
        return new BatchSemantics(o).run(in, Map.of("ecommerce_purchase", SCHEMA),
                List.of(new BatchSemantics.RuleRow("r", 1, "r", rule, cooldown, 0)));
    }

    @Test
    void watermarkSimulationRejectsEventsArrivingAfterBucketClose() throws Exception {
        List<BatchSemantics.InEvent> in = new ArrayList<>();
        in.add(new BatchSemantics.InEvent("t", 0, 0, T0, ev("a", "u", T0, 1)));
        in.add(new BatchSemantics.InEvent("t", 0, 1, T0 + 10 * 60_000L, ev("b", "u", T0 + 10 * 60_000L, 1)));
        in.add(new BatchSemantics.InEvent("t", 0, 2, T0 + 10 * 60_000L + 1, ev("late", "u", T0 + 1000, 1)));
        in.add(new BatchSemantics.InEvent("t", 0, 3, T0 + 10 * 60_000L + 2, ev("ok", "u", T0 + 9 * 60_000L, 1)));
        BatchSemantics.Result r = run(in, "{\"trigger_events\":[\"purchase\"]}", 0, true);
        assertTrue(r.events.get("late").late);
        assertFalse(r.events.get("ok").late);
        assertEquals(1, r.late);
        assertEquals(3, r.alerts.size());
    }

    @Test
    void idlePartitionsDoNotHoldBackTheWatermark() throws Exception {
        List<BatchSemantics.InEvent> in = new ArrayList<>();
        in.add(new BatchSemantics.InEvent("slow", 0, 0, T0, ev("s", "u", T0, 1)));
        in.add(new BatchSemantics.InEvent("fast", 0, 0, T0 + 30 * 60_000L, ev("f", "u", T0 + 30 * 60_000L, 1)));
        in.add(new BatchSemantics.InEvent("fast", 0, 1, T0 + 30 * 60_000L + 1, ev("late", "u", T0 + 60_000L, 1)));
        assertTrue(run(in, "{\"trigger_events\":[\"purchase\"]}", 0, true).events.get("late").late,
                "the slow topic is idle for 30 minutes, so the fast topic drives the watermark");
    }

    @Test
    void cooldownAndAggregatesFollowEventTime() throws Exception {
        List<BatchSemantics.InEvent> in = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            long t = T0 + i * 20 * 60_000L;
            in.add(new BatchSemantics.InEvent("t", 0, i, t, ev("e" + i, "u", t, 100)));
        }
        String rule = "{\"trigger_events\":[\"purchase\"],\"condition\":{\"type\":\"AGGREGATION\",\"field\":\"total_amount\","
                + "\"function\":\"SUM\",\"window\":{\"lookback_seconds\":3600},\"operator\":\"GTE\",\"value\":200}}";
        BatchSemantics.Result r = run(in, rule, 1800, false);
        assertEquals(List.of("e1", "e3"), r.alerts.stream().map(BatchSemantics.AlertKey::eventId).toList(),
                "e0 below threshold, e2 inside the 30 minute cooldown of e1");
    }
}
