package generator.events;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventTimeModelTest {

    @Test
    void normalEventsStayWithinOutOfOrderBoundAndLateOnesBeyondTolerance() {
        EventConfig c = EventConfig.parse(new String[]{"--late-event-rate", "0.1", "--max-out-of-order-ms", "20000",
                "--late-min-ms", "420000", "--late-max-ms", "1800000"});
        Instant now = Instant.parse("2026-10-07T10:00:00Z");
        Random r = new Random(1);
        int late = 0;
        for (int i = 0; i < 20_000; i++) {
            long delay = now.toEpochMilli() - EventFactory.eventTime(now, r, c).toEpochMilli();
            assertTrue(delay >= 0);
            if (delay > 20_000) {
                late++;
                assertTrue(delay >= 420_000 && delay <= 1_800_000, "late delay " + delay);
            }
        }
        assertEquals(0.1, late / 20_000.0, 0.01);
    }

    @Test
    void zipfConcentratesTrafficOnTopRanks() {
        EntityPool pool = new EntityPool(10_000, 0.0, 1.2);
        Random r = new Random(3);
        Map<String, Integer> counts = new HashMap<>();
        for (int i = 0; i < 50_000; i++) {
            counts.merge(pool.next(r), 1, Integer::sum);
        }
        int top = counts.getOrDefault("customer-0", 0);
        assertTrue(top > 50_000 * 0.15, "rank-1 share " + top);
        assertTrue(counts.size() > 1000);
    }
}
