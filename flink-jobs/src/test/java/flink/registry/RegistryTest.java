package flink.registry;

import com.fasterxml.jackson.databind.ObjectMapper;
import flink.model.ValidatedEvent;
import flink.state.CompactBucket;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistryTest {

    private static final ObjectMapper M = new ObjectMapper();

    private static SpecDefinition spec(String field, String filter) throws Exception {
        return CanonicalSpec.define(field, filter == null ? null : M.readTree(filter));
    }

    @Test
    void logicallyEqualFiltersShareOneSpecKey() throws Exception {
        SpecDefinition a = spec("total_amount", "{\"operator\":\"AND\",\"children\":["
                + "{\"field\":\"event_type\",\"operator\":\"EQ\",\"value\":\"purchase\"},"
                + "{\"field\":\"product_category\",\"operator\":\"IN\",\"value\":[\"books\",\"food\"]}]}");
        SpecDefinition b = spec(" total_amount", "{\"operator\":\"AND\",\"children\":["
                + "{\"operator\":\"AND\",\"children\":[{\"field\":\"product_category\",\"operator\":\"IN\",\"value\":[\"food\",\"books\"]}]},"
                + "{\"type\":\"RAW_FIELD\",\"field\":\"event_type\",\"operator\":\"=\",\"value\":\"purchase\"}]}");
        assertEquals(a.canonical(), b.canonical());
        assertEquals(a.specKey(), b.specKey());
        SpecDefinition c = spec("total_amount", "{\"field\":\"event_type\",\"operator\":\"EQ\",\"value\":\"refund\"}");
        assertNotEquals(a.specKey(), c.specKey());
    }

    @Test
    void countOfEventIdIsTheAllEventsSourceAndNumbersAreCanonical() throws Exception {
        assertEquals(spec("event_id", null).specKey(), spec("*", null).specKey());
        assertEquals(spec("events", "{\"field\":\"quantity\",\"value\":5}").specKey(),
                spec("event_id", "{\"field\":\"quantity\",\"operator\":\"EQ\",\"value\":5.0}").specKey());
        assertEquals(spec("x", "{\"operator\":\"NOT\",\"children\":[{\"operator\":\"NOT\",\"children\":[{\"field\":\"a\",\"value\":1}]}]}").specKey(),
                spec("x", "{\"field\":\"a\",\"value\":1}").specKey());
    }

    @Test
    void requiredEventTypesRouteSpecs() throws Exception {
        assertEquals(Set.of("purchase"), spec("x", "{\"operator\":\"AND\",\"children\":["
                + "{\"field\":\"event_type\",\"value\":\"Purchase\"},{\"field\":\"a\",\"value\":1}]}").eventTypes());
        assertEquals(Set.of("refund", "chargeback"), spec("x", "{\"operator\":\"OR\",\"children\":["
                + "{\"field\":\"event_type\",\"value\":\"refund\"},{\"field\":\"event_type\",\"value\":\"chargeback\"}]}").eventTypes());
        assertNull(spec("x", "{\"field\":\"source_system\",\"value\":\"crm\"}").eventTypes());
        assertNull(spec("x", "{\"operator\":\"OR\",\"children\":["
                + "{\"field\":\"event_type\",\"value\":\"refund\"},{\"field\":\"a\",\"value\":1}]}").eventTypes());
    }

    @Test
    void refCountingRetiresAndReacquireStartsNewEpoch() throws Exception {
        MetricRegistry r = new MetricRegistry();
        SpecDefinition s = spec("total_amount", null);
        MetricRegistry.Entry e1 = r.acquire(s, 0L);
        r.acquire(s, 0L);
        long firstKey = e1.storageKey();
        assertEquals(s.specKey(), firstKey);
        assertEquals(2, e1.refCount());
        r.release(s.specKey(), 10L);
        assertTrue(r.entry(s.specKey()).active());
        assertFalse(r.isRetired(firstKey));
        r.release(s.specKey(), 20L);
        assertFalse(r.entry(s.specKey()).active());
        assertTrue(r.isRetired(firstKey));

        MetricRegistry.Entry e2 = r.acquire(s, 500L);
        assertEquals(1, e2.epoch());
        assertNotEquals(firstKey, e2.storageKey());
        assertEquals(500L, e2.effectiveFromMs());
        assertTrue(r.isRetired(firstKey));
        assertEquals(1, r.pruneRetired(20L + 1000L, 999L));
        assertFalse(r.hasRetired());
    }

    @Test
    void accumulateHonoursFilterEffectiveTimeAndMissingValues() throws Exception {
        MetricRegistry r = new MetricRegistry();
        SpecDefinition sum = spec("total_amount", "{\"field\":\"event_type\",\"value\":\"purchase\"}");
        SpecDefinition count = spec("event_id", null);
        r.acquire(sum, 1000L);
        r.acquire(count, 0L);
        CompactBucket b = new CompactBucket(0L);
        ValidatedEvent early = ValidatedEvent.builder().str("event_type", "purchase").time(999L).num("total_amount", 5).build();
        ValidatedEvent ok = ValidatedEvent.builder().str("event_type", "purchase").time(1000L).num("total_amount", 7).build();
        ValidatedEvent noAmount = ValidatedEvent.builder().str("event_type", "purchase").time(2000L).build();
        ValidatedEvent other = ValidatedEvent.builder().str("event_type", "login").time(3000L).num("total_amount", 100).build();
        assertEquals(1, r.accumulate(early, b));
        assertEquals(2, r.accumulate(ok, b));
        assertEquals(1, r.accumulate(noAmount, b));
        assertEquals(1, r.accumulate(other, b));
        int i = b.indexOf(sum.specKey());
        assertEquals(7.0, b.sumAt(i), 1e-9);
        assertEquals(1, b.countAt(i));
        assertEquals(4, b.countAt(b.indexOf(count.specKey())));
    }

    @Test
    void seededMetadataRestoresEpochAndEffectiveTime() throws Exception {
        MetricRegistry original = new MetricRegistry();
        SpecDefinition s = spec("quantity", null);
        original.acquire(s, 0L);
        original.release(s.specKey(), 1L);
        original.acquire(s, 777L);
        String meta = original.encodeMeta(s.specKey());
        String retired = original.encodeRetired();

        MetricRegistry restored = new MetricRegistry();
        restored.seed(s.specKey(), meta);
        restored.seedRetired(retired);
        MetricRegistry.Entry e = restored.acquire(s, 99_999L);
        assertEquals(original.entry(s.specKey()).storageKey(), e.storageKey());
        assertEquals(777L, e.effectiveFromMs());
        assertTrue(restored.isRetired(MetricRegistry.storageKey(s.specKey(), 0)));
    }
}
