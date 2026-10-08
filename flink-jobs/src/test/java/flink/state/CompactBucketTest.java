package flink.state;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompactBucketTest {

    @Test
    void accumulatesSumCountMinMaxPerKey() {
        CompactBucket b = new CompactBucket(TimeBuckets.bucketStart(1_700_000_123_456L));
        b.add(42L, 10.0);
        b.add(42L, -3.5);
        b.add(7L, 100.0);
        b.add(42L, 20.0);
        assertEquals(2, b.size());
        int i = b.indexOf(42L);
        assertEquals(26.5, b.sumAt(i), 1e-9);
        assertEquals(3, b.countAt(i));
        assertEquals(-3.5, b.minAt(i), 1e-9);
        assertEquals(20.0, b.maxAt(i), 1e-9);
        assertTrue(b.indexOf(7L) < i, "keys must stay sorted");
    }

    @Test
    void growsBeyondInitialCapacityAndKeepsOrder() {
        CompactBucket b = new CompactBucket(0L);
        for (long k = 100; k > 0; k--) {
            b.add(k * 31 - 1500, k);
        }
        assertEquals(100, b.size());
        for (int i = 1; i < b.size(); i++) {
            assertTrue(b.keyAt(i - 1) < b.keyAt(i));
        }
    }

    @Test
    void removeIfDropsRetiredKeys() {
        CompactBucket b = new CompactBucket(0L);
        b.add(1L, 1.0);
        b.add(2L, 2.0);
        b.add(3L, 3.0);
        assertTrue(b.removeIf(k -> k == 2L));
        assertEquals(2, b.size());
        assertTrue(b.indexOf(2L) < 0);
        assertEquals(3.0, b.sumAt(b.indexOf(3L)), 1e-9);
        assertFalse(b.removeIf(k -> k == 99L));
    }

    @Test
    void serializerRoundTripAndCopyAreLossless() throws Exception {
        CompactBucket b = new CompactBucket(TimeBuckets.BUCKET_MS * 1234);
        b.add(-5L, 1e15);
        b.add(9L, -2.25);
        b.add(9L, 4.0);
        DataOutputSerializer out = new DataOutputSerializer(1024);
        CompactBucketSerializer.INSTANCE.serialize(b, out);
        CompactBucket back = CompactBucketSerializer.INSTANCE.deserialize(new DataInputDeserializer(out.getCopyOfBuffer()));
        assertEquals(b, back);
        assertEquals(b, CompactBucketSerializer.INSTANCE.copy(b));

        DataOutputSerializer copied = new DataOutputSerializer(1024);
        CompactBucketSerializer.INSTANCE.copy(new DataInputDeserializer(out.getCopyOfBuffer()), copied);
        assertEquals(b, CompactBucketSerializer.INSTANCE.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer())));
    }

    @Test
    void timeBucketMathHandlesBoundariesAndNegativeTimes() {
        long start = TimeBuckets.BUCKET_MS * 1000;
        assertEquals(start, TimeBuckets.bucketStart(start));
        assertEquals(start, TimeBuckets.bucketStart(start + TimeBuckets.BUCKET_MS - 1));
        assertEquals(start + TimeBuckets.BUCKET_MS, TimeBuckets.bucketStart(start + TimeBuckets.BUCKET_MS));
        assertEquals(TimeBuckets.slotOf(start), TimeBuckets.slotOf(start + TimeBuckets.MAX_WINDOW_MS));
        assertEquals(287, TimeBuckets.slotOf(-1L));
        assertEquals(-TimeBuckets.BUCKET_MS, TimeBuckets.bucketStart(-1L));
        assertEquals(start, TimeBuckets.roundUpToBucket(start));
        assertEquals(start + TimeBuckets.BUCKET_MS, TimeBuckets.roundUpToBucket(start + 1));
    }
}
