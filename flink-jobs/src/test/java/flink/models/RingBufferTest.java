package flink.models;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class RingBufferTest {

    private RingBuffer ringBuffer;

    @BeforeEach
    public void setUp() {
        ringBuffer = new RingBuffer();
    }

    @Test
    public void testTimestampNormalization() {
        // 5 minutes = 300,000 ms
        long t1 = 1700000000000L; // divisible by 300,000? 1700000000000 % 300000 = 200000
        long normalized = RingBuffer.normalizeToBucketStartTime(t1);
        assertEquals(0, normalized % RingBuffer.BUCKET_DURATION_MS);
        assertTrue(t1 >= normalized);
        assertTrue(t1 - normalized < RingBuffer.BUCKET_DURATION_MS);

        long t2 = normalized + 150_000L; // halfway into the 5-minute bucket
        assertEquals(normalized, RingBuffer.normalizeToBucketStartTime(t2));

        long t3 = normalized + RingBuffer.BUCKET_DURATION_MS; // next bucket
        assertEquals(t3, RingBuffer.normalizeToBucketStartTime(t3));
    }

    @Test
    public void testSlotMapping() {
        long baseTime = 1700000000000L;
        long b0 = RingBuffer.normalizeToBucketStartTime(baseTime);
        int slot0 = RingBuffer.getSlotIndex(b0);
        assertTrue(slot0 >= 0 && slot0 < RingBuffer.TOTAL_BUCKETS);

        // 288 buckets later (exactly 24 hours = 86,400,000 ms) should map to the exact same slot
        long b288 = b0 + RingBuffer.MAX_WINDOW_MS;
        int slot288 = RingBuffer.getSlotIndex(b288);
        assertEquals(slot0, slot288);
    }

    @Test
    public void testFiveAggregationFunctions() {
        long t = 1700000000000L;

        // Metric 1: COUNT
        ringBuffer.updateMetric(t, "cnt", "COUNT", 1.0);
        ringBuffer.updateMetric(t + 1000, "cnt", "COUNT", 1.0);
        ringBuffer.updateMetric(t + 2000, "cnt", "COUNT", 1.0);

        // Metric 2: SUM
        ringBuffer.updateMetric(t, "sum_amt", "SUM", 100.0);
        ringBuffer.updateMetric(t + 1000, "sum_amt", "SUM", 250.5);

        // Metric 3: AVG
        ringBuffer.updateMetric(t, "avg_score", "AVG", 80.0);
        ringBuffer.updateMetric(t + 1000, "avg_score", "AVG", 100.0);

        // Metric 4: MIN
        ringBuffer.updateMetric(t, "min_price", "MIN", 50.0);
        ringBuffer.updateMetric(t + 1000, "min_price", "MIN", 20.0);
        ringBuffer.updateMetric(t + 2000, "min_price", "MIN", 75.0);

        // Metric 5: MAX
        ringBuffer.updateMetric(t, "max_price", "MAX", 50.0);
        ringBuffer.updateMetric(t + 1000, "max_price", "MAX", 120.0);
        ringBuffer.updateMetric(t + 2000, "max_price", "MAX", 75.0);

        long bStart = RingBuffer.normalizeToBucketStartTime(t);
        Bucket bucket = ringBuffer.getBucket(bStart);
        assertNotNull(bucket);

        assertEquals(3.0, bucket.getMetricValue("cnt", "COUNT"));
        assertEquals(350.5, bucket.getMetricValue("sum_amt", "SUM"), 0.001);
        assertEquals(90.0, bucket.getMetricValue("avg_score", "AVG"), 0.001);
        assertEquals(20.0, bucket.getMetricValue("min_price", "MIN"), 0.001);
        assertEquals(120.0, bucket.getMetricValue("max_price", "MAX"), 0.001);
    }

    @Test
    public void testAggregateWindowCombiningBuckets() {
        long b0 = RingBuffer.normalizeToBucketStartTime(1700000000000L);
        long b1 = b0 + RingBuffer.BUCKET_DURATION_MS; // +5 min
        long b2 = b1 + RingBuffer.BUCKET_DURATION_MS; // +10 min

        // Bucket 0: sum 100, count 1
        ringBuffer.updateMetric(b0, "revenue", "SUM", 100.0);
        ringBuffer.updateMetric(b0, "tx_count", "COUNT", 1.0);

        // Bucket 1: sum 200, count 2
        ringBuffer.updateMetric(b1, "revenue", "SUM", 200.0);
        ringBuffer.updateMetric(b1, "tx_count", "COUNT", 1.0);
        ringBuffer.updateMetric(b1 + 1000, "tx_count", "COUNT", 1.0);

        // Bucket 2: sum 300, count 1
        ringBuffer.updateMetric(b2, "revenue", "SUM", 300.0);
        ringBuffer.updateMetric(b2, "tx_count", "COUNT", 1.0);

        // Lookback window covering b0, b1, b2 (lookback = 15 minutes = 900,000 ms)
        long lookback15m = 15 * 60 * 1000L;
        double totalRevenue = ringBuffer.aggregateWindow("revenue", "SUM", b2 + 1000L, lookback15m);
        assertEquals(600.0, totalRevenue, 0.001);

        double totalTx = ringBuffer.aggregateWindow("tx_count", "COUNT", b2 + 1000L, lookback15m);
        assertEquals(4.0, totalTx, 0.001);

        // Lookback window covering only b1 and b2 (lookback = 10 minutes = 600,000 ms)
        long lookback10m = 10 * 60 * 1000L;
        double partialRevenue = ringBuffer.aggregateWindow("revenue", "SUM", b2 + 1000L, lookback10m);
        assertEquals(500.0, partialRevenue, 0.001);
    }

    @Test
    public void testCircularOverwriteAfter24Hours() {
        long t0 = RingBuffer.normalizeToBucketStartTime(1700000000000L);
        ringBuffer.updateMetric(t0, "score", "SUM", 50.0);

        Bucket b0 = ringBuffer.getBucket(t0);
        assertNotNull(b0);
        assertEquals(50.0, b0.getMetricValue("score", "SUM"));

        // Advance by exactly 24 hours + 5 minutes -> maps to (slot0 + 1) or same slot if exactly 24 hours
        long t24hLater = t0 + RingBuffer.MAX_WINDOW_MS; // maps to SAME slot
        int slot0 = RingBuffer.getSlotIndex(t0);
        int slot24h = RingBuffer.getSlotIndex(t24hLater);
        assertEquals(slot0, slot24h);

        // Writing to the slot 24 hours later should overwrite the expired bucket (Layer 1 cleanup)
        ringBuffer.updateMetric(t24hLater, "score", "SUM", 99.0);

        Bucket bNew = ringBuffer.getBucket(t24hLater);
        assertNotNull(bNew);
        assertEquals(99.0, bNew.getMetricValue("score", "SUM"));

        // Old bucket at t0 should no longer exist
        assertNull(ringBuffer.getBucket(t0));
    }

    @Test
    public void testCleanupOldBuckets() {
        long t0 = RingBuffer.normalizeToBucketStartTime(1700000000000L);
        long t1 = t0 + RingBuffer.BUCKET_DURATION_MS;
        long t2 = t1 + RingBuffer.BUCKET_DURATION_MS;

        ringBuffer.updateMetric(t0, "c", "COUNT", 1.0);
        ringBuffer.updateMetric(t1, "c", "COUNT", 1.0);
        ringBuffer.updateMetric(t2, "c", "COUNT", 1.0);

        assertEquals(3, ringBuffer.getActiveBucketsCount());

        // Clean up buckets ending <= t1's end time
        long cutoff = t1 + RingBuffer.BUCKET_DURATION_MS; // end of t1
        ringBuffer.cleanup(cutoff);

        // t0 and t1 should be removed, t2 remains
        assertNull(ringBuffer.getBucket(t0));
        assertNull(ringBuffer.getBucket(t1));
        assertNotNull(ringBuffer.getBucket(t2));
        assertEquals(1, ringBuffer.getActiveBucketsCount());
    }

    @Test
    public void testLayer2MetricCleanup() {
        Bucket bucket = new Bucket(1700000000000L);
        bucket.updateMetric("login_count", "COUNT", 1.0);
        bucket.updateMetric("purchase_sum", "SUM", 100.0);
        bucket.updateMetric("old_metric", "COUNT", 5.0);

        assertEquals(3, bucket.getMetrics().size());

        java.util.Set<String> globalActiveMetricIds = new java.util.HashSet<>(
                java.util.Arrays.asList("login_count", "purchase_sum")
        );

        bucket.getMetrics().keySet().removeIf(metricId -> !globalActiveMetricIds.contains(metricId));

        assertEquals(2, bucket.getMetrics().size());
        assertTrue(bucket.getMetrics().containsKey("login_count"));
        assertTrue(bucket.getMetrics().containsKey("purchase_sum"));
        assertFalse(bucket.getMetrics().containsKey("old_metric"));
    }
}

