package flink.models;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Ring Buffer data structure maintaining 288 buckets of 5 minutes each (total 24 hours).
 * Implements circular slot mapping, incremental writes, combined bucket reads for windows,
 * and 3-layer cleanup support.
 */
public class RingBuffer implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int TOTAL_BUCKETS = 288; // 24 hours / 5 minutes = 288
    public static final long BUCKET_DURATION_MS = 5 * 60 * 1000L; // 300,000 ms
    public static final long MAX_WINDOW_MS = 24 * 60 * 60 * 1000L; // 86,400,000 ms

    private final Bucket[] buckets;

    public RingBuffer() {
        this.buckets = new Bucket[TOTAL_BUCKETS];
    }

    /**
     * Normalizes any epoch millisecond timestamp to the start time of its 5-minute bucket.
     */
    public static long normalizeToBucketStartTime(long timestampMs) {
        long remainder = timestampMs % BUCKET_DURATION_MS;
        if (remainder < 0) {
            remainder += BUCKET_DURATION_MS;
        }
        return timestampMs - remainder;
    }

    /**
     * Maps a bucket start timestamp to a slot index in the circular buffer (0..287).
     */
    public static int getSlotIndex(long bucketStartTimeMs) {
        long bucketId = bucketStartTimeMs / BUCKET_DURATION_MS;
        int slot = (int) (bucketId % TOTAL_BUCKETS);
        if (slot < 0) {
            slot += TOTAL_BUCKETS;
        }
        return slot;
    }

    /**
     * Retrieves or creates a bucket at the given start timestamp.
     * Overwrites expired buckets older than 24 hours occupying the same slot (Layer 1 cleanup).
     */
    public synchronized Bucket getOrCreateBucket(long bucketStartTimeMs) {
        int slot = getSlotIndex(bucketStartTimeMs);
        Bucket existing = buckets[slot];

        if (existing == null) {
            Bucket newBucket = new Bucket(bucketStartTimeMs);
            buckets[slot] = newBucket;
            return newBucket;
        }

        if (existing.getStartTimeMs() == bucketStartTimeMs) {
            return existing;
        }

        if (bucketStartTimeMs > existing.getStartTimeMs()) {
            // The slot holds an expired bucket from a previous 24h cycle -> circular overwrite
            Bucket newBucket = new Bucket(bucketStartTimeMs);
            buckets[slot] = newBucket;
            return newBucket;
        }

        // The slot already holds a newer bucket, meaning this incoming timestamp is older than 24h
        return null;
    }

    /**
     * Incrementally updates a single metric for an event timestamp.
     */
    public synchronized void updateMetric(long eventTimeMs, String metricId, String aggregation, double value) {
        long bucketStartTime = normalizeToBucketStartTime(eventTimeMs);
        Bucket bucket = getOrCreateBucket(bucketStartTime);
        if (bucket != null) {
            bucket.updateMetric(metricId, aggregation, value);
        }
    }

    /**
     * Incrementally updates multiple metrics for an event timestamp.
     */
    public synchronized void updateMetrics(long eventTimeMs, List<MetricUpdate> updates) {
        if (updates == null || updates.isEmpty()) {
            return;
        }
        long bucketStartTime = normalizeToBucketStartTime(eventTimeMs);
        Bucket bucket = getOrCreateBucket(bucketStartTime);
        if (bucket != null) {
            for (MetricUpdate update : updates) {
                bucket.updateMetric(update.getMetricId(), update.getAggregation(), update.getValue());
            }
        }
    }

    /**
     * Returns a specific bucket by its start timestamp, or null if not found/expired.
     */
    public synchronized Bucket getBucket(long bucketStartTimeMs) {
        int slot = getSlotIndex(bucketStartTimeMs);
        Bucket b = buckets[slot];
        if (b != null && b.getStartTimeMs() == bucketStartTimeMs) {
            return b;
        }
        return null;
    }

    /**
     * Combines buckets within [endTimestampMs - lookbackMs, endTimestampMs]
     * to compute the aggregated metric value over a lookback window ("đọc gộp bucket").
     */
    public synchronized double aggregateWindow(String metricId, String aggregation, long endTimestampMs, long lookbackMs) {
        if (metricId == null || aggregation == null || lookbackMs <= 0) {
            return 0.0;
        }

        long windowEndBucketStart = normalizeToBucketStartTime(endTimestampMs);
        int numBuckets = Math.max(1, (int) Math.round((double) lookbackMs / BUCKET_DURATION_MS));
        numBuckets = Math.min(numBuckets, TOTAL_BUCKETS);

        long windowStartBucketStart = windowEndBucketStart - (long) (numBuckets - 1) * BUCKET_DURATION_MS;

        MetricBucketValue combined = new MetricBucketValue();

        for (long bStart = windowStartBucketStart; bStart <= windowEndBucketStart; bStart += BUCKET_DURATION_MS) {
            int slot = getSlotIndex(bStart);
            Bucket b = buckets[slot];
            if (b != null && b.getStartTimeMs() == bStart) {
                MetricBucketValue val = b.getMetrics().get(metricId);
                if (val != null) {
                    combined.merge(val);
                }
            }
        }

        return combined.getValue(aggregation);
    }

    /**
     * Cleans up all buckets whose end time is older than or equal to cutoffTimestampMs.
     * Used by event-time timer (Layer 2 cleanup).
     */
    public synchronized void cleanup(long cutoffTimestampMs) {
        for (int i = 0; i < TOTAL_BUCKETS; i++) {
            Bucket b = buckets[i];
            if (b != null && b.getEndTimeMs() <= cutoffTimestampMs) {
                buckets[i] = null;
            }
        }
    }

    /**
     * Counts currently active (non-null) buckets.
     */
    public synchronized int getActiveBucketsCount() {
        int count = 0;
        for (Bucket b : buckets) {
            if (b != null) count++;
        }
        return count;
    }

    public Bucket[] getBuckets() {
        return buckets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RingBuffer that = (RingBuffer) o;
        return Arrays.equals(buckets, that.buckets);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(buckets);
    }

    @Override
    public String toString() {
        return "RingBuffer{" +
                "activeBuckets=" + getActiveBucketsCount() +
                ", totalCapacity=" + TOTAL_BUCKETS +
                '}';
    }
}
