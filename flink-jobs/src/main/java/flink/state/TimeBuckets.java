package flink.state;

/**
 * Time arithmetic for the 288 x 5-minute ring buffer (24 hours).
 * A "bucket index" is {@code floorDiv(epochMs, BUCKET_MS)}; a "slot" is the bucket index modulo {@link #SLOTS}.
 */
public final class TimeBuckets {

    public static final long BUCKET_MS = 300_000L;
    public static final int SLOTS = 288;
    public static final long MAX_WINDOW_MS = BUCKET_MS * SLOTS;

    private TimeBuckets() {
    }

    public static long bucketIndex(long epochMs) {
        return Math.floorDiv(epochMs, BUCKET_MS);
    }

    public static long bucketStart(long epochMs) {
        return bucketIndex(epochMs) * BUCKET_MS;
    }

    public static int slotOfIndex(long bucketIndex) {
        return (int) Math.floorMod(bucketIndex, (long) SLOTS);
    }

    public static int slotOf(long epochMs) {
        return slotOfIndex(bucketIndex(epochMs));
    }

    public static long roundUpToBucket(long epochMs) {
        long start = bucketStart(epochMs);
        return start == epochMs ? epochMs : start + BUCKET_MS;
    }
}
