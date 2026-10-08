package flink.state;

import java.util.Arrays;
import java.util.function.LongPredicate;

/**
 * One 5-minute bucket of one entity. Holds sum/count/min/max for every aggregation spec that matched
 * at least one event in the bucket, so a single storage cell serves all five functions
 * (SUM, COUNT, AVG, MIN, MAX). Keys are spec storage keys kept sorted for binary search.
 */
public final class CompactBucket {

    private static final int INITIAL_CAPACITY = 4;

    long startMs;
    int size;
    long[] keys;
    double[] sum;
    long[] count;
    double[] min;
    double[] max;

    public CompactBucket(long startMs) {
        this(startMs, INITIAL_CAPACITY);
    }

    CompactBucket(long startMs, int capacity) {
        this.startMs = startMs;
        int cap = Math.max(1, capacity);
        this.keys = new long[cap];
        this.sum = new double[cap];
        this.count = new long[cap];
        this.min = new double[cap];
        this.max = new double[cap];
    }

    public long getStartMs() {
        return startMs;
    }

    public long getEndMs() {
        return startMs + TimeBuckets.BUCKET_MS;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public long keyAt(int i) {
        return keys[i];
    }

    public double sumAt(int i) {
        return sum[i];
    }

    public long countAt(int i) {
        return count[i];
    }

    public double minAt(int i) {
        return min[i];
    }

    public double maxAt(int i) {
        return max[i];
    }

    public int indexOf(long key) {
        return Arrays.binarySearch(keys, 0, size, key);
    }

    public void add(long key, double value) {
        int idx = indexOf(key);
        if (idx < 0) {
            idx = insertAt(-idx - 1, key);
        }
        sum[idx] += value;
        count[idx]++;
        if (value < min[idx]) {
            min[idx] = value;
        }
        if (value > max[idx]) {
            max[idx] = value;
        }
    }

    public boolean removeIf(LongPredicate shouldRemove) {
        int write = 0;
        for (int read = 0; read < size; read++) {
            if (shouldRemove.test(keys[read])) {
                continue;
            }
            if (write != read) {
                keys[write] = keys[read];
                sum[write] = sum[read];
                count[write] = count[read];
                min[write] = min[read];
                max[write] = max[read];
            }
            write++;
        }
        boolean changed = write != size;
        size = write;
        return changed;
    }

    private int insertAt(int pos, long key) {
        if (size == keys.length) {
            int cap = keys.length * 2;
            keys = Arrays.copyOf(keys, cap);
            sum = Arrays.copyOf(sum, cap);
            count = Arrays.copyOf(count, cap);
            min = Arrays.copyOf(min, cap);
            max = Arrays.copyOf(max, cap);
        }
        int tail = size - pos;
        if (tail > 0) {
            System.arraycopy(keys, pos, keys, pos + 1, tail);
            System.arraycopy(sum, pos, sum, pos + 1, tail);
            System.arraycopy(count, pos, count, pos + 1, tail);
            System.arraycopy(min, pos, min, pos + 1, tail);
            System.arraycopy(max, pos, max, pos + 1, tail);
        }
        keys[pos] = key;
        sum[pos] = 0.0;
        count[pos] = 0L;
        min[pos] = Double.POSITIVE_INFINITY;
        max[pos] = Double.NEGATIVE_INFINITY;
        size++;
        return pos;
    }

    public CompactBucket copy() {
        CompactBucket c = new CompactBucket(startMs, Math.max(size, 1));
        System.arraycopy(keys, 0, c.keys, 0, size);
        System.arraycopy(sum, 0, c.sum, 0, size);
        System.arraycopy(count, 0, c.count, 0, size);
        System.arraycopy(min, 0, c.min, 0, size);
        System.arraycopy(max, 0, c.max, 0, size);
        c.size = size;
        return c;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CompactBucket)) {
            return false;
        }
        CompactBucket other = (CompactBucket) o;
        if (startMs != other.startMs || size != other.size) {
            return false;
        }
        for (int i = 0; i < size; i++) {
            if (keys[i] != other.keys[i] || count[i] != other.count[i]
                    || Double.compare(sum[i], other.sum[i]) != 0
                    || Double.compare(min[i], other.min[i]) != 0
                    || Double.compare(max[i], other.max[i]) != 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int h = Long.hashCode(startMs);
        for (int i = 0; i < size; i++) {
            h = 31 * h + Long.hashCode(keys[i]);
            h = 31 * h + Long.hashCode(count[i]);
        }
        return h;
    }

    @Override
    public String toString() {
        return "CompactBucket{startMs=" + startMs + ", specs=" + size + '}';
    }
}
