package flink.models;

import java.io.Serializable;
import java.util.Objects;

/**
 * Stores incremental aggregated values for a single metric within a 5-minute bucket.
 * Supports all 5 aggregate operations: SUM, COUNT, AVG, MIN, MAX.
 */
public class MetricBucketValue implements Serializable {
    private static final long serialVersionUID = 1L;

    private double sum;
    private long count;
    private double min;
    private double max;

    public MetricBucketValue() {
        this.sum = 0.0;
        this.count = 0L;
        this.min = Double.MAX_VALUE;
        this.max = -Double.MAX_VALUE;
    }

    public MetricBucketValue(double sum, long count, double min, double max) {
        this.sum = sum;
        this.count = count;
        this.min = min;
        this.max = max;
    }

    /**
     * Incremental update for an incoming event value according to aggregation type.
     */
    public void update(String aggregation, double value) {
        if (aggregation == null) {
            return;
        }
        String op = aggregation.trim().toUpperCase();
        switch (op) {
            case "COUNT":
                this.count++;
                break;
            case "SUM":
                this.sum += value;
                this.count++;
                break;
            case "AVG":
                this.sum += value;
                this.count++;
                break;
            case "MIN":
                this.min = Math.min(this.min, value);
                this.count++;
                break;
            case "MAX":
                this.max = Math.max(this.max, value);
                this.count++;
                break;
            default:
                this.sum += value;
                this.count++;
                this.min = Math.min(this.min, value);
                this.max = Math.max(this.max, value);
                break;
        }
    }

    /**
     * Retrieves the evaluated value for the given aggregation operation.
     */
    public double getValue(String aggregation) {
        if (aggregation == null) {
            return 0.0;
        }
        String op = aggregation.trim().toUpperCase();
        switch (op) {
            case "COUNT":
                return (double) this.count;
            case "SUM":
                return this.sum;
            case "AVG":
                return this.count > 0 ? (this.sum / this.count) : 0.0;
            case "MIN":
                return this.count > 0 ? this.min : 0.0;
            case "MAX":
                return this.count > 0 ? this.max : 0.0;
            default:
                return this.sum;
        }
    }

    /**
     * Merges another bucket's value into this one (used when reading combined buckets across a window).
     */
    public void merge(MetricBucketValue other) {
        if (other == null || other.count == 0) {
            return;
        }
        this.sum += other.sum;
        this.count += other.count;
        this.min = Math.min(this.min, other.min);
        this.max = Math.max(this.max, other.max);
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricBucketValue that = (MetricBucketValue) o;
        return Double.compare(that.sum, sum) == 0 &&
                count == that.count &&
                Double.compare(that.min, min) == 0 &&
                Double.compare(that.max, max) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sum, count, min, max);
    }

    @Override
    public String toString() {
        return "MetricBucketValue{" +
                "sum=" + sum +
                ", count=" + count +
                ", min=" + (count > 0 ? min : "N/A") +
                ", max=" + (count > 0 ? max : "N/A") +
                ", avg=" + (count > 0 ? (sum / count) : "N/A") +
                '}';
    }
}

