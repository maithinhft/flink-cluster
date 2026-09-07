package flink.models;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a 5-minute bucket containing aggregated metric values.
 */
public class Bucket implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final long DURATION_MS = 5 * 60 * 1000L; // 300,000 ms = 5 minutes

    private long startTimeMs;
    private long endTimeMs;
    private Map<String, MetricBucketValue> metrics;

    public Bucket() {
        this.metrics = new HashMap<>();
    }

    public Bucket(long startTimeMs) {
        this.startTimeMs = startTimeMs;
        this.endTimeMs = startTimeMs + DURATION_MS;
        this.metrics = new HashMap<>();
    }

    /**
     * Updates a metric value within this bucket.
     */
    public void updateMetric(String metricId, String aggregation, double value) {
        if (metricId == null) {
            return;
        }
        MetricBucketValue bucketValue = metrics.computeIfAbsent(metricId, k -> new MetricBucketValue());
        bucketValue.update(aggregation, value);
    }

    /**
     * Gets the aggregated metric value for a given metric_id and aggregation function.
     */
    public double getMetricValue(String metricId, String aggregation) {
        MetricBucketValue bucketValue = metrics.get(metricId);
        if (bucketValue == null) {
            return 0.0;
        }
        return bucketValue.getValue(aggregation);
    }

    /**
     * Merges all metrics from another bucket into this bucket.
     */
    public void merge(Bucket other) {
        if (other == null || other.metrics == null) {
            return;
        }
        for (Map.Entry<String, MetricBucketValue> entry : other.metrics.entrySet()) {
            MetricBucketValue thisVal = this.metrics.computeIfAbsent(entry.getKey(), k -> new MetricBucketValue());
            thisVal.merge(entry.getValue());
        }
    }

    public long getStartTimeMs() {
        return startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs) {
        this.startTimeMs = startTimeMs;
        this.endTimeMs = startTimeMs + DURATION_MS;
    }

    public long getEndTimeMs() {
        return endTimeMs;
    }

    public void setEndTimeMs(long endTimeMs) {
        this.endTimeMs = endTimeMs;
    }

    public Map<String, MetricBucketValue> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, MetricBucketValue> metrics) {
        this.metrics = metrics != null ? metrics : new HashMap<>();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Bucket bucket = (Bucket) o;
        return startTimeMs == bucket.startTimeMs &&
                endTimeMs == bucket.endTimeMs &&
                Objects.equals(metrics, bucket.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startTimeMs, endTimeMs, metrics);
    }

    @Override
    public String toString() {
        return "Bucket{" +
                "startTimeMs=" + startTimeMs +
                ", endTimeMs=" + endTimeMs +
                ", metricsCount=" + metrics.size() +
                '}';
    }
}

