package flink.models;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents an extracted metric update from an event.
 */
public class MetricUpdate implements Serializable {
    private static final long serialVersionUID = 1L;

    private String metricId;
    private String aggregation;
    private double value;

    public MetricUpdate() {
    }

    public MetricUpdate(String metricId, String aggregation, double value) {
        this.metricId = metricId;
        this.aggregation = aggregation;
        this.value = value;
    }

    public String getMetricId() {
        return metricId;
    }

    public void setMetricId(String metricId) {
        this.metricId = metricId;
    }

    public String getAggregation() {
        return aggregation;
    }

    public void setAggregation(String aggregation) {
        this.aggregation = aggregation;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MetricUpdate that = (MetricUpdate) o;
        return Double.compare(that.value, value) == 0 &&
                Objects.equals(metricId, that.metricId) &&
                Objects.equals(aggregation, that.aggregation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricId, aggregation, value);
    }

    @Override
    public String toString() {
        return "MetricUpdate{" +
                "metricId='" + metricId + '\'' +
                ", aggregation='" + aggregation + '\'' +
                ", value=" + value +
                '}';
    }
}

