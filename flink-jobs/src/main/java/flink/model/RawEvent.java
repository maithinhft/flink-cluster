package flink.model;

/**
 * Raw Kafka event value plus the broker timestamp (used for end-to-end latency).
 */
public class RawEvent {

    public String value;
    public long kafkaTimestamp;
    public String topic;

    public RawEvent() {
    }

    public RawEvent(String value, long kafkaTimestamp, String topic) {
        this.value = value;
        this.kafkaTimestamp = kafkaTimestamp;
        this.topic = topic;
    }
}
