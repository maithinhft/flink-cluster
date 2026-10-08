package flink.model;

/**
 * Output record for Kafka sinks: optional message key plus JSON value.
 */
public class KeyedRecord {

    public String key;
    public String value;

    public KeyedRecord() {
    }

    public KeyedRecord(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
