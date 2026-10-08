package flink.io;

import flink.compile.Values;
import flink.model.KeyedRecord;
import flink.model.RawEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Kafka (de)serialisation and watermark helpers shared by the jobs.
 */
public final class KafkaIo {

    private KafkaIo() {
    }

    /**
     * Value-only string deserializer that skips null values (Debezium tombstones) instead of failing.
     */
    public static final class NullSafeStringDeserializer implements KafkaRecordDeserializationSchema<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) {
            if (record.value() != null) {
                out.collect(new String(record.value(), StandardCharsets.UTF_8));
            }
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return Types.STRING;
        }
    }

    /**
     * Keeps the Kafka record timestamp next to the value for end-to-end latency measurement.
     */
    public static final class RawEventDeserializer implements KafkaRecordDeserializationSchema<RawEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RawEvent> out) {
            if (record.value() != null) {
                out.collect(new RawEvent(new String(record.value(), StandardCharsets.UTF_8),
                        record.timestamp(), record.topic()));
            }
        }

        @Override
        public TypeInformation<RawEvent> getProducedType() {
            return TypeInformation.of(RawEvent.class);
        }
    }

    public static final class KeyedRecordSerializer implements KafkaRecordSerializationSchema<KeyedRecord> {
        private static final long serialVersionUID = 1L;
        private final String topic;

        public KeyedRecordSerializer(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(KeyedRecord element, KafkaSinkContext context, Long timestamp) {
            byte[] key = element.key == null ? null : element.key.getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topic, null, timestamp, key, element.value.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Extracts {@code event_time} with a cheap string scan; falls back to the Kafka timestamp.
     */
    public static final class EventTimeAssigner implements SerializableTimestampAssigner<RawEvent> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractTimestamp(RawEvent e, long recordTimestamp) {
            long t = extractEventTime(e.value);
            if (t != Values.NO_TIMESTAMP) {
                return t;
            }
            return e.kafkaTimestamp > 0 ? e.kafkaTimestamp : recordTimestamp;
        }
    }

    public static long extractEventTime(String json) {
        if (json == null) {
            return Values.NO_TIMESTAMP;
        }
        int idx = json.indexOf("\"event_time\"");
        if (idx < 0) {
            return Values.NO_TIMESTAMP;
        }
        int colon = json.indexOf(':', idx + 12);
        if (colon < 0) {
            return Values.NO_TIMESTAMP;
        }
        int start = json.indexOf('"', colon + 1);
        if (start < 0) {
            return Values.NO_TIMESTAMP;
        }
        int end = json.indexOf('"', start + 1);
        if (end < 0) {
            return Values.NO_TIMESTAMP;
        }
        return Values.parseTimestamp(json.substring(start + 1, end));
    }

    public static WatermarkStrategy<RawEvent> eventWatermarks(Duration outOfOrderness, Duration idleness,
                                                             long alignmentMaxDriftMs) {
        WatermarkStrategy<RawEvent> s = WatermarkStrategy.<RawEvent>forBoundedOutOfOrderness(outOfOrderness)
                .withTimestampAssigner(new EventTimeAssigner())
                .withIdleness(idleness);
        if (alignmentMaxDriftMs > 0) {
            s = s.withWatermarkAlignment("events", Duration.ofMillis(alignmentMaxDriftMs), Duration.ofSeconds(1));
        }
        return s;
    }

    /**
     * Control streams carry no event time. Emitting a (near) maximal watermark keeps them from holding
     * back the minimum watermark of the two-input operators they are connected to.
     */
    public static <T> WatermarkStrategy<T> controlWatermarks() {
        return ctx -> new WatermarkGenerator<T>() {
            @Override
            public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput output) {
                output.emitWatermark(new Watermark(Long.MAX_VALUE - 1));
            }
        };
    }
}
