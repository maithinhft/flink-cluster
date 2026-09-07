package flink;

import flink.operators.BucketAggregationProcessFunction;
import flink.operators.SchemaValidationBroadcastProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public class ValidationJob {
    private static final Logger LOG = LoggerFactory.getLogger(ValidationJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink Event Validation & Bucket Aggregation Job...");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameters = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameters);

        String bootstrapServers = parameters.get("bootstrap.servers", "kafka:29092");
        String schemaTopic = parameters.get("schema.topic", "schema_registry");
        String eventsTopicPattern = parameters.get("events.topic.pattern", "events.*");

        if (parameters.has("checkpoint.dir")) {
            env.getCheckpointConfig().setCheckpointStorage(parameters.get("checkpoint.dir"));
        }

        LOG.info("Kafka Bootstrap Servers: {}", bootstrapServers);
        LOG.info("Schema Topic: {}", schemaTopic);
        LOG.info("Events Topic Pattern: {}", eventsTopicPattern);

        KafkaSource<String> schemaSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(schemaTopic)
                .setGroupId("flink-schema-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> schemaStream = env.fromSource(
                schemaSource,
                WatermarkStrategy.noWatermarks(),
                "Schema Registry Source");

        BroadcastStream<String> broadcastSchemaStream = schemaStream
                .broadcast(
                        SchemaValidationBroadcastProcessFunction.SCHEMA_STATE_DESCRIPTOR,
                        SchemaValidationBroadcastProcessFunction.LATEST_VERSION_DESCRIPTOR,
                        SchemaValidationBroadcastProcessFunction.DEPRECATED_SCHEMAS_DESCRIPTOR);

        WatermarkStrategy<String> eventWatermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((eventJson, recordTimestamp) -> {
                    try {
                        int idx = eventJson.indexOf("\"event_time\"");
                        if (idx != -1) {
                            int startQuote = eventJson.indexOf('"', idx + 12);
                            int endQuote = eventJson.indexOf('"', startQuote + 1);
                            if (startQuote != -1 && endQuote != -1) {
                                String timeStr = eventJson.substring(startQuote + 1, endQuote);
                                return Instant.parse(timeStr).toEpochMilli();
                            }
                        }
                    } catch (Exception ignored) {
                    }
                    return recordTimestamp > 0 ? recordTimestamp : System.currentTimeMillis();
                })
                .withIdleness(Duration.ofMinutes(1));

        KafkaSource<String> eventSource = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(java.util.regex.Pattern.compile(eventsTopicPattern))
                .setGroupId("flink-event-validation-group")
                .setProperty("partition.discovery.interval.ms", "60000")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> eventStream = env.fromSource(
                eventSource,
                eventWatermarkStrategy,
                "Events Multi-Topic Source");

        SingleOutputStreamOperator<String> cleanEventsStream = eventStream
                .connect(broadcastSchemaStream)
                .process(new SchemaValidationBroadcastProcessFunction())
                .name("Schema Validation Operator");

        DataStream<String> dirtyEventsStream = cleanEventsStream
                .getSideOutput(SchemaValidationBroadcastProcessFunction.DIRTY_DATA_TAG);
        dirtyEventsStream.map(data -> {
            LOG.info("DIRTY DATA (DLQ) -> {}", data);
            return data;
        }).name("Dirty Events DLQ Sink");

        SingleOutputStreamOperator<String> aggregatedStream = cleanEventsStream
                .keyBy(eventJson -> {
                    try {
                        int idx = eventJson.indexOf("\"entity_id\"");
                        if (idx != -1) {
                            int startQuote = eventJson.indexOf('"', idx + 11);
                            int endQuote = eventJson.indexOf('"', startQuote + 1);
                            if (startQuote != -1 && endQuote != -1) {
                                return eventJson.substring(startQuote + 1, endQuote);
                            }
                        }
                    } catch (Exception ignored) {
                    }
                    return "unknown_entity";
                })
                .process(new BucketAggregationProcessFunction())
                .name("Bucket Aggregation Operator (Ring Buffer)");

        DataStream<String> lateEventsStream = aggregatedStream
                .getSideOutput(BucketAggregationProcessFunction.LATE_DATA_TAG);
        lateEventsStream.map(data -> {
            LOG.info("LATE DATA (DLQ) -> {}", data);
            return data;
        }).name("Late Events DLQ Sink");

        aggregatedStream.map(data -> {
            LOG.info("AGGREGATED BUCKET EVENT -> {}", data);
            return data;
        }).name("Aggregated Events Sink");

        env.execute("Flink Event Validation & Bucket Aggregation Job");
    }
}
