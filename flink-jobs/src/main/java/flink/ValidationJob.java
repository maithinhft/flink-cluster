package flink;

import flink.operators.BucketAggregationProcessFunction;
import flink.operators.SchemaValidationBroadcastProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
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

        int parallelism = parameters.getInt("parallelism", 4);
        env.setParallelism(parallelism);

        String bootstrapServers = parameters.get("bootstrap.servers", "kafka:29092");
        String schemaTopic = parameters.get("schema.topic", "schema_registry");
        String eventsTopicPattern = parameters.get("events.topic.pattern", "events.*");
        String resultTopic = parameters.get("result.topic", "result");
        String dlqTopic = parameters.get("dlq.topic", "dlq");

        if (parameters.has("checkpoint.dir")) {
            Configuration config = new Configuration();
            config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, parameters.get("checkpoint.dir"));
            env.configure(config);
        }

        LOG.info("Kafka Bootstrap Servers: {}", bootstrapServers);
        LOG.info("Schema Topic: {}", schemaTopic);
        LOG.info("Events Topic Pattern: {}", eventsTopicPattern);
        LOG.info("Result Topic: {}", resultTopic);
        LOG.info("DLQ Topic: {}", dlqTopic);

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

        KafkaSink<String> resultSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(resultTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(dlqTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<String> dirtyEventsStream = cleanEventsStream
                .getSideOutput(SchemaValidationBroadcastProcessFunction.DIRTY_DATA_TAG);

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
                .name("Bucket Aggregation Operator (Ring Buffer)")
                .disableChaining();

        DataStream<String> lateEventsStream = aggregatedStream
                .getSideOutput(BucketAggregationProcessFunction.LATE_DATA_TAG);

        DataStream<String> dlqStream = dirtyEventsStream.union(lateEventsStream);
        dlqStream.sinkTo(dlqSink).name("DLQ Kafka Sink");

        aggregatedStream.sinkTo(resultSink).name("Aggregated Result Kafka Sink");

        env.execute("Flink Event Validation & Bucket Aggregation Job");
    }
}
